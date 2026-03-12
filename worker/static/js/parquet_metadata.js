// Shared module for parquet metadata fetching, caching, and URL helpers.
// Used by download_panel_control.js and extent_handler.js.

import { duckdbClient } from './duckdb_client.js';
import { proxyUrl } from './utils.js';

class ParquetMetadata {
  constructor() {
    this.metaJsonCache = new Map();
    this.partitionCache = new Map();
    this.bboxCache = new Map();
    this.rgBboxCache = new Map();
  }

  getParquetUrl(originalUrl) {
    return originalUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet');
  }

  getMetaJsonUrl(originalUrl) {
    return originalUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet.meta.json');
  }

  getBaseUrl(originalUrl) {
    const lastSlash = originalUrl.lastIndexOf('/');
    return originalUrl.substring(0, lastSlash + 1);
  }

  async fetchMetaJson(metaUrl) {
    if (this.metaJsonCache.has(metaUrl)) {
      return this.metaJsonCache.get(metaUrl);
    }

    try {
      const proxied = proxyUrl(metaUrl);
      const response = await fetch(proxied);

      if (!response.ok) {
        throw new Error(`Failed to fetch meta.json: ${response.status}`);
      }

      const metaJson = await response.json();
      this.metaJsonCache.set(metaUrl, metaJson);
      const partitions = metaJson.extents ? Object.keys(metaJson.extents) : [];
      this.partitionCache.set(metaUrl, partitions);
      return metaJson;
    } catch (error) {
      console.error('Error fetching partition metadata:', error);
      return null;
    }
  }

  async getPartitions(metaUrl) {
    if (this.partitionCache.has(metaUrl)) {
      return this.partitionCache.get(metaUrl);
    }
    const metaJson = await this.fetchMetaJson(metaUrl);
    if (!metaJson) return null;
    return this.partitionCache.get(metaUrl);
  }

  async getExtents(metaUrl) {
    const metaJson = await this.fetchMetaJson(metaUrl);
    return metaJson?.extents ?? null;
  }

  // --- DuckDB-based parquet metadata reading ---

  /**
   * Read geoparquet bbox from a single parquet file's key-value metadata.
   * Uses DuckDB's parquet_kv_metadata() — only reads the file footer via range requests.
   * @returns {Promise<[number,number,number,number]|null>} [minx, miny, maxx, maxy] or null
   */
  async getParquetBbox(parquetUrl) {
    if (this.bboxCache.has(parquetUrl)) return this.bboxCache.get(parquetUrl);
    await duckdbClient.init();

    try {
      const safeUrl = proxyUrl(parquetUrl, { absolute: true }).replace(/'/g, "''");
      const geoMeta = await this._getGeoMetadata(safeUrl);
      if (!geoMeta) { this.bboxCache.set(parquetUrl, null); return null; }

      const primaryCol = geoMeta.primary_column || 'geometry';
      const colMeta = geoMeta.columns?.[primaryCol];
      if (!colMeta?.bbox || colMeta.bbox.length < 4) { this.bboxCache.set(parquetUrl, null); return null; }

      const [minx, miny, maxx, maxy] = colMeta.bbox;
      if (!this._isValidWgs84Bbox(minx, miny, maxx, maxy)) {
        console.warn('[ParquetMetadata] Parquet bbox outside WGS84 range:', colMeta.bbox);
        this.bboxCache.set(parquetUrl, null);
        return null;
      }

      this.bboxCache.set(parquetUrl, colMeta.bbox);
      return colMeta.bbox;
    } catch (error) {
      console.error('[ParquetMetadata] Failed to read parquet bbox:', error);
      this.bboxCache.set(parquetUrl, null);
      return null;
    }
  }

  /**
   * Read per-row-group bounding boxes from a single parquet file.
   * @returns {Promise<Object<string,[number,number,number,number]>|null>} {rg_N: bbox} or null
   */
  async getRowGroupBboxes(parquetUrl) {
    const result = await this.getRowGroupBboxesMulti([parquetUrl]);
    if (!result) return null;
    const firstKey = Object.keys(result)[0];
    return firstKey ? result[firstKey] : null;
  }

  /**
   * Read per-row-group bounding boxes from multiple parquet files in a single query.
   * Uses DuckDB's parquet_metadata() list support for parallelism.
   * @param {string[]} parquetUrls - Array of parquet file URLs
   * @returns {Promise<Object<string, Object<string,[number,number,number,number]>>|null>}
   *   { filename: { rg_0: [minx,miny,maxx,maxy], ... }, ... } or null
   */
  async getRowGroupBboxesMulti(parquetUrls) {
    if (!parquetUrls?.length) return null;

    // Check if all URLs are already cached
    const cacheKey = parquetUrls.join('\n');
    if (this.rgBboxCache.has(cacheKey)) return this.rgBboxCache.get(cacheKey);

    await duckdbClient.init();

    try {
      const proxyUrls = parquetUrls.map(u => proxyUrl(u, { absolute: true }));
      const proxyToFilename = {};
      for (let i = 0; i < parquetUrls.length; i++) {
        proxyToFilename[proxyUrls[i]] = parquetUrls[i].split('/').pop();
      }

      const firstSafeUrl = proxyUrls[0].replace(/'/g, "''");
      const coveringPaths = await this._getCoveringBboxPaths(firstSafeUrl);
      if (!coveringPaths) { this.rgBboxCache.set(cacheKey, null); return null; }

      const { xminPath, yminPath, xmaxPath, ymaxPath } = coveringPaths;
      const allPaths = [xminPath, yminPath, xmaxPath, ymaxPath];
      const urlList = proxyUrls.map(u => `'${u.replace(/'/g, "''")}'`).join(',');

      const queryResult = await duckdbClient.conn.query(
        `SELECT file_name, row_group_id, path_in_schema, stats_min, stats_max
         FROM parquet_metadata([${urlList}])
         WHERE path_in_schema IN (${allPaths.map(p => `'${p}'`).join(',')})
         ORDER BY file_name, row_group_id, path_in_schema`
      );

      const rows = queryResult.toArray();
      if (rows.length === 0) { this.rgBboxCache.set(cacheKey, null); return null; }

      const fileGroups = {};
      for (const row of rows) {
        const fileName = proxyToFilename[row.file_name] || row.file_name;
        if (!fileGroups[fileName]) fileGroups[fileName] = {};
        const rgId = Number(row.row_group_id);
        if (!fileGroups[fileName][rgId]) fileGroups[fileName][rgId] = {};
        const path = row.path_in_schema;
        if (path === xminPath) fileGroups[fileName][rgId].xmin = Number(row.stats_min);
        if (path === yminPath) fileGroups[fileName][rgId].ymin = Number(row.stats_min);
        if (path === xmaxPath) fileGroups[fileName][rgId].xmax = Number(row.stats_max);
        if (path === ymaxPath) fileGroups[fileName][rgId].ymax = Number(row.stats_max);
      }

      const allExtents = {};
      for (const [fileName, groups] of Object.entries(fileGroups)) {
        const extents = {};
        for (const [rgId, g] of Object.entries(groups)) {
          if (g.xmin == null || g.ymin == null || g.xmax == null || g.ymax == null) continue;
          if (!this._isValidWgs84Bbox(g.xmin, g.ymin, g.xmax, g.ymax)) continue;
          extents[`rg_${rgId}`] = [g.xmin, g.ymin, g.xmax, g.ymax];
        }
        if (Object.keys(extents).length > 0) {
          allExtents[fileName] = extents;
        }
      }

      const result = Object.keys(allExtents).length > 0 ? allExtents : null;
      this.rgBboxCache.set(cacheKey, result);
      return result;
    } catch (error) {
      console.error('[ParquetMetadata] Failed to read row group bboxes:', error);
      this.rgBboxCache.set(cacheKey, null);
      return null;
    }
  }

  // --- Internal helpers ---

  /** Read and parse the 'geo' key-value metadata from a parquet file */
  async _getGeoMetadata(safeUrl) {
    const result = await duckdbClient.conn.query(
      `SELECT value FROM parquet_kv_metadata('${safeUrl}') WHERE key='geo'`
    );
    const rows = result.toArray();
    if (rows.length === 0) return null;
    return this._parseKvBlob(rows[0].value);
  }

  /** Get covering.bbox paths from geo kv metadata (GeoParquet 1.1+) */
  async _getCoveringBboxPaths(safeUrl) {
    const geoMeta = await this._getGeoMetadata(safeUrl);
    if (!geoMeta) return null;

    const primaryCol = geoMeta.primary_column || 'geometry';
    const covering = geoMeta.columns?.[primaryCol]?.covering?.bbox;
    if (!covering) return null;

    // DuckDB parquet_metadata() uses ", " (comma-space) to join nested struct field paths
    return {
      xminPath: covering.xmin?.join(', ') || 'bbox, xmin',
      yminPath: covering.ymin?.join(', ') || 'bbox, ymin',
      xmaxPath: covering.xmax?.join(', ') || 'bbox, xmax',
      ymaxPath: covering.ymax?.join(', ') || 'bbox, ymax',
    };
  }

  /** Decode DuckDB kv_metadata BLOB value to parsed JSON */
  _parseKvBlob(raw) {
    if (raw instanceof Uint8Array || raw instanceof ArrayBuffer) {
      raw = new TextDecoder().decode(raw);
    } else if (typeof raw !== 'string') {
      raw = String(raw);
    }
    return JSON.parse(raw);
  }

  _isValidWgs84Bbox(minx, miny, maxx, maxy) {
    return Math.abs(minx) <= 180 && Math.abs(maxx) <= 180 &&
           Math.abs(miny) <= 90 && Math.abs(maxy) <= 90;
  }
}

export const parquetMetadata = new ParquetMetadata();
