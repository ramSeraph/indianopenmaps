// Shared DuckDB WASM singleton: manages lifecycle, provides parquet metadata reading.
// Used by extent_handler.js (visualization) and partial_download_handler.js (downloads).

const DUCKDB_BASE = 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir';
import * as duckdb from 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir/duckdb-browser.mjs';

class DuckDBClient {
  constructor() {
    this.db = null;
    this.conn = null;
    this.initialized = false;
  }

  async init() {
    if (this.initialized) return;

    try {
      const CUSTOM_BUNDLES = {
        mvp: {
          mainModule: `${DUCKDB_BASE}/duckdb-mvp.wasm`,
          mainWorker: `${DUCKDB_BASE}/duckdb-browser-mvp.worker.js`,
        },
        eh: {
          mainModule: `${DUCKDB_BASE}/duckdb-eh.wasm`,
          mainWorker: `${DUCKDB_BASE}/duckdb-browser-eh.worker.js`,
        },
        coi: {
          mainModule: `${DUCKDB_BASE}/duckdb-coi.wasm`,
          mainWorker: `${DUCKDB_BASE}/duckdb-browser-coi.worker.js`,
          pthreadWorker: `${DUCKDB_BASE}/duckdb-browser-coi.pthread.worker.js`,
        },
      };
      const bundle = await duckdb.selectBundle(CUSTOM_BUNDLES);

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
      );
      try {
        const worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);

        this.db = new duckdb.AsyncDuckDB(logger, worker);
        await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      } finally {
        URL.revokeObjectURL(worker_url);
      }

      this.conn = await this.db.connect();
      await this.conn.query(`INSTALL spatial; LOAD spatial;`);
      // Disable built-in HTTP handler first, then load httpfs for proper range request support
      await this.conn.query(`SET builtin_httpfs = false;`);
      await this.conn.query(`INSTALL httpfs; LOAD httpfs;`);

      this.initialized = true;
      console.log('[DuckDB] Initialized with httpfs');
    } catch (error) {
      console.error('[DuckDB] Failed to initialize:', error);
      throw error;
    }
  }

  buildProxyUrl(url) {
    const origin = window.location.origin;
    return `${origin}/proxy?url=${encodeURIComponent(url)}`;
  }

  /**
   * Read geoparquet bbox from a single parquet file's key-value metadata.
   * Uses DuckDB's parquet_kv_metadata() — only reads the file footer via range requests.
   * @returns {Promise<[number,number,number,number]|null>} [minx, miny, maxx, maxy] or null
   */
  async getParquetBbox(parquetUrl) {
    await this.init();

    try {
      const proxyUrl = this.buildProxyUrl(parquetUrl);
      const safeUrl = proxyUrl.replace(/'/g, "''");
      const result = await this.conn.query(
        `SELECT value FROM parquet_kv_metadata('${safeUrl}') WHERE key='geo'`
      );
      const rows = result.toArray();
      if (rows.length === 0) return null;

      const geoMeta = this._parseKvBlob(rows[0].value);
      const primaryCol = geoMeta.primary_column || 'geometry';
      const colMeta = geoMeta.columns?.[primaryCol];
      if (!colMeta?.bbox || colMeta.bbox.length < 4) return null;

      const [minx, miny, maxx, maxy] = colMeta.bbox;
      if (!this._isValidWgs84Bbox(minx, miny, maxx, maxy)) {
        console.warn('[DuckDB] Parquet bbox outside WGS84 range:', colMeta.bbox);
        return null;
      }

      return colMeta.bbox;
    } catch (error) {
      console.error('[DuckDB] Failed to read parquet bbox:', error);
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
    await this.init();

    try {
      // Read covering paths from the first file's geo metadata
      const firstProxyUrl = this.buildProxyUrl(parquetUrls[0]);
      const firstSafeUrl = firstProxyUrl.replace(/'/g, "''");

      const kvResult = await this.conn.query(
        `SELECT value FROM parquet_kv_metadata('${firstSafeUrl}') WHERE key='geo'`
      );
      const kvRows = kvResult.toArray();
      if (kvRows.length === 0) return null;

      const geoMeta = this._parseKvBlob(kvRows[0].value);
      const primaryCol = geoMeta.primary_column || 'geometry';
      const covering = geoMeta.columns?.[primaryCol]?.covering?.bbox;
      if (!covering) return null;

      // DuckDB parquet_metadata() uses ", " (comma-space) to join nested struct field paths
      const xminPath = covering.xmin?.join(', ') || 'bbox, xmin';
      const yminPath = covering.ymin?.join(', ') || 'bbox, ymin';
      const xmaxPath = covering.xmax?.join(', ') || 'bbox, xmax';
      const ymaxPath = covering.ymax?.join(', ') || 'bbox, ymax';
      const paths = [xminPath, yminPath, xmaxPath, ymaxPath];

      const proxyUrls = parquetUrls.map(u => this.buildProxyUrl(u).replace(/'/g, "''"));
      const urlList = proxyUrls.map(u => `'${u}'`).join(',');

      const result = await this.conn.query(
        `SELECT file_name, row_group_id, path_in_schema, stats_min, stats_max
         FROM parquet_metadata([${urlList}])
         WHERE path_in_schema IN (${paths.map(p => `'${p}'`).join(',')})
         ORDER BY file_name, row_group_id, path_in_schema`
      );

      const rows = result.toArray();
      if (rows.length === 0) return null;

      // Build a map: proxyUrl -> original filename for labeling
      const proxyToFilename = {};
      for (let i = 0; i < parquetUrls.length; i++) {
        const proxyUrl = this.buildProxyUrl(parquetUrls[i]);
        const filename = parquetUrls[i].split('/').pop();
        proxyToFilename[proxyUrl] = filename;
      }

      // Group by file, then by row_group_id
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

      return Object.keys(allExtents).length > 0 ? allExtents : null;
    } catch (error) {
      console.error('[DuckDB] Failed to read row group bboxes:', error);
      return null;
    }
  }

  // --- Internal helpers ---

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

export const duckdbClient = new DuckDBClient();
