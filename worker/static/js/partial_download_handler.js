// Partial download handler — owns a DuckDB client for queries,
// manages OPFS temp directories and download lifecycle.

import { DuckDBClient } from './duckdb_client.js';
import { parquetMetadata } from './parquet_metadata.js';
import { SizeGetter } from './size_getter.js';
import { proxyUrl, OPFS_PREFIX_TMPDIR, getOpfsPrefixes, ScopedProgress } from './utils.js';
import { CsvFormatHandler } from './format_csv.js';
import { GeoJsonFormatHandler } from './format_geojson.js';
import { GeoParquetFormatHandler } from './format_geoparquet.js';
import { GeoPackageFormatHandler } from './format_geopackage.js';
import { ShapefileFormatHandler } from './format_shapefile.js';
import { KmlFormatHandler } from './format_kml.js';
import { DxfFormatHandler } from './format_dxf.js';

export const FORMAT_OPTIONS = [
  { value: 'geopackage', label: 'GeoPackage (.gpkg)' },
  { value: 'geojson', label: 'GeoJSON' },
  { value: 'geojsonseq', label: 'GeoJSONSeq (.geojsonl)' },
  { value: 'geoparquet', label: 'GeoParquet (v1.1)' },
  { value: 'geoparquet2', label: 'GeoParquet (v2.0)' },
  { value: 'csv', label: 'CSV (WKT geometry)' },
  { value: 'shapefile', label: 'Shapefile (.shp)' },
  { value: 'kml', label: 'KML (.kml)' },
  { value: 'dxf', label: 'DXF (.dxf)' },
];

/** Normalize extent (array or object) to [minx, miny, maxx, maxy]. */
function extentBounds(extent) {
  return Array.isArray(extent)
    ? extent
    : [extent.minx, extent.miny, extent.maxx, extent.maxy];
}

/** Returns the fraction of extent overlapped by bbox (0–1), or null if extent is missing/degenerate. */
function bboxOverlapRatio(extent, bbox) {
  if (!extent) return null;
  const [minx, miny, maxx, maxy] = extentBounds(extent);
  const area = (maxx - minx) * (maxy - miny);
  if (area <= 0) return null;
  const ix = Math.max(0, Math.min(bbox.east, maxx) - Math.max(bbox.west, minx));
  const iy = Math.max(0, Math.min(bbox.north, maxy) - Math.max(bbox.south, miny));
  return Math.min(1, (ix * iy) / area);
}

function getFormatHandler(format, opts) {
  switch (format) {
    case 'csv': return new CsvFormatHandler(opts);
    case 'geojson': return new GeoJsonFormatHandler({ commaSeparated: true, ...opts });
    case 'geojsonseq': return new GeoJsonFormatHandler({ commaSeparated: false, ...opts });
    case 'geoparquet': return new GeoParquetFormatHandler({ version: '1.1', ...opts });
    case 'geoparquet2': return new GeoParquetFormatHandler({ version: '2.0', ...opts });
    case 'geopackage': return new GeoPackageFormatHandler(opts);
    case 'shapefile': return new ShapefileFormatHandler(opts);
    case 'kml': return new KmlFormatHandler(opts);
    case 'dxf': return new DxfFormatHandler(opts);
    default: throw new Error(`Unsupported format: ${format}`);
  }
}

// Delay before revoking blob URLs / cleaning up OPFS files, so the browser can finish streaming.
// There's no browser event for "blob URL download finished." The File System Access API
// (showSaveFilePicker) would give explicit completion, but is Chrome/Edge only.
const DOWNLOAD_CLEANUP_DELAY_MS = 120000;

// Progress phases: 0→WRITE_START (init), WRITE_START→WRITE_END (format handler write),
// WRITE_END→100 (save + cleanup)
const PROGRESS_WRITE_START = 5;
const PROGRESS_WRITE_END = 90;

// Unique tab ID to avoid OPFS conflicts between tabs.
// Uses '-' internally so it can be extracted from '_'-delimited filenames with a single split.
const TAB_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

// Web Lock held for this tab's lifetime — used to detect orphaned OPFS files.
// The lock name encodes the TAB_ID so other tabs can query which IDs are alive.
const TAB_LOCK_PREFIX = 'iom_tab_';
const TAB_LOCK_NAME = `${TAB_LOCK_PREFIX}${TAB_ID}`;

// Hold a Web Lock for this tab's lifetime. The lock is acquired via a
// never-resolving promise so it auto-releases when the tab closes/crashes.
// We store the ready promise so cleanup in other tabs won't treat our files as orphaned
// before our lock is visible in navigator.locks.query().
const lockReady = new Promise(resolve => {
  navigator.locks.request(TAB_LOCK_NAME, () => {
    resolve();
    return new Promise(() => {});
  });
});

// OPFS file/dir name prefixes — each followed by TAB_ID.
const OPFS_PREFIXES = getOpfsPrefixes();

function extractTabId(name) {
  for (const prefix of OPFS_PREFIXES) {
    if (name.startsWith(prefix)) return name.slice(prefix.length).split('_')[0];
  }
  return null;
}

async function cleanupOrphanedOpfsEntries() {
  try {
    const { held } = await navigator.locks.query();
    const aliveTabIds = new Set(
      held.filter(l => l.name.startsWith(TAB_LOCK_PREFIX)).map(l => l.name.slice(TAB_LOCK_PREFIX.length))
    );

    const root = await navigator.storage.getDirectory();
    let count = 0;
    for await (const [name, handle] of root) {
      const tabId = extractTabId(name);
      if (tabId && !aliveTabIds.has(tabId)) {
        try {
          await root.removeEntry(name, { recursive: handle.kind === 'directory' });
          count++;
        } catch (e) { /* may be locked or already removed */ }
      }
    }

    if (count > 0) console.log(`[PartialDownload] Cleaned up ${count} orphaned OPFS entries`);
  } catch (e) {
    console.warn('[PartialDownload] OPFS orphan cleanup failed:', e);
  }
}

cleanupOrphanedOpfsEntries();

// Memory config: 50% of device RAM, clamped to [512MB, maxMB], step 128MB
const MEMORY_STEP = 128;
const MEMORY_MIN_MB = 512;

export function getDeviceMaxMemoryMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  return Math.max(MEMORY_MIN_MB, Math.floor(deviceMemGB * 1024 * 0.75 / MEMORY_STEP) * MEMORY_STEP);
}

export function getDefaultMemoryLimitMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  const halfMB = Math.floor(deviceMemGB * 1024 * 0.5 / MEMORY_STEP) * MEMORY_STEP;
  return Math.max(MEMORY_MIN_MB, Math.min(halfMB, getDeviceMaxMemoryMB()));
}

export { MEMORY_STEP, MEMORY_MIN_MB };

export class PartialDownloadHandler {
  constructor() {
    this.initialized = false;
    this.cancelled = false;
    this.sizeGetter = new SizeGetter();
    this._duckdb = new DuckDBClient();
  }

  async _init(onStatus) {
    if (this.initialized) return;

    // Ensure our lock is visible before creating OPFS files,
    // so other tabs' cleanup won't treat them as orphaned.
    await lockReady;

    onStatus?.('Initializing DuckDB...');
    await this._duckdb.init();
    await this._duckdb.conn.query(`SET temp_directory = 'opfs://${OPFS_PREFIX_TMPDIR}${TAB_ID}'`);

    this.initialized = true;
    console.log('[PartialDownload] Ready');
  }

  cancel() {
    this.cancelled = true;
    // Cancel the format handler first (kills gpkg worker, sets cancelled flag, etc.)
    this._formatHandler?.cancel();
    // Terminate DuckDB so in-flight queries throw and write() finally blocks run
    this._duckdb.terminate();
    this.initialized = false;

    // Delayed fallback: reject the cancel promise to unblock Promise.race
    // if write() hasn't thrown yet despite DuckDB + worker termination
    setTimeout(() => {
      if (this._rejectCancel) {
        this._rejectCancel(new DOMException('Download cancelled', 'AbortError'));
        this._rejectCancel = null;
      }
    }, 1000);

    console.log('[PartialDownload] Cancelled — DuckDB worker terminated');
  }

  throwIfCancelled() {
    if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');
  }

  destroy() {
    this.cancel();
  }

  _getPartitionsForBbox(metaJson, bbox) {
    if (!metaJson.extents) return [];
    
    const partitions = [];
    for (const [filename, extent] of Object.entries(metaJson.extents)) {
      const [minx, miny, maxx, maxy] = extentBounds(extent);
      if (!(bbox.east < minx || bbox.west > maxx || 
            bbox.north < miny || bbox.south > maxy)) {
        partitions.push(filename);
      }
    }
    return partitions;
  }

  /**
   * Resolve routeUrl + bbox into an array of parquet URLs.
   * For partitioned sources, filters partitions by bbox overlap.
   */
  async _resolveParquetUrls(routeUrl, isPartitioned, bbox, onStatus) {
    if (!isPartitioned) {
      return [parquetMetadata.getParquetUrl(routeUrl)];
    }

    const metaUrl = parquetMetadata.getMetaJsonUrl(routeUrl);
    const baseUrl = parquetMetadata.getBaseUrl(routeUrl);
    const metaJson = await parquetMetadata.fetchMetaJson(metaUrl);

    if (!metaJson) {
      throw new Error('Could not load partition metadata');
    }

    const filteredPartitions = this._getPartitionsForBbox(metaJson, bbox);
    if (filteredPartitions.length === 0) {
      throw new Error('No data found in current bbox');
    }

    onStatus?.(`Found ${filteredPartitions.length} partition(s) in bbox...`);
    return filteredPartitions.map(p => baseUrl + p);
  }

  /**
   * Estimate download size by fetching file sizes (HEAD) and computing
   * the ratio of bbox overlap with each file's extent.
   */
  async _estimateSize(parquetUrls, bbox, routeUrl, isPartitioned) {
    const sizes = await Promise.all(
      parquetUrls.map(url => this.sizeGetter.getSizeBytes(url))
    );

    // Get per-file extents
    let fileExtents;
    if (isPartitioned) {
      const metaUrl = parquetMetadata.getMetaJsonUrl(routeUrl);
      const metaJson = await parquetMetadata.fetchMetaJson(metaUrl);
      fileExtents = metaJson?.extents ?? {};
    } else {
      const parquetBbox = await parquetMetadata.getParquetBbox(parquetUrls[0], this._duckdb);
      fileExtents = parquetBbox ? { [parquetUrls[0]]: parquetBbox } : {};
    }

    this.throwIfCancelled();

    let totalEstimate = 0;

    for (let i = 0; i < parquetUrls.length; i++) {
      const fileSize = sizes[i];
      if (!fileSize) continue;

      // Find matching extent — for partitioned, key is the filename portion
      const filename = parquetUrls[i].split('/').pop();
      const extent = fileExtents[filename] || fileExtents[parquetUrls[i]];
      const ratio = bboxOverlapRatio(extent, bbox);
      totalEstimate += fileSize * (ratio ?? 1);
    }

    return Math.round(totalEstimate);
  }

  /**
   * Generate suggested filename for download.
   */
  _getDownloadBaseName(sourceName, bbox) {
    const coordStr = [bbox.west, bbox.south, bbox.east, bbox.north]
      .map(c => c.toFixed(4).replace(/\./g, '-'))
      .join('--');
    return `${sourceName.replace(/\s+/g, '_')}.${coordStr}`;
  }

  /**
   * Prepare for download: init DuckDB, resolve URLs, estimate size, create format handler.
   * Returns the format handler for the caller to inspect (e.g. estimatedOutputBytes)
   * before committing to the download.
   */
  async prepare(options) {
    const { routeUrl, isPartitioned, bbox, format, onProgress, onStatus, memoryLimit } = options;

    this.cancelled = false;

    // Set up cancellation promise early — cancel() rejects this to unblock
    // any hung async operation (DuckDB init, queries, etc.)
    this._cancelPromise = new Promise((_, reject) => {
      this._rejectCancel = reject;
    });

    await Promise.race([this._init(onStatus), this._cancelPromise]);

    this.throwIfCancelled();

    // Apply memory limit (can change between downloads)
    if (memoryLimit) {
      await this._duckdb.conn.query(`SET memory_limit = '${memoryLimit}'`);
    }

    onProgress?.(0);

    // Resolve parquet URLs from route config
    const parquetUrls = await this._resolveParquetUrls(routeUrl, isPartitioned, bbox, onStatus);

    this.throwIfCancelled();

    onStatus?.(`Estimating download size from ${parquetUrls.length} file(s)...`);
    const estimatedBytes = await this._estimateSize(parquetUrls, bbox, routeUrl, isPartitioned);

    this.throwIfCancelled();

    onProgress?.(PROGRESS_WRITE_START);

    const urls = parquetUrls.map(u => proxyUrl(u, { absolute: true }));

    this._formatHandler = getFormatHandler(format, {
      tabId: TAB_ID, duckdb: this._duckdb,
      urls, bbox, estimatedBytes
    });
    return this._formatHandler;
  }

  /**
   * Execute the download using a previously prepared format handler.
   */
  async download(formatHandler, { sourceName, bbox, onProgress, onStatus }) {
    try {
      const writeProgress = new ScopedProgress(onProgress, PROGRESS_WRITE_START, PROGRESS_WRITE_END);

      await Promise.race([
        formatHandler.write({
          onProgress: writeProgress.callback,
          onStatus,
        }),
        this._cancelPromise
      ]);

      this.throwIfCancelled();

      onProgress?.(PROGRESS_WRITE_END);

      const baseName = this._getDownloadBaseName(sourceName, bbox);

      onStatus?.('Saving file...');
      await formatHandler.triggerDownload(baseName, DOWNLOAD_CLEANUP_DELAY_MS);

      onProgress?.(100);

      return true;

    } catch (e) {
      // Worker termination from cancel() throws a generic error, not AbortError
      this.throwIfCancelled();
      throw e;
    } finally {
      this._rejectCancel = null;
      this._cancelPromise = null;
      this._formatHandler = null;
      await formatHandler.cleanup();
    }
  }
}
