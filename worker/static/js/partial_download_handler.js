// Partial download handler using DuckDB WASM with OPFS
// Writes to OPFS via COPY TO, then triggers download via blob URL

const DUCKDB_BASE = 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir';
// JS API from custom build with OPFS temp directory spillover support
import * as duckdb from 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir/duckdb-browser.mjs';
import { CsvFormatHandler } from './format_csv.js';
import { GeoJsonFormatHandler } from './format_geojson.js';
import { GeoParquetFormatHandler } from './format_geoparquet.js';
import { GeoPackageFormatHandler } from './format_geopackage.js';

function getFormatHandler(format, opts) {
  switch (format) {
    case 'csv': return new CsvFormatHandler(opts);
    case 'geojson': return new GeoJsonFormatHandler({ commaSeparated: true, ...opts });
    case 'geojsonseq': return new GeoJsonFormatHandler({ commaSeparated: false, ...opts });
    case 'geoparquet': return new GeoParquetFormatHandler({ version: '1.1', ...opts });
    case 'geoparquet2': return new GeoParquetFormatHandler({ version: '2.0', ...opts });
    case 'geopackage': return new GeoPackageFormatHandler(opts);
    default: throw new Error(`Unsupported format: ${format}`);
  }
}

// Delay before revoking blob URLs / cleaning up OPFS files, so the browser can finish streaming.
// There's no browser event for "blob URL download finished." The File System Access API
// (showSaveFilePicker) would give explicit completion, but is Chrome/Edge only.
const DOWNLOAD_CLEANUP_DELAY_MS = 120000;

// Unique tab ID to avoid OPFS conflicts between tabs
const TAB_ID = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

// Trigger download from an OPFS file via blob URL.
// File objects from OPFS are disk-backed; createObjectURL just creates a
// reference — the browser streams from disk on demand. No RAM copy.
async function triggerDownload(blobParts, downloadFileName) {
  const blob = new Blob(blobParts);

  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = downloadFileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  // Revoke after a delay so the browser's download manager can finish reading
  setTimeout(() => URL.revokeObjectURL(url), DOWNLOAD_CLEANUP_DELAY_MS);
}

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
    this.db = null;
    this.conn = null;
    this.initialized = false;
    this.cancelled = false;
    this.currentDownload = null;
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
      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
      
      this.db = new duckdb.AsyncDuckDB(logger, worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      URL.revokeObjectURL(worker_url);
      
      this.conn = await this.db.connect();
      await this.conn.query(`SET temp_directory = 'opfs://tmp_${TAB_ID}'`);
      await this.conn.query(`INSTALL spatial; LOAD spatial;`);
      
      this.initialized = true;
      console.log('[PartialDownload] DuckDB WASM initialized');
    } catch (error) {
      console.error('[PartialDownload] Failed to initialize DuckDB:', error);
      throw error;
    }
  }

  cancel() {
    this.cancelled = true;
    this.currentDownload = null;
  }

  getPartitionsForBbox(metaJson, bbox) {
    if (!metaJson.extents) return [];
    
    const partitions = [];
    for (const [filename, extent] of Object.entries(metaJson.extents)) {
      const [minx, miny, maxx, maxy] = extent;
      if (!(bbox.east < minx || bbox.west > maxx || 
            bbox.north < miny || bbox.south > maxy)) {
        partitions.push(filename);
      }
    }
    return partitions;
  }

  buildProxyUrl(url) {
    const origin = window.location.origin;
    return `${origin}/proxy?url=${encodeURIComponent(url)}`;
  }

  /**
   * Generate suggested filename for download.
   */
  getSuggestedFileName(sourceName, bbox, extension) {
    const coordStr = [bbox.west, bbox.south, bbox.east, bbox.north]
      .map(c => c.toFixed(4).replace(/\./g, '-'))
      .join('--');
    const baseName = sourceName.replace(/\s+/g, '_');
    return `${baseName}.${coordStr}${extension}`;
  }

  /**
   * 1. COPY TO opfs:// temp file (large file stays on disk via OPFS)
   * 2. copyFileToBuffer to read it back
   * 3. Stream to download via service worker
   */
  async download(options) {
    const { sourceName, parquetUrl, baseUrl, partitions, bbox, format, onProgress, onStatus, memoryLimit } = options;

    if (!this.initialized) {
      onStatus?.('Initializing DuckDB...');
      await this.init();
    }

    // Apply memory limit (can change between downloads)
    if (memoryLimit) {
      await this.conn.query(`SET memory_limit = '${memoryLimit}'`);
    }
    await this.conn.query('SET arrow_large_buffer_size=true');

    this.cancelled = false;
    this.currentDownload = { sourceName, bbox };
    let handler = null;

    try {
      onProgress?.(5);
      onStatus?.('Preparing...');

      // Build list of URLs to query
      let urls = [];
      if (parquetUrl) {
        urls = [this.buildProxyUrl(parquetUrl)];
      } else if (partitions && partitions.length > 0) {
        urls = partitions.map(p => this.buildProxyUrl(baseUrl + p));
      }

      if (urls.length === 0) {
        throw new Error('No parquet files to query');
      }

      handler = getFormatHandler(format, { tabId: TAB_ID, conn: this.conn, db: this.db, urls, bbox });
      await handler.prepareOpfs();

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(10);
      onStatus?.(`Filtering ${urls.length} file(s) and writing to OPFS...`);

      const result = await handler.write({
        onStatus,
        cancelled: () => this.cancelled,
      });

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(70);
      onStatus?.('Streaming to your file...');

      await handler.releaseOpfs();

      const opfsFileName = handler.outputFileName;
      const downloadFileName = this.getSuggestedFileName(sourceName, bbox, handler.extension);

      const root = await navigator.storage.getDirectory();
      const handle = await root.getFileHandle(opfsFileName);
      const file = await handle.getFile();
      const blobParts = handler.wrapBlobParts(file);

      // Download via blob URL from OPFS file (disk-backed, no RAM copy).
      onStatus?.('Saving file...');
      await triggerDownload(blobParts, downloadFileName);
      // OPFS cleanup deferred — browser may still be streaming from the file
      setTimeout(async () => {
        try {
          const root = await navigator.storage.getDirectory();
          await root.removeEntry(opfsFileName);
        } catch (e) { /* ignore */ }
      }, DOWNLOAD_CLEANUP_DELAY_MS);

      onProgress?.(100);
      onStatus?.('Download complete!');

      return true;

    } catch (error) {
      if (error.name === 'AbortError') {
        onStatus?.('Download cancelled');
      } else {
        console.error('[PartialDownload] Download failed:', error);
        onStatus?.(`Error: ${error.message}`);
      }
      throw error;
    } finally {
      await handler?.cleanup();
      this.currentDownload = null;
    }
  }
}
