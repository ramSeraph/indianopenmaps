// Partial download handler using DuckDB WASM with OPFS
// Writes to OPFS via COPY TO, then triggers download via blob URL

const DUCKDB_BASE = 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir';
// JS API from custom build with OPFS temp directory spillover support
import * as duckdb from 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir/duckdb-browser.mjs';
import { buildCopyQuery as buildCsvCopyQuery } from './format_csv.js';
import { buildCopyQuery as buildGeoJsonCopyQuery } from './format_geojson.js';
import { writeGeoParquet } from './format_geoparquet.js';

// Unique tab ID to avoid OPFS conflicts between tabs
const TAB_ID = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Trigger download from an OPFS file via blob URL.
// File objects from OPFS are disk-backed; createObjectURL just creates a
// reference — the browser streams from disk on demand. No RAM copy.
async function triggerDownload(opfsFileName, downloadFileName, wrapGeoJson = false) {
  const root = await navigator.storage.getDirectory();
  const handle = await root.getFileHandle(opfsFileName);
  const file = await handle.getFile();

  // For GeoJSON, wrap newline-delimited features in a FeatureCollection.
  // Blob([parts...]) is lazy — parts are concatenated on read, not upfront.
  const blob = wrapGeoJson
    ? new Blob(
        ['{"type":"FeatureCollection","features":[\n', file, '\n]}'],
        { type: 'application/geo+json' }
      )
    : file;

  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = downloadFileName;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  // Revoke after a delay so the browser's download manager can finish reading
  setTimeout(() => URL.revokeObjectURL(url), 120000);
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
    this.currentOpfsPath = null;
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

  isDownloading() {
    return this.currentDownload !== null;
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

  getFormatInfo(format) {
    switch (format) {
      case 'geojson':
        return {
          ext: '.geojson',
          description: 'GeoJSON',
          accept: { 'application/geo+json': ['.geojson', '.json'] }
        };
      case 'geojsonseq':
        return {
          ext: '.geojsonl',
          description: 'GeoJSON Sequence',
          accept: { 'application/geo+json-seq': ['.geojsonl', '.geojson'] }
        };
      case 'csv':
        return {
          ext: '.csv',
          description: 'CSV with WKT geometry',
          accept: { 'text/csv': ['.csv'] }
        };
      case 'geoparquet':
        return {
          ext: '.parquet',
          description: 'GeoParquet',
          accept: { 'application/x-parquet': ['.parquet'] }
        };
      default:
        throw new Error(`Unsupported format: ${format}`);
    }
  }

  /**
   * Generate suggested filename for download.
   */
  getSuggestedFileName(sourceName, bbox, format) {
    const formatInfo = this.getFormatInfo(format);
    const coordStr = [bbox.west, bbox.south, bbox.east, bbox.north]
      .map(c => c.toFixed(4).replace(/\./g, '-'))
      .join('--');
    const baseName = sourceName.replace(/\s+/g, '_');
    return `${baseName}.${coordStr}${formatInfo.ext}`;
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

    this.cancelled = false;
    this.currentDownload = { sourceName, bbox };
    const formatInfo = this.getFormatInfo(format);

    // OPFS temp file path
    const opfsPath = `opfs://partial_${TAB_ID}_${Date.now()}${formatInfo.ext}`;
    this.currentOpfsPath = opfsPath;

    try {
      onProgress?.(5);
      onStatus?.('Preparing...');

      // Register the OPFS file name with DuckDB and wait for it to be ready
      await this.db.registerOPFSFileName(opfsPath);
      await sleep(5);

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

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(10);
      onStatus?.(`Filtering ${urls.length} file(s) and writing to OPFS...`);

      const urlList = urls.map(u => `'${u}'`).join(', ');
      const bboxWkt = `POLYGON((${bbox.west} ${bbox.south}, ${bbox.east} ${bbox.south}, ${bbox.east} ${bbox.north}, ${bbox.west} ${bbox.north}, ${bbox.west} ${bbox.south}))`;

      // Build and execute format-specific COPY query
      if (format === 'geoparquet') {
        await writeGeoParquet(this.conn, this.db, urlList, bboxWkt, opfsPath, TAB_ID, {
          onStatus,
          cancelled: () => this.cancelled,
        });
      } else {
        let copyQuery;
        if (format === 'csv') {
          copyQuery = buildCsvCopyQuery(urlList, bboxWkt, opfsPath);
        } else {
          copyQuery = await buildGeoJsonCopyQuery(this.conn, urlList, bboxWkt, opfsPath);
        }
        await this.conn.query(copyQuery);
      }

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(70);
      onStatus?.('Streaming to your file...');

      // Drop the DuckDB registration so we can access the OPFS file directly
      await this.db.dropFile(opfsPath);
      this.currentOpfsPath = null;

      const opfsFileName = opfsPath.replace('opfs://', '');
      const downloadFileName = this.getSuggestedFileName(sourceName, bbox, format);

      // Download via blob URL from OPFS file (disk-backed, no RAM copy).
      onStatus?.('Saving file...');
      await triggerDownload(opfsFileName, downloadFileName, format === 'geojson');
      // OPFS cleanup deferred — browser may still be streaming from the file
      setTimeout(async () => {
        try {
          const root = await navigator.storage.getDirectory();
          await root.removeEntry(opfsFileName);
        } catch (e) { /* ignore */ }
      }, 120000);

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
      await this.cleanup();
      this.currentDownload = null;
    }
  }

  async cleanup() {
    try {
      if (this.currentOpfsPath) {
        try {
          await this.db.dropFile(this.currentOpfsPath);
        } catch (e) {
          // May already be dropped
        }
      }
    } catch (e) {
      // Ignore cleanup errors
    } finally {
      this.currentOpfsPath = null;
    }
  }

  async dispose() {
    this.cancel();
    await this.cleanup();
    if (this.conn) {
      await this.conn.close();
      this.conn = null;
    }
    if (this.db) {
      await this.db.terminate();
      this.db = null;
    }
    this.initialized = false;
  }
}
