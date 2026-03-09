// Partial download handler — uses shared DuckDB client for queries,
// manages OPFS temp directories and download lifecycle.

import { duckdbClient } from './duckdb_client.js';
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

// Web Lock held for this tab's lifetime — used to detect orphaned OPFS files.
// The lock name encodes the TAB_ID so other tabs can query which IDs are alive.
const TAB_LOCK_NAME = `iom_tab_${TAB_ID}`;
// Hold a Web Lock for this tab's lifetime. The lock is acquired via a
// never-resolving promise so it auto-releases when the tab closes/crashes.
// We store the ready promise so orphan cleanup can wait for it.
const lockReady = new Promise(resolve => {
  navigator.locks.request(TAB_LOCK_NAME, () => {
    resolve();
    return new Promise(() => {});
  });
});

// OPFS files/dirs created by this module use these prefixes followed by TAB_ID.
const OPFS_PREFIXES = ['temp_gpkg_', 'partial_', 'temp_', 'gpkg_', 'tmp_'];

function extractTabId(name) {
  for (const prefix of OPFS_PREFIXES) {
    if (name.startsWith(prefix)) return name.slice(prefix.length).split('_').slice(0, 2).join('_');
  }
  return null;
}

async function cleanupOrphanedOpfsEntries() {
  try {
    // Wait until our own lock is held so it shows up in locks.query()
    await lockReady;

    const { held } = await navigator.locks.query();
    const aliveTabIds = new Set(
      held.filter(l => l.name.startsWith('iom_tab_')).map(l => l.name.slice('iom_tab_'.length))
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
    this.currentDownload = null;
  }

  async init() {
    if (this.initialized) return;

    await duckdbClient.init();
    this.tempDirSeq = 0;
    await duckdbClient.conn.query(`SET temp_directory = 'opfs://tmp_${TAB_ID}_${this.tempDirSeq}'`);

    this.initialized = true;
    console.log('[PartialDownload] Ready');
  }

  cancel() {
    this.cancelled = true;
    this.currentDownload = null;
  }

  async rotateTempDir() {
    try {
      const oldDirName = `tmp_${TAB_ID}_${this.tempDirSeq}`;
      this.tempDirSeq++;
      await duckdbClient.conn.query(`SET temp_directory = 'opfs://tmp_${TAB_ID}_${this.tempDirSeq}'`);
      const root = await navigator.storage.getDirectory();
      await root.removeEntry(oldDirName, { recursive: true });
    } catch (e) { /* dir may not exist or already removed */ }
  }

  getPartitionsForBbox(metaJson, bbox) {
    if (!metaJson.extents) return [];
    
    const partitions = [];
    for (const [filename, extent] of Object.entries(metaJson.extents)) {
      const [minx, miny, maxx, maxy] = Array.isArray(extent)
        ? extent
        : [extent.minx, extent.miny, extent.maxx, extent.maxy];
      if (!(bbox.east < minx || bbox.west > maxx || 
            bbox.north < miny || bbox.south > maxy)) {
        partitions.push(filename);
      }
    }
    return partitions;
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
      await duckdbClient.conn.query(`SET memory_limit = '${memoryLimit}'`);
    }
    await duckdbClient.conn.query('SET arrow_large_buffer_size=true');

    this.cancelled = false;
    this.currentDownload = { sourceName, bbox };
    let handler = null;

    try {
      onProgress?.(5);
      onStatus?.('Preparing...');

      // Build list of URLs to query
      let urls = [];
      if (parquetUrl) {
        urls = [duckdbClient.buildProxyUrl(parquetUrl)];
      } else if (partitions && partitions.length > 0) {
        urls = partitions.map(p => duckdbClient.buildProxyUrl(baseUrl + p));
      }

      if (urls.length === 0) {
        throw new Error('No parquet files to query');
      }

      handler = getFormatHandler(format, { tabId: TAB_ID, conn: duckdbClient.conn, db: duckdbClient.db, urls, bbox });
      await handler.prepareOpfs();

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(10);
      onStatus?.(`Filtering ${urls.length} file(s) and writing to OPFS...`);

      await handler.write({
        onStatus,
        cancelled: () => this.cancelled,
      });

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(70);
      onStatus?.('Streaming to your file...');

      await handler.releaseOpfs();

      const downloadFileName = this.getSuggestedFileName(sourceName, bbox, handler.extension);

      onStatus?.('Saving file...');
      await handler.triggerDownload(downloadFileName, DOWNLOAD_CLEANUP_DELAY_MS);

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
      await this.rotateTempDir();
      this.currentDownload = null;
    }
  }
}
