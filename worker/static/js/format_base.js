// Base class for partial download format handlers

import { getStorageEstimate, formatSize } from './utils.js';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export class FormatHandler {
  constructor({ tabId, duckdb, urls, bbox, estimatedBytes } = {}) {
    this.tabId = tabId;
    this.duckdb = duckdb;
    this.urls = urls || [];
    this.bbox = bbox;
    this.estimatedBytes = estimatedBytes ?? null;
    this._duckdbRegisteredPaths = new Set();
    this._prepared = false;
    this._downloadedFiles = new Set();
  }

  getExpectedBrowserStorageUsage() { throw new Error('Not implemented'); }
  getTotalExpectedDiskUsage() { throw new Error('Not implemented'); }

  /**
   * Start a background interval that polls disk usage and reports progress
   * based on estimated output size. Returns a stop function.
   */
  startDiskProgressTracker(onProgress, onStatus, messagePrefix, expectedBytes, intervalMs = 5000) {
    if (!expectedBytes || expectedBytes <= 0) return () => {};

    let baselineUsage = null;
    let stopped = false;
    let lastPct = 0;

    const poll = async () => {
      if (stopped) return;
      const { usage: currentUsage } = await getStorageEstimate();
      if (baselineUsage === null) {
        baselineUsage = currentUsage;
        return;
      }
      const written = Math.max(0, currentUsage - baselineUsage);
      const pct = Math.min(100, (written / expectedBytes) * 100);
      if (pct > lastPct) lastPct = pct;
      onProgress?.(lastPct);
      onStatus?.(`${messagePrefix} ~${formatSize(written)} / ~${formatSize(expectedBytes)}`);
    };

    // Capture baseline immediately
    poll();
    const id = setInterval(poll, intervalMs);

    return () => {
      stopped = true;
      clearInterval(id);
    };
  }

  get parquetSource() {
    const urlList = this.urls.map(u => `'${u}'`).join(', ');
    return `read_parquet([${urlList}], union_by_name=true)`;
  }

  async createDuckdbOpfsFile(prefix, ext) {
    const path = `opfs://${prefix}${this.tabId}_${Date.now()}.${ext}`;
    await this.duckdb.db.registerOPFSFileName(path);
    await sleep(5);
    this._duckdbRegisteredPaths.add(path);
    return path;
  }

  async removeOpfsFile(opfsFileName) {
    try {
      const root = await navigator.storage.getDirectory();
      await root.removeEntry(opfsFileName);
    } catch (e) { /* may already be cleaned up */ }
  }

  async releaseDuckdbOpfsFile(opfsFileName) {
    try {
      await this.duckdb.db.dropFile(opfsFileName);
    } catch (e) { /* db terminated or file already gone */ }
    this._duckdbRegisteredPaths.delete(opfsFileName);
  }

  async releaseDuckDbOpfsFiles() {
    const paths = Array.from(this._duckdbRegisteredPaths);

    for (const path of paths) {
      await this.releaseDuckdbOpfsFile(path);
    }
  }

  async getOpfsFile(opfsPath) {
    const root = await navigator.storage.getDirectory();
    const handle = await root.getFileHandle(opfsPath.replace('opfs://', ''));
    return await handle.getFile();
  }

  /**
   * Return a list of { downloadName, blobParts } for triggerDownload.
   * blobParts may contain File objects (from OPFS) and Uint8Array headers.
   * OPFS files to clean up are derived from File objects in blobParts.
   * Subclasses override to provide multi-file or wrapped downloads.
   */
  async getDownloadMap(baseName) {
    throw new Error('Not implemented');
  }

  get bboxWkt() {
    return `POLYGON((${this.bbox.west} ${this.bbox.south}, ${this.bbox.east} ${this.bbox.south}, ${this.bbox.east} ${this.bbox.north}, ${this.bbox.west} ${this.bbox.north}, ${this.bbox.west} ${this.bbox.south}))`;
  }

  get bboxFilter() {
    const { west, south, east, north } = this.bbox;
    // bbox column filter enables row group skipping via parquet column statistics;
    // ST_Intersects does precise geometry filtering on surviving rows
    const bboxRowGroupFilter = `bbox.xmin <= ${east} AND bbox.xmax >= ${west} AND bbox.ymin <= ${north} AND bbox.ymax >= ${south}`;
    return `${bboxRowGroupFilter} AND ST_Intersects(geometry, ST_GeomFromText('${this.bboxWkt}'))`;
  }

  async triggerDownload(baseName, cleanupDelayMs) {
    const entries = await this.getDownloadMap(baseName);

    for (const { downloadName, blobParts } of entries) {
      const blob = new Blob(blobParts);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = downloadName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);

      // Derive OPFS files to clean up from File objects in blobParts
      const opfsFiles = blobParts.filter(p => p instanceof File).map(f => f.name);
      for (const f of opfsFiles) this._downloadedFiles.add(f);

      // Clean up blob URL and OPFS files after delay
      setTimeout(async () => {
        URL.revokeObjectURL(url);
        const root = await navigator.storage.getDirectory();
        for (const name of opfsFiles) {
          try { await root.removeEntry(name); } catch (e) { /* ignore */ }
        }
      }, cleanupDelayMs);

      // Small delay between downloads so browser doesn't block them
      if (entries.length > 1) await new Promise(r => setTimeout(r, 200));
    }
  }

  async write(callbacks) {
    // callbacks: { onProgress(0–100), onStatus(msg), cancelled() }
    try {
      await this._write(callbacks);
    } finally {
      // Safety net: release any DuckDB file handles not already released by _write().
      // On cancellation, dropFile throws AbortError which releaseDuckdbOpfsFile eats.
      await this.releaseDuckDbOpfsFiles();
    }
  }

  async _write(_callbacks) {
    throw new Error('Not implemented');
  }

  async cleanup() {
    // Sweep all OPFS files belonging to this tab, except files already handed to delayed cleanup
    const root = await navigator.storage.getDirectory();
    for await (const [name] of root) {
      if (!name.includes(this.tabId)) continue;
      if (this._downloadedFiles.has(name)) continue;
      try {
        await root.removeEntry(name, { recursive: true });
      } catch (e) { /* may already be cleaned up */ }
    }
  }
}
