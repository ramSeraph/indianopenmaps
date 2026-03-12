// Base class for partial download format handlers

import { OPFS_PREFIX_OUTPUT, getStorageEstimate, formatSize } from './utils.js';

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export class FormatHandler {
  constructor({ tabId, conn, db, urls, bbox, estimatedBytes } = {}) {
    this.tabId = tabId;
    this.conn = conn;
    this.db = db;
    this.urls = urls || [];
    this.bbox = bbox;
    this.estimatedBytes = estimatedBytes ?? null;
    this._opfsPath = null;
    this._prepared = false;
    this._outputFileName = null;
    this._downloadTriggered = false;
  }

  get extension() { throw new Error('Not implemented'); }
  get needsDuckDBRegistration() { return true; }

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

  async createTempOpfsFile(prefix) {
    const path = `opfs://${prefix}${this.tabId}_${Date.now()}.parquet`;
    await this.db.registerOPFSFileName(path);
    await sleep(5);
    return path;
  }

  async releaseTempFile(opfsFileName) {
    try {
      const root = await navigator.storage.getDirectory();
      await root.removeEntry(opfsFileName);
    } catch (e) { /* may already be cleaned up */ }
  }

  async prepareOpfs() {
    this._opfsPath = `opfs://${OPFS_PREFIX_OUTPUT}${this.tabId}_${Date.now()}${this.extension}`;
    this._outputFileName = this._opfsPath.replace('opfs://', '');
    if (this.needsDuckDBRegistration) {
      await this.db.registerOPFSFileName(this._opfsPath);
      this._prepared = true;
      await sleep(5);
    }
  }

  async releaseOpfs() {
    if (this._prepared && this.db) {
      try {
        await this.db.dropFile(this._opfsPath);
      } catch (e) { /* db may have been terminated */ }
      this._prepared = false;
    }
  }

  get outputFileName() { return this._outputFileName; }

  wrapBlobParts(file) { return [file]; }

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

  async triggerDownload(downloadFileName, cleanupDelayMs) {
    const root = await navigator.storage.getDirectory();
    const handle = await root.getFileHandle(this._outputFileName);
    const file = await handle.getFile();
    const blobParts = this.wrapBlobParts(file);

    const blob = new Blob(blobParts);
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = downloadFileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);

    this._downloadTriggered = true;

    const outputFileName = this._outputFileName;
    setTimeout(async () => {
      URL.revokeObjectURL(url);
      try {
        const root = await navigator.storage.getDirectory();
        await root.removeEntry(outputFileName);
      } catch (e) { /* ignore */ }
    }, cleanupDelayMs);
  }

  async write(callbacks) {
    // callbacks: { onProgress(0–100), onStatus(msg), cancelled() }
    await this.prepareOpfs();
    try {
      await this._write(callbacks);
    } finally {
      // Only release DuckDB's file handle if not cancelled — on cancellation
      // the db worker is terminated and dropFile would hang. cleanup() handles
      // OPFS file removal directly without DuckDB.
      if (!callbacks?.cancelled?.()) {
        await this.releaseOpfs();
      } else {
        this._prepared = false;
      }
    }
  }

  async _write(_callbacks) {
    throw new Error('Not implemented');
  }

  async cleanup() {
    this._prepared = false;

    // Sweep all OPFS files belonging to this tab, except the output file if download was triggered
    const root = await navigator.storage.getDirectory();
    for await (const [name] of root) {
      if (!name.includes(this.tabId)) continue;
      if (this._downloadTriggered && name === this._outputFileName) continue;
      try {
        await root.removeEntry(name, { recursive: true });
      } catch (e) { /* may already be cleaned up */ }
    }
    this._opfsPath = null;
    this._outputFileName = null;
  }
}
