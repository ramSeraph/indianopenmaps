// CSV format handler for partial downloads

import { FormatHandler } from './format_base.js';
import { OPFS_PREFIX_OUTPUT } from './utils.js';

export class CsvFormatHandler extends FormatHandler {
  constructor({ ...opts } = {}) {
    super(opts);
    this.opfsPath = null;
    this.extension = 'csv';
  }

  getExpectedBrowserStorageUsage() { return this.estimatedBytes * 10; }
  getTotalExpectedDiskUsage() { return this.estimatedBytes * 20; }

  async _write({ onProgress, onStatus }) {
    const stopTracker = this.startDiskProgressTracker(
      onProgress, onStatus, 'Writing CSV:', this.getExpectedBrowserStorageUsage()
    );

    this.opfsPath = await this.createDuckdbOpfsFile(OPFS_PREFIX_OUTPUT, this.extension);
    try {
      await this.duckdb.conn.query(`
        COPY (
          SELECT * EXCLUDE (geometry, bbox), ST_AsText(geometry) as geometry_wkt
          FROM ${this.parquetSource}
          WHERE ${this.bboxFilter}
        ) TO '${this.opfsPath}' (FORMAT CSV, HEADER true)
      `);
    } finally {
      stopTracker();
    }

    await this.releaseDuckdbOpfsFile(this.opfsPath);
    onProgress?.(100);
  }

  async getDownloadMap(baseName) {
    const file = await this.getOpfsFile(this.opfsPath);
    return [{ downloadName: `${baseName}.${this.extension}`, blobParts: [file] }];
  }
}
