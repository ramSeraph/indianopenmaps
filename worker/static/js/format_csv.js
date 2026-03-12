// CSV format handler for partial downloads

import { FormatHandler } from './format_base.js';

export class CsvFormatHandler extends FormatHandler {
  get extension() { return '.csv'; }

  getExpectedBrowserStorageUsage() { return this.estimatedBytes * 10; }
  getTotalExpectedDiskUsage() { return this.estimatedBytes * 20; }

  async _write({ onProgress, onStatus }) {
    const stopTracker = this.startDiskProgressTracker(
      onProgress, onStatus, 'Writing CSV:', this.getExpectedBrowserStorageUsage()
    );

    try {
      await this.conn.query(`
        COPY (
          SELECT * EXCLUDE (geometry, bbox), ST_AsText(geometry) as geometry_wkt
          FROM ${this.parquetSource}
          WHERE ${this.bboxFilter}
        ) TO '${this._opfsPath}' (FORMAT CSV, HEADER true)
      `);
    } finally {
      stopTracker();
    }

    onProgress?.(100);
  }
}
