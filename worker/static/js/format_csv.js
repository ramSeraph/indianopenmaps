// CSV format handler for partial downloads

import { FormatHandler } from './format_base.js';

export class CsvFormatHandler extends FormatHandler {
  get extension() { return '.csv'; }

  async write() {
    await this.conn.query(`
      COPY (
        SELECT * EXCLUDE (geometry), ST_AsText(geometry) as geometry_wkt
        FROM ${this.parquetSource}
        WHERE ${this.bboxFilter}
      ) TO '${this._opfsPath}' (FORMAT CSV, HEADER true)
    `);
  }
}
