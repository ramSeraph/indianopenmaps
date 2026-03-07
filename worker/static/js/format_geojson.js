// GeoJSON / GeoJSONSeq format handler for partial downloads

import { FormatHandler } from './format_base.js';

export class GeoJsonFormatHandler extends FormatHandler {
  constructor({ commaSeparated = false, ...opts } = {}) {
    super(opts);
    this.commaSeparated = commaSeparated;
  }

  get extension() { return this.commaSeparated ? '.geojson' : '.geojsonl'; }

  async write() {
    // Discover non-geometry columns
    const schemaResult = await this.conn.query(
      `SELECT column_name FROM (DESCRIBE SELECT * FROM ${this.parquetSource}) WHERE column_name != 'geometry'`
    );
    const propCols = [];
    for (let i = 0; i < schemaResult.numRows; i++) {
      propCols.push(schemaResult.getChildAt(0).get(i));
    }
    const structEntries = propCols.map(c => `'${c}', "${c}"`).join(', ');

    const jsonExpr = `json_object(
        'type', 'Feature',
        'geometry', ST_AsGeoJSON(geometry)::JSON,
        'properties', json_object(${structEntries})
      )`;

    // For GeoJSON FeatureCollection, prepend comma to all rows except the first
    const selectExpr = this.commaSeparated
      ? `CASE WHEN ROW_NUMBER() OVER () > 1 THEN ',' ELSE '' END || ${jsonExpr}`
      : jsonExpr;

    const featureQuery = `
      SELECT ${selectExpr} as feature
      FROM ${this.parquetSource}
      WHERE ${this.bboxFilter}
    `;

    await this.conn.query(`
      COPY (${featureQuery}) TO '${this._opfsPath}' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER E'\\x01')
    `);
  }

  wrapBlobParts(file) {
    if (this.commaSeparated) {
      return ['{"type":"FeatureCollection","features":[\n', file, ']}'];
    }
    return [file];
  }
}
