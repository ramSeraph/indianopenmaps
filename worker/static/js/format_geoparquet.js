// GeoParquet format handler for partial downloads
// 3-step pipeline: write temp → compute metadata → re-copy with Hilbert sort
// v2.0 support inspired by https://github.com/geoparquet/geoparquet-io

import { FormatHandler, sleep } from './format_base.js';

// DuckDB returns uppercase; GeoParquet spec requires title case
const DUCKDB_TO_SPEC = {
  'POINT': 'Point', 'LINESTRING': 'LineString', 'POLYGON': 'Polygon',
  'MULTIPOINT': 'MultiPoint', 'MULTILINESTRING': 'MultiLineString', 'MULTIPOLYGON': 'MultiPolygon',
  'GEOMETRYCOLLECTION': 'GeometryCollection',
};

const VERSION_CONFIG = {
  '1.1': {
    metadataVersion: '1.1.0',
    geomExpr: 'ST_AsWKB(geometry)::BLOB AS geometry',
    geomRef: 'ST_GeomFromWKB(geometry)',
    bboxSelect: `,
          struct_pack(
            xmin := ST_XMin(geometry),
            ymin := ST_YMin(geometry),
            xmax := ST_XMax(geometry),
            ymax := ST_YMax(geometry)
          ) AS bbox`,
    getBboxQuery: (path) =>
      `SELECT MIN(bbox.xmin) as xmin, MIN(bbox.ymin) as ymin,
              MAX(bbox.xmax) as xmax, MAX(bbox.ymax) as ymax
       FROM '${path}'`,
    getColumnMeta: (geomTypes, geoBbox) => ({
      encoding: 'WKB',
      geometry_types: geomTypes,
      bbox: geoBbox,
      covering: {
        bbox: {
          xmin: ['bbox', 'xmin'], ymin: ['bbox', 'ymin'],
          xmax: ['bbox', 'xmax'], ymax: ['bbox', 'ymax']
        }
      }
    }),
  },
  '2.0': {
    metadataVersion: '2.0.0',
    geomExpr: 'ST_AsWKB(geometry) AS geometry',
    geomRef: 'geometry',
    bboxSelect: '',
    getBboxQuery: (path) =>
      `SELECT ST_XMin(ST_Extent_Agg(geometry)) as xmin, ST_YMin(ST_Extent_Agg(geometry)) as ymin,
              ST_XMax(ST_Extent_Agg(geometry)) as xmax, ST_YMax(ST_Extent_Agg(geometry)) as ymax
       FROM '${path}' WHERE geometry IS NOT NULL`,
    getColumnMeta: (geomTypes, geoBbox) => ({
      encoding: 'WKB',
      geometry_types: geomTypes,
      bbox: geoBbox,
    }),
  },
};

export class GeoParquetFormatHandler extends FormatHandler {
  constructor({ version = '1.1', ...opts } = {}) {
    super(opts);
    this.versionConfig = VERSION_CONFIG[version];
    if (!this.versionConfig) throw new Error(`Unsupported GeoParquet version: ${version}`);
  }

  get extension() { return '.parquet'; }

  async write({ onStatus, cancelled }) {
    const cfg = this.versionConfig;

    // Step 1: Write initial parquet
    onStatus?.(`Writing GeoParquet (v${cfg.metadataVersion})...`);
    const tempOpfsPath = `opfs://temp_${this.tabId}_${Date.now()}.parquet`;
    this.trackTempFile(tempOpfsPath.replace('opfs://', ''));
    await this.db.registerOPFSFileName(tempOpfsPath);
    await sleep(5);

    await this.conn.query(`
      COPY (
        SELECT * REPLACE (${cfg.geomExpr})${cfg.bboxSelect}
        FROM ${this.parquetSource}
        WHERE ${this.bboxFilter}
      ) TO '${tempOpfsPath}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'NONE')
    `);

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Step 2: Compute actual bbox and geometry_types from local OPFS file
    onStatus?.('Computing metadata...');
    const statsResult = await this.conn.query(cfg.getBboxQuery(tempOpfsPath));
    const geoBbox = [
      statsResult.getChildAt(0).get(0),
      statsResult.getChildAt(1).get(0),
      statsResult.getChildAt(2).get(0),
      statsResult.getChildAt(3).get(0)
    ];

    const typesResult = await this.conn.query(`
      SELECT DISTINCT ST_GeometryType(${cfg.geomRef}) as geom_type
      FROM '${tempOpfsPath}' WHERE geometry IS NOT NULL
    `);
    const geomTypes = [];
    for (let i = 0; i < typesResult.numRows; i++) {
      const raw = typesResult.getChildAt(0).get(i);
      geomTypes.push(DUCKDB_TO_SPEC[raw] || raw);
    }

    // Step 3: Re-COPY with correct geo metadata + Hilbert sort
    const geoMeta = {
      version: cfg.metadataVersion,
      primary_column: 'geometry',
      columns: { geometry: cfg.getColumnMeta(geomTypes, geoBbox) }
    };
    const geoMetaEscaped = JSON.stringify(geoMeta).replace(/'/g, "''");

    onStatus?.('Sorting by Hilbert curve & finalizing...');
    const [bxmin, bymin, bxmax, bymax] = geoBbox;
    await this.conn.query(`
      COPY (
        SELECT * FROM '${tempOpfsPath}'
        ORDER BY ST_Hilbert(${cfg.geomRef},
          ST_Extent(ST_MakeEnvelope(${bxmin}, ${bymin}, ${bxmax}, ${bymax})))
      ) TO '${this._opfsPath}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'NONE', KV_METADATA {geo: '${geoMetaEscaped}'})
    `);

    // Release DuckDB hold and eagerly clean up temp file
    try {
      await this.db.dropFile(tempOpfsPath);
    } catch (e) { /* ignore */ }
    await this.releaseTempFile(tempOpfsPath.replace('opfs://', ''));
  }
}
