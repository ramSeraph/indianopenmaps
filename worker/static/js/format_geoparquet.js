// GeoParquet format handler for partial downloads
// 3-step pipeline: write temp → compute metadata → re-copy with Hilbert sort
// v2.0 support inspired by https://github.com/geoparquet/geoparquet-io

import { FormatHandler } from './format_base.js';
import { OPFS_PREFIX_OUTPUT, OPFS_PREFIX_TMP, ScopedProgress } from './utils.js';

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
    this.extension = 'parquet';
    this.opfsPath = null;
    if (!this.versionConfig) throw new Error(`Unsupported GeoParquet version: ${version}`);
  }

  // Peak browser storage: intermediate parquet (~1× source) + final output coexist on OPFS
  getExpectedBrowserStorageUsage() {
    const outputFactor = this.versionConfig.metadataVersion.startsWith('2') ? 0.8 : 1;
    return this.estimatedBytes * (1 + outputFactor);
  }

  // Intermediate deleted before save; peak total = max(browser, output + download copy)
  getTotalExpectedDiskUsage() {
    const outputFactor = this.versionConfig.metadataVersion.startsWith('2') ? 0.8 : 1;
    return this.estimatedBytes * Math.max(1 + outputFactor, 2 * outputFactor);
  }

  async _write({ onProgress, onStatus, cancelled }) {
    const cfg = this.versionConfig;

    // Stage 1 (0–50%): Write initial parquet from remote source
    onStatus?.(`Writing GeoParquet (v${cfg.metadataVersion})...`);
    const stage1 = new ScopedProgress(onProgress, 0, 50);
    const tempOpfsPath = await this.createDuckdbOpfsFile(OPFS_PREFIX_TMP, this.extension);

    const stopTracker1 = this.startDiskProgressTracker(
      stage1.callback, onStatus, 'Fetching data:', this.estimatedBytes
    );
    try {
      await this.duckdb.conn.query(`
        COPY (
          SELECT * EXCLUDE (bbox) REPLACE (${cfg.geomExpr})${cfg.bboxSelect}
          FROM ${this.parquetSource}
          WHERE ${this.bboxFilter}
        ) TO '${tempOpfsPath}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'NONE')
      `);
    } finally {
      stopTracker1();
    }

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');
    // Measure actual temp file size for accurate stage 3 progress tracking
    const tempFile = await this.getOpfsFile(tempOpfsPath);
    const tempFileSize = tempFile.size;


    // Stage 2 (50–60%): Compute actual bbox and geometry_types from local OPFS file
    onStatus?.('Computing metadata...');
    onProgress?.(50);
    const statsResult = await this.duckdb.conn.query(cfg.getBboxQuery(tempOpfsPath));

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    const geoBbox = [
      statsResult.getChildAt(0).get(0),
      statsResult.getChildAt(1).get(0),
      statsResult.getChildAt(2).get(0),
      statsResult.getChildAt(3).get(0)
    ];

    onStatus?.('Determining geometry types...');
    onProgress?.(60);
    const typesResult = await this.duckdb.conn.query(`
      SELECT DISTINCT ST_GeometryType(${cfg.geomRef}) as geom_type
      FROM '${tempOpfsPath}' WHERE geometry IS NOT NULL
    `);

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    const geomTypes = [];
    for (let i = 0; i < typesResult.numRows; i++) {
      const raw = typesResult.getChildAt(0).get(i);
      geomTypes.push(DUCKDB_TO_SPEC[raw] || raw);
    }
    onProgress?.(70);

    // Stage 3 (70–100%): Re-COPY with correct geo metadata + Hilbert sort
    const geoMeta = {
      version: cfg.metadataVersion,
      primary_column: 'geometry',
      columns: { geometry: cfg.getColumnMeta(geomTypes, geoBbox) }
    };
    const geoMetaEscaped = JSON.stringify(geoMeta).replace(/'/g, "''");

    onStatus?.('Sorting by Hilbert curve & finalizing...');
    const stage2 = new ScopedProgress(onProgress, 70, 100);
    const [bxmin, bymin, bxmax, bymax] = geoBbox;

    this.opfsPath = await this.createDuckdbOpfsFile(OPFS_PREFIX_OUTPUT, this.extension);
    const stopTracker2 = this.startDiskProgressTracker(
      stage2.callback, onStatus, 'Sorting & finalizing:', tempFileSize
    );
    try {
      await this.duckdb.conn.query(`
        COPY (
          SELECT * FROM '${tempOpfsPath}'
          ORDER BY ST_Hilbert(${cfg.geomRef},
            ST_Extent(ST_MakeEnvelope(${bxmin}, ${bymin}, ${bxmax}, ${bymax})))
        ) TO '${this.opfsPath}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'NONE', KV_METADATA {geo: '${geoMetaEscaped}'})
      `);
    } finally {
      stopTracker2();
    }

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Release DuckDB hold and eagerly clean up temp file
    await this.releaseDuckdbOpfsFile(tempOpfsPath);
    await this.releaseDuckdbOpfsFile(this.opfsPath);
    await this.removeOpfsFile(tempOpfsPath.replace('opfs://', ''));
    onProgress?.(100);
  }

  async getDownloadMap(baseName) {
    const file = await this.getOpfsFile(this.opfsPath);
    return [{ downloadName: `${baseName}.${this.extension}`, blobParts: [file] }];
  }

}
