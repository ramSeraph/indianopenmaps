// Web Worker for writing GeoPackage files to OPFS.
// Reads an intermediate parquet from OPFS via hyparquet,
// writes GPKG via wa-sqlite's OPFSAdaptiveVFS.

import SQLiteESMFactory from 'https://ramseraph.github.io/sqwab/dist/wa-sqlite-async.mjs';
import * as SQLite from 'https://ramseraph.github.io/sqwab/src/sqlite-api.js';
import { OPFSAdaptiveVFS } from 'https://ramseraph.github.io/sqwab/src/examples/OPFSAdaptiveVFS.js';
import { parquetRead, parquetMetadataAsync, parquetSchema } from 'https://esm.sh/hyparquet@1.25.0';
import { compressors } from 'https://esm.sh/hyparquet-compressors@1';
import { fileToAsyncBuffer } from './utils.js';

let sqlite3 = null;
let db = null;

async function init(dbPath) {
  const module = await SQLiteESMFactory();
  sqlite3 = SQLite.Factory(module);

  const vfs = await OPFSAdaptiveVFS.create('opfs-adaptive', module);
  sqlite3.vfs_register(vfs, true);

  db = await sqlite3.open_v2(dbPath);
}

async function exec(sql) {
  if (!db) throw new Error('Database not initialized');
  await sqlite3.exec(db, sql);
}

async function insertBatch(sql, paramSets) {
  if (!db) throw new Error('Database not initialized');
  for await (const stmt of sqlite3.statements(db, sql)) {
    for (const params of paramSets) {
      await sqlite3.reset(stmt);
      sqlite3.bind_collection(stmt, params);
      await sqlite3.step(stmt);
    }
  }
}

async function close() {
  if (db) {
    await sqlite3.close(db);
    db = null;
  }
}

// --- GeoPackage constants and helpers ---

// 8-byte GeoPackageBinary header (no envelope) for WGS84 (SRID 4326)
const GP_HEADER = new Uint8Array([0x47, 0x50, 0x00, 0x01, 0xE6, 0x10, 0x00, 0x00]);

const WGS84_WKT = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]';

function esc(s) {
  return s.replace(/'/g, "''");
}

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2) {
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  }
  return bytes;
}

// WKB type codes for single → multi promotion
const SINGLE_TO_MULTI_WKB = { 1: 4, 2: 5, 3: 6 }; // Point→Multi, Line→Multi, Polygon→Multi

/**
 * Promote a single-geometry WKB to its MULTI variant by wrapping it.
 * MULTI WKB = [byte_order(1)] [multi_type(4)] [count=1(4)] [original_wkb]
 * If already MULTI or unknown type, return as-is.
 */
function promoteToMulti(wkb) {
  if (wkb.length < 5) return wkb;
  const littleEndian = wkb[0] === 1;
  const typeVal = littleEndian
    ? wkb[1] | (wkb[2] << 8) | (wkb[3] << 16) | (wkb[4] << 24)
    : (wkb[1] << 24) | (wkb[2] << 16) | (wkb[3] << 8) | wkb[4];
  // Extract base type (lower 3 bits handle 2D; higher bits hold Z/M/ZM flags)
  const baseType = typeVal % 1000; // handles ISO WKB (1000=Z, 2000=M, 3000=ZM)
  const flags = typeVal - baseType;
  const multiType = SINGLE_TO_MULTI_WKB[baseType];
  if (!multiType) return wkb; // already multi or geometry collection

  const result = new Uint8Array(9 + wkb.length);
  result[0] = wkb[0]; // same byte order
  const newType = multiType + flags;
  if (littleEndian) {
    result[1] = newType & 0xFF; result[2] = (newType >> 8) & 0xFF;
    result[3] = (newType >> 16) & 0xFF; result[4] = (newType >> 24) & 0xFF;
    result[5] = 1; result[6] = 0; result[7] = 0; result[8] = 0; // count = 1
  } else {
    result[1] = (newType >> 24) & 0xFF; result[2] = (newType >> 16) & 0xFF;
    result[3] = (newType >> 8) & 0xFF; result[4] = newType & 0xFF;
    result[5] = 0; result[6] = 0; result[7] = 0; result[8] = 1; // count = 1
  }
  result.set(wkb, 9);
  return result;
}

function buildGpkgGeom(wkbHex, needsPromote) {
  if (!wkbHex) return null;
  let wkb = hexToBytes(wkbHex);
  if (needsPromote) wkb = promoteToMulti(wkb);
  const result = new Uint8Array(GP_HEADER.length + wkb.length);
  result.set(GP_HEADER, 0);
  result.set(wkb, GP_HEADER.length);
  return result;
}

const MULTI_MAP = {
  'POINT': 'MULTIPOINT', 'LINESTRING': 'MULTILINESTRING', 'POLYGON': 'MULTIPOLYGON',
};

function resolveGeomTypeName(geomTypes) {
  if (geomTypes.size === 1) return [...geomTypes][0];
  const bases = new Set([...geomTypes].map(t => t.replace('MULTI', '')));
  if (bases.size === 1) {
    const base = [...bases][0];
    return MULTI_MAP[base] || 'MULTI' + base;
  }
  return 'GEOMETRY';
}

// Map parquet physical/logical types to SQLite types
function parquetTypeToSqlite(element) {
  const logical = element.logicalType;
  if (logical) {
    if (logical.type === 'STRING' || logical.type === 'UTF8' || logical.type === 'ENUM' || logical.type === 'JSON') return 'TEXT';
    if (logical.type === 'DATE' || logical.type === 'TIME' || logical.type === 'TIMESTAMP') return 'TEXT';
    if (logical.type === 'DECIMAL') return 'REAL';
    if (logical.type === 'INTEGER' || logical.type === 'INT') return 'INTEGER';
  }
  const phys = element.type;
  if (phys === 'INT32' || phys === 'INT64' || phys === 'BOOLEAN') return 'INTEGER';
  if (phys === 'FLOAT' || phys === 'DOUBLE') return 'REAL';
  if (phys === 'BYTE_ARRAY' || phys === 'FIXED_LEN_BYTE_ARRAY') return 'TEXT';
  return 'TEXT';
}

// Columns that are internal to the intermediate parquet, not user attributes
const INTERNAL_COLS = new Set(['geom_wkb', '_geom_type', '_bbox_minx', '_bbox_miny', '_bbox_maxx', '_bbox_maxy', 'bbox']);

// --- Main pipeline: parquet → GPKG ---

/**
 * Read intermediate parquet from OPFS, write complete GPKG.
 * @param {object} args
 * @param {string} args.parquetFileName - OPFS filename of intermediate parquet
 * @param {string} args.gpkgFileName - desired OPFS filename for output GPKG
 * @param {number} msgId - message ID for sending progress updates
 */
async function writeFromParquet({ parquetFileName, gpkgFileName }, msgId) {
  const progress = (status) => self.postMessage({ id: msgId, progress: true, status });

  const root = await navigator.storage.getDirectory();
  const fileHandle = await root.getFileHandle(parquetFileName);
  const file = await fileHandle.getFile();
  const asyncBuffer = fileToAsyncBuffer(file);

  // Derive attribute columns from parquet schema
  const metadata = await parquetMetadataAsync(asyncBuffer);
  const schema = parquetSchema(metadata);

  // Attribute columns (non-internal). Struct/list columns are serialized as JSON (like GDAL).
  const columns = [];
  for (const child of schema.children) {
    if (INTERNAL_COLS.has(child.element.name)) continue;
    const isNested = child.children?.length > 0;
    columns.push({
      name: child.element.name,
      sqliteType: isNested ? 'TEXT' : parquetTypeToSqlite(child.element),
      jsonSerialize: isNested,
    });
  }

  // Pass 1: collect metadata (geometry types, bbox), one row-group at a time.
  // onComplete returns row-oriented data: array of rows, each row an array of values.
  // With columns filter, row indices match filter order (0..N-1), not full schema.
  progress('Scanning metadata...');
  const geomTypes = new Set();
  let xmin = Infinity, ymin = Infinity, xmax = -Infinity, ymax = -Infinity;

  let rowOffset = 0;
  for (const rg of metadata.row_groups) {
    const rgEnd = rowOffset + Number(rg.num_rows);
    let rows;
    await parquetRead({
      file: asyncBuffer,
      compressors,
      columns: ['_geom_type', '_bbox_minx', '_bbox_miny', '_bbox_maxx', '_bbox_maxy'],
      rowStart: rowOffset,
      rowEnd: rgEnd,
      onComplete: (data) => { rows = data; },
    });
    for (const row of rows) {
      if (row[0]) geomTypes.add(row[0]);         // _geom_type
      if (row[1] != null && row[1] < xmin) xmin = row[1]; // _bbox_minx
      if (row[2] != null && row[2] < ymin) ymin = row[2]; // _bbox_miny
      if (row[3] != null && row[3] > xmax) xmax = row[3]; // _bbox_maxx
      if (row[4] != null && row[4] > ymax) ymax = row[4]; // _bbox_maxy
    }
    rowOffset = rgEnd;
  }

  const bbox = {
    xmin: xmin === Infinity ? 0 : xmin,
    ymin: ymin === Infinity ? 0 : ymin,
    xmax: xmax === -Infinity ? 0 : xmax,
    ymax: ymax === -Infinity ? 0 : ymax,
  };
  const geomTypeName = resolveGeomTypeName(geomTypes);
  const promoteTypes = new Set(
    [...geomTypes].filter(t => MULTI_MAP[t] && MULTI_MAP[t] === geomTypeName)
  );

  // Initialize wa-sqlite and create GPKG schema
  progress('Initializing GeoPackage writer...');
  await init(gpkgFileName);

  try {
    await exec('PRAGMA application_id = 0x47503130'); // "GP10"
    await exec('PRAGMA user_version = 10400');         // v1.4.0
    await exec('PRAGMA journal_mode = MEMORY');
    await exec('PRAGMA synchronous = OFF');

    progress('Creating GeoPackage schema...');
    await exec(`
      CREATE TABLE gpkg_spatial_ref_sys (
        srs_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL PRIMARY KEY,
        organization TEXT NOT NULL,
        organization_coordsys_id INTEGER NOT NULL,
        definition TEXT NOT NULL,
        description TEXT
      )
    `);

    await exec(`
      INSERT INTO gpkg_spatial_ref_sys VALUES
        ('Undefined Cartesian SRS', -1, 'NONE', -1, 'undefined', 'undefined Cartesian coordinate reference system'),
        ('Undefined Geographic SRS', 0, 'NONE', 0, 'undefined', 'undefined geographic coordinate reference system'),
        ('WGS 84 geodetic', 4326, 'EPSG', 4326, '${esc(WGS84_WKT)}', 'longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid')
    `);

    await exec(`
      CREATE TABLE gpkg_contents (
        table_name TEXT NOT NULL PRIMARY KEY,
        data_type TEXT NOT NULL,
        identifier TEXT UNIQUE,
        description TEXT DEFAULT '',
        last_change TEXT NOT NULL,
        min_x DOUBLE,
        min_y DOUBLE,
        max_x DOUBLE,
        max_y DOUBLE,
        srs_id INTEGER,
        CONSTRAINT fk_gc_r_srs_id FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
      )
    `);

    await exec(`
      CREATE TABLE gpkg_geometry_columns (
        table_name TEXT NOT NULL,
        column_name TEXT NOT NULL,
        geometry_type_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL,
        z TINYINT NOT NULL,
        m TINYINT NOT NULL,
        CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name),
        CONSTRAINT fk_gc_tn FOREIGN KEY (table_name) REFERENCES gpkg_contents(table_name),
        CONSTRAINT fk_gc_srs FOREIGN KEY (srs_id) REFERENCES gpkg_spatial_ref_sys(srs_id)
      )
    `);

    const colDefs = columns.map(c => `"${c.name}" ${c.sqliteType}`).join(', ');
    await exec(`
      CREATE TABLE features (
        fid INTEGER PRIMARY KEY AUTOINCREMENT,
        geom BLOB,
        _bbox_minx REAL, _bbox_miny REAL, _bbox_maxx REAL, _bbox_maxy REAL
        ${columns.length > 0 ? ', ' + colDefs : ''}
      )
    `);

    // Pass 2: read one row-group at a time using rowStart/rowEnd, insert into SQLite.
    // This keeps only one row-group of data in memory at any point.
    progress('Writing features...');
    const bboxCols = '_bbox_minx, _bbox_miny, _bbox_maxx, _bbox_maxy';
    const placeholders = ['?', '?', '?', '?', '?', ...columns.map(() => '?')].join(', ');
    const attrCols = columns.length > 0 ? ', ' + columns.map(c => `"${c.name}"`).join(', ') : '';
    const insertSql = `INSERT INTO features (geom, ${bboxCols}${attrCols}) VALUES (${placeholders})`;

    // Pre-compute full-schema column indices for pass 2 (no columns filter)
    const colIndex = {};
    schema.children.forEach((child, i) => { colIndex[child.element.name] = i; });
    const iWkb = colIndex['geom_wkb'];
    const iGT = colIndex['_geom_type'];
    const iMinX = colIndex['_bbox_minx'], iMinY = colIndex['_bbox_miny'];
    const iMaxX = colIndex['_bbox_maxx'], iMaxY = colIndex['_bbox_maxy'];
    const attrIndices = columns.map(c => colIndex[c.name]);

    let rowCount = 0;
    await exec('BEGIN TRANSACTION');

    rowOffset = 0;
    for (const rg of metadata.row_groups) {
      const rgEnd = rowOffset + Number(rg.num_rows);
      let rows;
      await parquetRead({
        file: asyncBuffer,
        compressors,
        rowStart: rowOffset,
        rowEnd: rgEnd,
        onComplete: (data) => { rows = data; },
      });

      const paramSets = [];
      for (const row of rows) {
        const needsPromote = promoteTypes.has(row[iGT]);
        const params = [
          buildGpkgGeom(row[iWkb], needsPromote),
          row[iMinX], row[iMinY], row[iMaxX], row[iMaxY],
        ];
        for (let ci = 0; ci < attrIndices.length; ci++) {
          const val = row[attrIndices[ci]];
          params.push(columns[ci].jsonSerialize && val != null ? JSON.stringify(val) : val);
        }
        paramSets.push(params);
      }
      await insertBatch(insertSql, paramSets);
      rowCount += paramSets.length;
      progress(`Writing features... (${rowCount} rows)`);

      rowOffset = rgEnd;
    }

    await exec('COMMIT');

    // Build R-tree spatial index
    progress('Building spatial index...');

    await exec(`
      CREATE TABLE gpkg_extensions (
        table_name TEXT,
        column_name TEXT,
        extension_name TEXT NOT NULL,
        definition TEXT NOT NULL,
        scope TEXT NOT NULL,
        CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name)
      )
    `);

    await exec(`
      INSERT INTO gpkg_extensions VALUES (
        'features', 'geom', 'gpkg_rtree_index',
        'http://www.geopackage.org/spec120/#extension_rtree',
        'write-only'
      )
    `);

    await exec('CREATE VIRTUAL TABLE rtree_features_geom USING rtree(id, minx, maxx, miny, maxy)');
    await exec('INSERT INTO rtree_features_geom (id, minx, maxx, miny, maxy) SELECT fid, _bbox_minx, _bbox_maxx, _bbox_miny, _bbox_maxy FROM features');

    for (const col of ['_bbox_minx', '_bbox_miny', '_bbox_maxx', '_bbox_maxy']) {
      await exec(`ALTER TABLE features DROP COLUMN ${col}`);
    }

    // Populate metadata tables
    progress('Finalizing metadata...');
    const now = new Date().toISOString().replace('Z', '').replace(/\.\d{3}$/, '') + 'Z';

    await exec(`
      INSERT INTO gpkg_contents VALUES (
        'features', 'features', 'features', '',
        '${now}',
        ${bbox.xmin}, ${bbox.ymin}, ${bbox.xmax}, ${bbox.ymax},
        4326
      )
    `);

    await exec(`
      INSERT INTO gpkg_geometry_columns VALUES (
        'features', 'geom', '${geomTypeName}', 4326, 0, 0
      )
    `);

    progress(`GeoPackage complete (${rowCount} features)`);
    return { rowCount };

  } finally {
    await close();
  }
}

// --- Message handler ---

self.onmessage = async (e) => {
  const { id, method, args } = e.data;
  try {
    let result;
    switch (method) {
      case 'writeFromParquet':
        result = await writeFromParquet(args, id);
        break;
      default:
        throw new Error(`Unknown method: ${method}`);
    }
    self.postMessage({ id, result });
  } catch (error) {
    self.postMessage({ id, error: error.message || String(error) });
  }
};
