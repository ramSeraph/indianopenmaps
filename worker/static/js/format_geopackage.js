// GeoPackage format handler for partial downloads
// Uses wa-sqlite with OPFSAdaptiveVFS to write GPKG directly to OPFS,
// streaming results from DuckDB via conn.send() to avoid memory bottleneck.

// 8-byte GeoPackageBinary header (no envelope) for WGS84 (SRID 4326)
// 0x4750 = "GP" magic, 0x00 = version, 0x01 = flags (LE, no envelope, not empty)
// 0xE6100000 = 4326 as int32 LE
const GP_HEADER = new Uint8Array([0x47, 0x50, 0x00, 0x01, 0xE6, 0x10, 0x00, 0x00]);

const WGS84_WKT = 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]';

function esc(s) {
  return s.replace(/'/g, "''");
}

// Build GeoPackageBinary blob: 8-byte GP header + WKB bytes
function buildGpkgGeom(wkbBytes) {
  if (!wkbBytes) return null;
  const wkb = wkbBytes instanceof Uint8Array ? wkbBytes : new Uint8Array(wkbBytes);
  const result = new Uint8Array(GP_HEADER.length + wkb.length);
  result.set(GP_HEADER, 0);
  result.set(wkb, GP_HEADER.length);
  return result;
}

// Wrapper to call the wa-sqlite worker and get a response
let gpkgWorker = null;
let msgId = 0;
const pendingMessages = new Map();

function postWorkerMessage(method, args, transfer) {
  return new Promise((resolve, reject) => {
    const id = ++msgId;
    pendingMessages.set(id, { resolve, reject });
    gpkgWorker.postMessage({ id, method, args }, transfer || []);
  });
}

function initWorker() {
  if (gpkgWorker) return;
  gpkgWorker = new Worker(new URL('./gpkg_worker.js', import.meta.url), { type: 'module' });
  gpkgWorker.onmessage = (e) => {
    const { id, result, error } = e.data;
    const pending = pendingMessages.get(id);
    if (pending) {
      pendingMessages.delete(id);
      if (error) {
        pending.reject(new Error(error));
      } else {
        pending.resolve(result);
      }
    }
  };
  gpkgWorker.onerror = (e) => {
    console.error('[GeoPackage Worker] Error:', e);
  };
}

function terminateWorker() {
  if (gpkgWorker) {
    gpkgWorker.terminate();
    gpkgWorker = null;
    pendingMessages.clear();
  }
}

// Map DuckDB types to SQLite types for CREATE TABLE
function duckdbTypeToSqlite(duckType) {
  const t = duckType.toUpperCase();
  if (t.includes('INT')) return 'INTEGER';
  if (t.includes('FLOAT') || t.includes('DOUBLE') || t.includes('DECIMAL') || t.includes('NUMERIC')) return 'REAL';
  if (t.includes('BOOL')) return 'INTEGER';
  if (t.includes('BLOB') || t.includes('BYTEA')) return 'BLOB';
  return 'TEXT';
}

/**
 * Write a GeoPackage file to OPFS using wa-sqlite (OPFSAdaptiveVFS).
 * DuckDB streams Arrow batches via conn.send(); rows are inserted into
 * wa-sqlite batch by batch.
 *
 * Returns the OPFS filename where the file was written.
 */
export async function writeGeoPackage(conn, db, urlList, bboxWkt, opfsPath, tabId, { onStatus, cancelled }) {
  // Step 1: Discover schema from source parquet
  onStatus?.('Discovering schema...');
  const schemaResult = await conn.query(
    `SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet([${urlList}], union_by_name=true))`
  );
  const columns = [];
  for (let i = 0; i < schemaResult.numRows; i++) {
    const name = schemaResult.getChildAt(0).get(i);
    const type = schemaResult.getChildAt(1).get(i);
    if (name !== 'geometry') {
      columns.push({ name, type });
    }
  }

  if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

  // Step 2: Compute geometry types and bounding box
  onStatus?.('Computing metadata...');
  const metaResult = await conn.query(`
    SELECT
      MIN(ST_XMin(geometry)) as xmin,
      MIN(ST_YMin(geometry)) as ymin,
      MAX(ST_XMax(geometry)) as xmax,
      MAX(ST_YMax(geometry)) as ymax
    FROM read_parquet([${urlList}], union_by_name=true)
    WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
      AND geometry IS NOT NULL
  `);
  const bbox = {
    xmin: metaResult.getChildAt(0).get(0),
    ymin: metaResult.getChildAt(1).get(0),
    xmax: metaResult.getChildAt(2).get(0),
    ymax: metaResult.getChildAt(3).get(0),
  };

  const typesResult = await conn.query(`
    SELECT DISTINCT ST_GeometryType(geometry) as geom_type
    FROM read_parquet([${urlList}], union_by_name=true)
    WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
      AND geometry IS NOT NULL
  `);
  const geomTypes = new Set();
  for (let i = 0; i < typesResult.numRows; i++) {
    geomTypes.add(typesResult.getChildAt(0).get(i));
  }
  // Resolve to a single GeoPackage-valid type.
  // If mixed single+multi of same base, promote to MULTI.
  const MULTI_MAP = {
    'POINT': 'MULTIPOINT', 'LINESTRING': 'MULTILINESTRING', 'POLYGON': 'MULTIPOLYGON',
  };
  let geomTypeName;
  if (geomTypes.size === 1) {
    geomTypeName = [...geomTypes][0];
  } else {
    // Check if all types share the same base (e.g. LINESTRING + MULTILINESTRING)
    const bases = new Set([...geomTypes].map(t => t.replace('MULTI', '')));
    if (bases.size === 1) {
      const base = [...bases][0];
      geomTypeName = MULTI_MAP[base] || 'MULTI' + base;
    } else {
      geomTypeName = 'GEOMETRY';
    }
  }

  if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

  // Step 3: Initialize wa-sqlite worker with OPFS
  onStatus?.('Initializing GeoPackage writer...');
  initWorker();
  const dbFileName = `gpkg_${tabId}_${Date.now()}.gpkg`;
  try {
    await postWorkerMessage('init', { dbPath: dbFileName });
  } catch (e) {
    terminateWorker();
    throw new Error(`Failed to initialize wa-sqlite: ${e.message}`);
  }

  try {
    // Step 4: Set GeoPackage PRAGMAs (wa-sqlite supports this natively)
    await postWorkerMessage('exec', { sql: 'PRAGMA application_id = 0x47503130' }); // "GP10"
    await postWorkerMessage('exec', { sql: 'PRAGMA user_version = 10400' });         // v1.4.0
    // Performance: no journal (write-once file), no fsync
    await postWorkerMessage('exec', { sql: 'PRAGMA journal_mode = MEMORY' });
    await postWorkerMessage('exec', { sql: 'PRAGMA synchronous = OFF' });

    // Step 5: Create GeoPackage metadata tables
    onStatus?.('Creating GeoPackage schema...');
    await postWorkerMessage('exec', { sql: `
      CREATE TABLE gpkg_spatial_ref_sys (
        srs_name TEXT NOT NULL,
        srs_id INTEGER NOT NULL PRIMARY KEY,
        organization TEXT NOT NULL,
        organization_coordsys_id INTEGER NOT NULL,
        definition TEXT NOT NULL,
        description TEXT
      )
    `});

    await postWorkerMessage('exec', { sql: `
      INSERT INTO gpkg_spatial_ref_sys VALUES
        ('Undefined Cartesian SRS', -1, 'NONE', -1, 'undefined', 'undefined Cartesian coordinate reference system'),
        ('Undefined Geographic SRS', 0, 'NONE', 0, 'undefined', 'undefined geographic coordinate reference system'),
        ('WGS 84 geodetic', 4326, 'EPSG', 4326, '${esc(WGS84_WKT)}', 'longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid')
    `});

    await postWorkerMessage('exec', { sql: `
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
    `});

    await postWorkerMessage('exec', { sql: `
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
    `});

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Step 6: Create features table (with temp bbox columns for rtree population)
    const colDefs = columns.map(c => `"${c.name}" ${duckdbTypeToSqlite(c.type)}`).join(', ');
    await postWorkerMessage('exec', { sql: `
      CREATE TABLE features (
        fid INTEGER PRIMARY KEY AUTOINCREMENT,
        geom BLOB,
        _bbox_minx REAL, _bbox_miny REAL, _bbox_maxx REAL, _bbox_maxy REAL
        ${columns.length > 0 ? ', ' + colDefs : ''}
      )
    `});

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Step 7: Stream data from DuckDB and insert into wa-sqlite
    onStatus?.('Writing features...');

    // Build DuckDB query that returns WKB geometry + bbox + all columns
    const selectCols = columns.map(c => `"${c.name}"`).join(', ');
    const duckQuery = `
      SELECT
        ST_AsWKB(geometry) AS geom_wkb,
        ST_XMin(geometry) AS _bbox_minx, ST_YMin(geometry) AS _bbox_miny,
        ST_XMax(geometry) AS _bbox_maxx, ST_YMax(geometry) AS _bbox_maxy
        ${columns.length > 0 ? ', ' + selectCols : ''}
      FROM read_parquet([${urlList}], union_by_name=true)
      WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
    `;

    // Build INSERT statement placeholder
    const bboxCols = '_bbox_minx, _bbox_miny, _bbox_maxx, _bbox_maxy';
    const placeholders = ['?', '?', '?', '?', '?', ...columns.map(() => '?')].join(', ');
    const attrCols = columns.length > 0 ? ', ' + columns.map(c => `"${c.name}"`).join(', ') : '';
    const insertSql = `INSERT INTO features (geom, ${bboxCols}${attrCols}) VALUES (${placeholders})`;

    // Stream Arrow batches from DuckDB
    const stream = await conn.send(duckQuery, true);
    let rowCount = 0;
    const BATCH_SIZE = 500;

    await postWorkerMessage('exec', { sql: 'BEGIN TRANSACTION' });

    for await (const batch of stream) {
      if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

      const numRows = batch.numRows;
      const paramSets = [];

      for (let r = 0; r < numRows; r++) {
        const row = [];

        // First column is geom_wkb (BLOB)
        const wkbVal = batch.getChildAt(0).get(r);
        row.push(buildGpkgGeom(wkbVal));

        // Columns 1-4 are bbox values (_bbox_minx, _bbox_miny, _bbox_maxx, _bbox_maxy)
        for (let b = 1; b <= 4; b++) {
          row.push(batch.getChildAt(b).get(r));
        }

        // Remaining columns (index 5+) are attribute data
        for (let c = 0; c < columns.length; c++) {
          const val = batch.getChildAt(c + 5).get(r);
          row.push(val);
        }

        paramSets.push(row);

        if (paramSets.length >= BATCH_SIZE) {
          await postWorkerMessage('insertBatch', { sql: insertSql, paramSets });
          rowCount += paramSets.length;
          paramSets.length = 0;
          onStatus?.(`Writing features... (${rowCount} rows)`);
        }
      }

      // Flush remaining rows in this batch
      if (paramSets.length > 0) {
        await postWorkerMessage('insertBatch', { sql: insertSql, paramSets });
        rowCount += paramSets.length;
      }
    }

    await postWorkerMessage('exec', { sql: 'COMMIT' });

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Step 8: Build R-tree spatial index
    onStatus?.('Building spatial index...');

    await postWorkerMessage('exec', { sql: `
      CREATE TABLE gpkg_extensions (
        table_name TEXT,
        column_name TEXT,
        extension_name TEXT NOT NULL,
        definition TEXT NOT NULL,
        scope TEXT NOT NULL,
        CONSTRAINT ge_tce UNIQUE (table_name, column_name, extension_name)
      )
    `});

    await postWorkerMessage('exec', { sql: `
      INSERT INTO gpkg_extensions VALUES (
        'features', 'geom', 'gpkg_rtree_index',
        'http://www.geopackage.org/spec120/#extension_rtree',
        'write-only'
      )
    `});

    await postWorkerMessage('exec', { sql:
      'CREATE VIRTUAL TABLE rtree_features_geom USING rtree(id, minx, maxx, miny, maxy)'
    });

    await postWorkerMessage('exec', { sql:
      'INSERT INTO rtree_features_geom (id, minx, maxx, miny, maxy) SELECT fid, _bbox_minx, _bbox_maxx, _bbox_miny, _bbox_maxy FROM features'
    });

    // Drop temporary bbox columns (SQLite 3.35+ supports DROP COLUMN)
    for (const col of ['_bbox_minx', '_bbox_miny', '_bbox_maxx', '_bbox_maxy']) {
      await postWorkerMessage('exec', { sql: `ALTER TABLE features DROP COLUMN ${col}` });
    }

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Step 9: Populate metadata tables
    onStatus?.('Finalizing metadata...');
    const now = new Date().toISOString().replace('Z', '').replace(/\.\d{3}$/, '') + 'Z';

    await postWorkerMessage('exec', { sql: `
      INSERT INTO gpkg_contents VALUES (
        'features', 'features', 'features', '',
        '${now}',
        ${bbox.xmin}, ${bbox.ymin}, ${bbox.xmax}, ${bbox.ymax},
        4326
      )
    `});

    await postWorkerMessage('exec', { sql: `
      INSERT INTO gpkg_geometry_columns VALUES (
        'features', 'geom', '${geomTypeName}', 4326, 0, 0
      )
    `});

    onStatus?.(`GeoPackage complete (${rowCount} features)`);

  } finally {
    // Close the wa-sqlite database and terminate the worker
    try {
      await postWorkerMessage('close', {});
    } catch (e) { /* ignore */ }
    terminateWorker();
  }

  // Return the OPFS filename so the download handler can find the file
  return dbFileName;
}
