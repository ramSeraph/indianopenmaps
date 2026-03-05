// GeoParquet format handler for partial downloads
// 3-step pipeline: write temp → compute metadata → re-copy with Hilbert sort

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// DuckDB returns uppercase; GeoParquet spec requires title case
const DUCKDB_TO_SPEC = {
  'POINT': 'Point', 'LINESTRING': 'LineString', 'POLYGON': 'Polygon',
  'MULTIPOINT': 'MultiPoint', 'MULTILINESTRING': 'MultiLineString', 'MULTIPOLYGON': 'MultiPolygon',
  'GEOMETRYCOLLECTION': 'GeometryCollection',
};

export async function writeGeoParquet(conn, db, urlList, bboxWkt, opfsPath, tabId, { onStatus, cancelled }) {
  // Step 1: Write initial parquet without geo metadata (bbox struct included for covering)
  onStatus?.('Writing GeoParquet...');
  const tempOpfsPath = `opfs://temp_${tabId}_${Date.now()}.parquet`;
  await db.registerOPFSFileName(tempOpfsPath);
  await sleep(5);

  await conn.query(`
    COPY (
      SELECT * REPLACE (ST_AsWKB(geometry)::BLOB AS geometry),
        struct_pack(
          xmin := ST_XMin(geometry),
          ymin := ST_YMin(geometry),
          xmax := ST_XMax(geometry),
          ymax := ST_YMax(geometry)
        ) AS bbox
      FROM read_parquet([${urlList}], union_by_name=true)
      WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
    ) TO '${tempOpfsPath}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'NONE')
  `);

  if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

  // Step 2: Compute actual bbox and geometry_types from local OPFS file
  onStatus?.('Computing metadata...');
  const statsResult = await conn.query(`
    SELECT MIN(bbox.xmin) as xmin, MIN(bbox.ymin) as ymin,
           MAX(bbox.xmax) as xmax, MAX(bbox.ymax) as ymax
    FROM '${tempOpfsPath}'
  `);
  const geoBbox = [
    statsResult.getChildAt(0).get(0),
    statsResult.getChildAt(1).get(0),
    statsResult.getChildAt(2).get(0),
    statsResult.getChildAt(3).get(0)
  ];

  const typesResult = await conn.query(`
    SELECT DISTINCT ST_GeometryType(ST_GeomFromWKB(geometry)) as geom_type
    FROM '${tempOpfsPath}' WHERE geometry IS NOT NULL
  `);
  const geomTypes = [];
  for (let i = 0; i < typesResult.numRows; i++) {
    const raw = typesResult.getChildAt(0).get(i);
    geomTypes.push(DUCKDB_TO_SPEC[raw] || raw);
  }

  // Step 3: Re-COPY from local OPFS file with correct geo metadata + Hilbert sort
  const geoMeta = {
    version: '1.1.0',
    primary_column: 'geometry',
    columns: {
      geometry: {
        encoding: 'WKB',
        geometry_types: geomTypes,
        bbox: geoBbox,
        covering: {
          bbox: {
            xmin: ['bbox', 'xmin'],
            ymin: ['bbox', 'ymin'],
            xmax: ['bbox', 'xmax'],
            ymax: ['bbox', 'ymax']
          }
        }
      }
    }
  };
  const geoMetaEscaped = JSON.stringify(geoMeta).replace(/'/g, "''");

  onStatus?.('Sorting by Hilbert curve & finalizing...');
  const [bxmin, bymin, bxmax, bymax] = geoBbox;
  await conn.query(`
    COPY (
      SELECT * FROM '${tempOpfsPath}'
      ORDER BY ST_Hilbert(ST_GeomFromWKB(geometry),
        ST_Extent(ST_MakeEnvelope(${bxmin}, ${bymin}, ${bxmax}, ${bymax})))
    ) TO '${opfsPath}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 15, ROW_GROUP_SIZE 100000, GEOPARQUET_VERSION 'NONE', KV_METADATA {geo: '${geoMetaEscaped}'})
  `);

  // Cleanup temp file
  try {
    await db.dropFile(tempOpfsPath);
    const tempFileName = tempOpfsPath.replace('opfs://', '');
    const root = await navigator.storage.getDirectory();
    await root.removeEntry(tempFileName);
  } catch (e) { /* ignore */ }
}
