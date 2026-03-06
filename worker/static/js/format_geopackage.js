// GeoPackage format handler for partial downloads
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// gpkg_worker reads parquet + writes GPKG entirely inside the worker

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Write a GeoPackage file to OPFS.
 * 1. DuckDB filters remote parquet → writes intermediate parquet to OPFS
 * 2. gpkg_worker reads the parquet (derives schema, collects metadata), writes complete GPKG
 *
 * Returns the OPFS filename where the file was written.
 */
export async function writeGeoPackage(conn, db, urlList, bboxWkt, opfsPath, tabId, { onStatus, cancelled }) {
  // Step 1: Write intermediate parquet to OPFS (single scan of remote data)
  onStatus?.('Filtering data...');
  const tempParquetPath = `opfs://temp_gpkg_${tabId}_${Date.now()}.parquet`;
  await db.registerOPFSFileName(tempParquetPath);
  await sleep(5);

  await conn.query(`
    COPY (
      SELECT
        hex(ST_AsWKB(geometry)::BLOB) AS geom_wkb,
        ST_GeometryType(geometry) AS _geom_type,
        ST_XMin(geometry) AS _bbox_minx, ST_YMin(geometry) AS _bbox_miny,
        ST_XMax(geometry) AS _bbox_maxx, ST_YMax(geometry) AS _bbox_maxy,
        * EXCLUDE (geometry)
      FROM read_parquet([${urlList}], union_by_name=true)
      WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
    ) TO '${tempParquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
  `);

  // Release DuckDB's hold so the worker can read the file
  await db.dropFile(tempParquetPath);

  if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

  // Step 2: Hand off to gpkg_worker — it reads parquet + writes GPKG entirely in the worker
  const tempParquetFileName = tempParquetPath.replace('opfs://', '');
  const gpkgFileName = `gpkg_${tabId}_${Date.now()}.gpkg`;

  const worker = new Worker(new URL('./gpkg_worker.js', import.meta.url), { type: 'module' });
  try {
    await new Promise((resolve, reject) => {
      const msgId = 1;
      worker.onmessage = (e) => {
        const { id, result, error, progress, status } = e.data;
        if (id !== msgId) return;
        if (progress) {
          onStatus?.(status);
          return;
        }
        if (error) reject(new Error(error));
        else resolve(result);
      };
      worker.onerror = (e) => reject(new Error(e.message));
      worker.postMessage({
        id: msgId,
        method: 'writeFromParquet',
        args: { parquetFileName: tempParquetFileName, gpkgFileName },
      });
    });
  } finally {
    worker.terminate();

    // Clean up intermediate parquet from OPFS
    try {
      const root = await navigator.storage.getDirectory();
      await root.removeEntry(tempParquetFileName);
    } catch (e) { /* ignore */ }
  }

  return gpkgFileName;
}
