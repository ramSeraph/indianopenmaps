// GeoPackage format handler for partial downloads
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// gpkg_worker reads parquet + writes GPKG entirely inside the worker

import { FormatHandler, sleep } from './format_base.js';

export class GeoPackageFormatHandler extends FormatHandler {
  get extension() { return '.gpkg'; }
  get needsDuckDBRegistration() { return false; }

  async write({ onStatus, cancelled }) {
    // Step 1: Write intermediate parquet to OPFS (single scan of remote data)
    onStatus?.('Filtering data...');
    const tempParquetPath = `opfs://temp_gpkg_${this.tabId}_${Date.now()}.parquet`;
    const tempParquetFileName = tempParquetPath.replace('opfs://', '');
    this.trackTempFile(tempParquetFileName);
    await this.db.registerOPFSFileName(tempParquetPath);
    await sleep(5);

    await this.conn.query(`
      COPY (
        SELECT
          hex(ST_AsWKB(geometry)::BLOB) AS geom_wkb,
          ST_GeometryType(geometry) AS _geom_type,
          ST_XMin(geometry) AS _bbox_minx, ST_YMin(geometry) AS _bbox_miny,
          ST_XMax(geometry) AS _bbox_maxx, ST_YMax(geometry) AS _bbox_maxy,
          * EXCLUDE (geometry)
        FROM ${this.parquetSource}
        WHERE ${this.bboxFilter}
      ) TO '${tempParquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
    `);

    // Release DuckDB's hold so the worker can read the file
    await this.db.dropFile(tempParquetPath);

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Step 2: Hand off to gpkg_worker — it reads parquet + writes GPKG entirely in the worker
    const gpkgFileName = `gpkg_${this.tabId}_${Date.now()}.gpkg`;

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
      await this.releaseTempFile(tempParquetFileName);
    }

    this._outputFileName = gpkgFileName;
  }
}