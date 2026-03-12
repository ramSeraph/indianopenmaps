// GeoPackage format handler for partial downloads
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// gpkg_worker reads parquet + writes GPKG entirely inside the worker

import { FormatHandler } from './format_base.js';
import { OPFS_PREFIX_GPKG_TMP, OPFS_PREFIX_GPKG, ScopedProgress } from './utils.js';

export class GeoPackageFormatHandler extends FormatHandler {
  get extension() { return '.gpkg'; }
  get needsDuckDBRegistration() { return false; }

  // Peak browser storage: intermediate parquet (~1× source) + gpkg output coexist on OPFS
  getExpectedBrowserStorageUsage() { return this.estimatedBytes * (1 + 1.5); }
  // Intermediate deleted before save; peak total = max(browser, output + download copy)
  getTotalExpectedDiskUsage() { return this.estimatedBytes * (1.5 + 1.5); }

  async _write({ onProgress, onStatus, cancelled }) {
    // Stage 1 (0–70%): Write intermediate parquet to OPFS (remote data fetch)
    onStatus?.('Filtering data...');
    const stage1 = new ScopedProgress(onProgress, 0, 70);
    const tempParquetPath = await this.createTempOpfsFile(OPFS_PREFIX_GPKG_TMP);
    const tempParquetFileName = tempParquetPath.replace('opfs://', '');

    const stopTracker1 = this.startDiskProgressTracker(
      stage1.callback, onStatus, 'Filtering data:', this.estimatedBytes
    );
    try {
      await this.conn.query(`
        COPY (
          SELECT
            hex(ST_AsWKB(geometry)::BLOB) AS geom_wkb,
            ST_GeometryType(geometry) AS _geom_type,
            ST_XMin(geometry) AS _bbox_minx, ST_YMin(geometry) AS _bbox_miny,
            ST_XMax(geometry) AS _bbox_maxx, ST_YMax(geometry) AS _bbox_maxy,
            * EXCLUDE (geometry, bbox)
          FROM ${this.parquetSource}
          WHERE ${this.bboxFilter}
        ) TO '${tempParquetPath}' (FORMAT PARQUET, COMPRESSION ZSTD)
      `);
    } finally {
      stopTracker1();
    }

    // Release DuckDB's hold so the worker can read the file
    await this.db.dropFile(tempParquetPath);

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    // Stage 2 (70–100%): Worker reads parquet + writes GPKG
    onStatus?.('Writing GeoPackage...');
    const stage2 = new ScopedProgress(onProgress, 70, 100);
    const gpkgFileName = `${OPFS_PREFIX_GPKG}${this.tabId}_${Date.now()}.gpkg`;

    const stopTracker2 = this.startDiskProgressTracker(
      stage2.callback, onStatus, 'Writing GeoPackage:', this.estimatedBytes * 1.5
    );
    const worker = new Worker(new URL('./gpkg_worker.js', import.meta.url), { type: 'module' });
    try {
      await new Promise((resolve, reject) => {
        const msgId = 1;
        const checkCancel = setInterval(() => {
          if (cancelled()) {
            clearInterval(checkCancel);
            worker.terminate();
            reject(new DOMException('Download cancelled', 'AbortError'));
          }
        }, 1000);
        worker.onmessage = (e) => {
          const { id, result, error, progress, status } = e.data;
          if (id !== msgId) return;
          if (progress) {
            onStatus?.(status);
            return;
          }
          clearInterval(checkCancel);
          if (error) reject(new Error(error));
          else resolve(result);
        };
        worker.onerror = (e) => {
          clearInterval(checkCancel);
          reject(new Error(e.message));
        };
        worker.postMessage({
          id: msgId,
          method: 'writeFromParquet',
          args: { parquetFileName: tempParquetFileName, gpkgFileName },
        });
      });
    } finally {
      stopTracker2();
      worker.terminate();
      await this.releaseTempFile(tempParquetFileName);
    }

    this._outputFileName = gpkgFileName;
    onProgress?.(100);
  }
}
