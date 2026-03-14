// GeoPackage format handler for partial downloads
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// gpkg_worker reads parquet + writes GPKG entirely inside the worker

import { FormatHandler } from './format_base.js';
import { OPFS_PREFIX_GPKG_TMP, OPFS_PREFIX_GPKG, ScopedProgress } from './utils.js';

export class GeoPackageFormatHandler extends FormatHandler {
  constructor({ ...opts } = {}) {
    super(opts);
    this.gpkgFileName = null;
    this.extension = 'gpkg';
    this._worker = null;
  }

  cancel() {
    super.cancel();
    this._worker?.terminate();
    this._worker = null;
  }

  // Peak browser storage: intermediate parquet (~1× source) + gpkg output coexist on OPFS
  getExpectedBrowserStorageUsage() { return this.estimatedBytes * (1 + 1.5); }
  // Intermediate deleted before save; peak total = max(browser, output + download copy)
  getTotalExpectedDiskUsage() { return this.estimatedBytes * (1.5 + 1.5); }

  async _write({ onProgress, onStatus }) {
    // Stage 1 (0–70%): Write intermediate parquet to OPFS (remote data fetch)
    const stage1 = new ScopedProgress(onProgress, 0, 70);
    const tempParquetPath = await this.createIntermediateParquet({
      prefix: OPFS_PREFIX_GPKG_TMP,
      extraColumns: [
        "ST_GeometryType(geometry) AS _geom_type",
        "ST_XMin(geometry) AS _bbox_minx", "ST_YMin(geometry) AS _bbox_miny",
        "ST_XMax(geometry) AS _bbox_maxx", "ST_YMax(geometry) AS _bbox_maxy",
      ],
      onProgress: stage1.callback, onStatus,
    });

    await this.releaseDuckdbOpfsFile(tempParquetPath);

    this.throwIfCancelled();

    // Stage 2 (70–100%): Worker reads parquet + writes GPKG
    onStatus?.('Writing GeoPackage...');
    const stage2 = new ScopedProgress(onProgress, 70, 100);
    this.gpkgFileName = `${OPFS_PREFIX_GPKG}${this.tabId}_${Date.now()}.gpkg`;

    const stopTracker2 = this.startDiskProgressTracker(
      stage2.callback, onStatus, 'Writing GeoPackage:', this.estimatedBytes * 1.5
    );
    this._worker = new Worker(new URL('./gpkg_worker.js', import.meta.url), { type: 'module' });
    try {
      await new Promise((resolve, reject) => {
        const msgId = 1;
        this._worker.onmessage = (e) => {
          const { id, result, error, progress, status } = e.data;
          if (id !== msgId) return;
          if (progress) {
            onStatus?.(status);
            return;
          }
          if (error) reject(new Error(error));
          else resolve(result);
        };
        this._worker.onerror = (e) => {
          reject(new Error(e.message));
        };
        this._worker.postMessage({
          id: msgId,
          method: 'writeFromParquet',
          args: { parquetFileName: tempParquetPath.replace('opfs://', ''), gpkgFileName: this.gpkgFileName },
        });
      });
    } finally {
      stopTracker2();
      this._worker?.terminate();
      this._worker = null;
      await this.removeOpfsFile(tempParquetPath.replace('opfs://', ''));
    }

    onProgress?.(100);
  }

  async getDownloadMap(baseName) {
    const file = await this.getOpfsFile(this.gpkgFileName);
    return [{ downloadName: `${baseName}.${this.extension}`, blobParts: [file] }];
  }
}
