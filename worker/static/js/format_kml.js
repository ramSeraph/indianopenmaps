// KML format handler for partial downloads.
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// DuckDB queries schema → hyparquet reads row-group-by-row-group →
// Placemarks streamed as XML text to OPFS → KML header/footer wrapped at download.

import { FormatHandler } from './format_base.js';
import { OPFS_PREFIX_KML_TMP, ScopedProgress, fileToAsyncBuffer, parseWkbHex } from './utils.js';
import { parquetRead, parquetMetadataAsync, parquetSchema } from 'https://esm.sh/hyparquet@1.25.0';
import { compressors } from 'https://esm.sh/hyparquet-compressors@1';
import { featureToPlacemark, KML_HEADER, KML_FOOTER } from './kml_writer.js';

const INTERNAL_COLS = new Set(['geom_wkb']);

export class KmlFormatHandler extends FormatHandler {
  constructor(opts = {}) {
    super(opts);
    this.extension = 'kml';
  }

  getExpectedBrowserStorageUsage() { return this.estimatedBytes * 8; }
  getTotalExpectedDiskUsage() { return this.estimatedBytes * 16; }

  async _write({ onProgress, onStatus }) {
    // Stage 1 (0–50%): DuckDB → intermediate parquet on OPFS
    const stage1 = new ScopedProgress(onProgress, 0, 50);
    const tempParquetPath = await this.createIntermediateParquet({
      prefix: OPFS_PREFIX_KML_TMP,
      onProgress: stage1.callback, onStatus,
    });

    // Discover columns from local intermediate parquet via DuckDB
    onStatus?.('Reading schema...');
    const columns = await this.describeColumns(tempParquetPath, INTERNAL_COLS);
    const attrColumns = columns.map(c => ({ originalName: c.name }));

    this.throwIfCancelled();

    // Release DuckDB hold on intermediate file
    await this.releaseDuckdbOpfsFile(tempParquetPath);

    // Stage 2 (50–100%): Read parquet with hyparquet, stream Placemarks to OPFS
    onStatus?.('Writing KML...');
    const stage2 = new ScopedProgress(onProgress, 50, 100);

    const file = await this.getOpfsFile(tempParquetPath);
    const asyncBuffer = fileToAsyncBuffer(file);
    const metadata = await parquetMetadataAsync(asyncBuffer);

    const hSchema = parquetSchema(metadata);
    const colIndex = {};
    hSchema.children.forEach((child, i) => { colIndex[child.element.name] = i; });
    const iWkb = colIndex['geom_wkb'];
    const attrIndices = attrColumns.map(c => colIndex[c.originalName]);

    // Open OPFS writable stream for Placemark body
    const bodyName = `${OPFS_PREFIX_KML_TMP}${this.tabId}_${Date.now()}.kml.body`;
    const bodyHandle = await this.getOpfsHandle(bodyName, { create: true });
    const bodyStream = await bodyHandle.createWritable();
    const encoder = new TextEncoder();

    const totalRows = metadata.row_groups.reduce((sum, rg) => sum + Number(rg.num_rows), 0);
    let processedRows = 0;
    let rowOffset = 0;

    try {
      for (const rg of metadata.row_groups) {
        this.throwIfCancelled();

        const rgEnd = rowOffset + Number(rg.num_rows);
        let rows;
        await parquetRead({
          file: asyncBuffer, compressors,
          rowStart: rowOffset, rowEnd: rgEnd,
          onComplete: (data) => { rows = data; },
        });

        let chunk = '';
        for (const row of rows) {
          const wkbHex = row[iWkb];
          if (!wkbHex) continue;

          const geom = parseWkbHex(wkbHex);
          const props = {};
          for (let ci = 0; ci < attrIndices.length; ci++) {
            const val = row[attrIndices[ci]];
            props[attrColumns[ci].originalName] = val != null && typeof val === 'object'
              ? JSON.stringify(val) : val;
          }

          chunk += featureToPlacemark(geom, props, attrColumns);
        }

        if (chunk) await bodyStream.write(encoder.encode(chunk));

        processedRows += rows.length;
        stage2.report((processedRows / totalRows) * 100);
        onStatus?.(`Writing KML... (${processedRows}/${totalRows})`);
        rowOffset = rgEnd;
      }
    } finally {
      try { await bodyStream.close(); } catch (e) { /* may already be closed */ }
    }

    this._bodyName = bodyName;

    // Clean up intermediate parquet
    await this.removeOpfsFile(tempParquetPath.replace('opfs://', ''));

    onProgress?.(100);
    onStatus?.('KML complete');
  }

  async getDownloadMap(baseName) {
    if (!this._bodyName) return [];
    const bodyFile = await this.getOpfsFile(this._bodyName);
    const encoder = new TextEncoder();
    return [{
      downloadName: `${baseName}.${this.extension}`,
      blobParts: [encoder.encode(KML_HEADER), bodyFile, encoder.encode(KML_FOOTER)],
    }];
  }
}
