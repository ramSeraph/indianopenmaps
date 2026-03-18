// DXF format handler for partial downloads.
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// DuckDB queries schema → hyparquet reads row-group-by-row-group →
// DXF entities streamed to OPFS → header/tables/footer wrapped at download.

import { FormatHandler } from './format_base.js';
import { OPFS_PREFIX_DXF_TMP, ScopedProgress, bboxUtmZone, fileToAsyncBuffer, parseWkbHex } from './utils.js';
import { parquetRead, parquetMetadataAsync, parquetSchema } from 'https://esm.sh/hyparquet@1.25.0';
import { compressors } from 'https://esm.sh/hyparquet-compressors@1';
import { featureToDxfEntities, buildDxfEnvelope, createUtmTransform } from './dxf_writer.js';

const INTERNAL_COLS = new Set(['geom_wkb']);

export class DxfFormatHandler extends FormatHandler {
  constructor(opts = {}) {
    super(opts);
    this.extension = 'dxf';
  }

  getExpectedBrowserStorageUsage() { return this.estimatedBytes * 8; }
  getTotalExpectedDiskUsage() { return this.estimatedBytes * 16; }

  getFormatWarning() {
    const utmInfo = bboxUtmZone(this.bbox);
    if (!utmInfo) {
      return {
        message: 'DXF export requires the current view to fit within a single UTM zone (6° of longitude).\n\n'
          + 'Please zoom in or pan so the map view does not cross a UTM zone boundary.',
        isBlocking: true,
      };
    }
    return null;
  }

  async _write({ onProgress, onStatus }) {
    // Stage 1 (0–50%): DuckDB → intermediate parquet on OPFS
    const stage1 = new ScopedProgress(onProgress, 0, 50);
    const tempParquetPath = await this.createIntermediateParquet({
      prefix: OPFS_PREFIX_DXF_TMP,
      onProgress: stage1.callback, onStatus,
    });

    // Discover columns from local intermediate parquet via DuckDB
    onStatus?.('Reading schema...');
    const columns = await this.describeColumns(tempParquetPath, INTERNAL_COLS);
    const attrColumns = columns.map(c => ({ originalName: c.name }));

    this.throwIfCancelled();

    // Release DuckDB hold on intermediate file
    await this.releaseDuckdbOpfsFile(tempParquetPath);

    // Stage 2 (50–100%): Read parquet with hyparquet, stream DXF entities to OPFS
    onStatus?.('Writing DXF...');
    const stage2 = new ScopedProgress(onProgress, 50, 100);

    // Compute UTM transform from bbox (validated by getFormatWarning)
    const utmInfo = bboxUtmZone(this.bbox);
    if (!utmInfo) throw new Error('DXF export requires bbox within a single UTM zone');
    const transform = createUtmTransform(utmInfo.zone, utmInfo.hemisphere);
    this._utmInfo = utmInfo;

    const file = await this.getOpfsFile(tempParquetPath);
    const asyncBuffer = fileToAsyncBuffer(file);
    const metadata = await parquetMetadataAsync(asyncBuffer);

    const hSchema = parquetSchema(metadata);
    const colIndex = {};
    hSchema.children.forEach((child, i) => { colIndex[child.element.name] = i; });
    const iWkb = colIndex['geom_wkb'];
    const attrIndices = attrColumns.map(c => colIndex[c.originalName]);

    // Open OPFS writable stream for entity body
    const bodyName = `${OPFS_PREFIX_DXF_TMP}${this.tabId}_${Date.now()}.dxf.body`;
    const bodyHandle = await this.getOpfsHandle(bodyName, { create: true });
    const bodyStream = await bodyHandle.createWritable();
    const encoder = new TextEncoder();

    const totalRows = metadata.row_groups.reduce((sum, rg) => sum + Number(rg.num_rows), 0);
    let processedRows = 0;
    let rowOffset = 0;

    const layerNames = new Set();

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

          const { dxf, layerName } = featureToDxfEntities(geom, props, transform);
          if (dxf) {
            chunk += dxf;
            layerNames.add(layerName);
          }
        }

        if (chunk) await bodyStream.write(encoder.encode(chunk));

        processedRows += rows.length;
        stage2.report((processedRows / totalRows) * 100);
        onStatus?.(`Writing DXF... (${processedRows}/${totalRows})`);
        rowOffset = rgEnd;
      }
    } finally {
      try { await bodyStream.close(); } catch (e) { /* may already be closed */ }
    }

    this._bodyName = bodyName;
    this._layerNames = layerNames;

    // Clean up intermediate parquet
    await this.removeOpfsFile(tempParquetPath.replace('opfs://', ''));

    onProgress?.(100);
    onStatus?.('DXF complete');
  }

  async getDownloadMap(baseName) {
    if (!this._bodyName) return [];
    const bodyFile = await this.getOpfsFile(this._bodyName);
    const encoder = new TextEncoder();
    const { header, footer } = buildDxfEnvelope(this._layerNames || new Set());
    const utmSuffix = this._utmInfo
      ? `.UTM${this._utmInfo.zone}${this._utmInfo.hemisphere}` : '';
    return [{
      downloadName: `${baseName}${utmSuffix}.${this.extension}`,
      blobParts: [encoder.encode(header), bodyFile, encoder.encode(footer)],
    }];
  }
}
