// Shapefile format handler for partial downloads.
// Pipeline: DuckDB filters remote parquet → intermediate parquet on OPFS →
// DuckDB queries metadata → hyparquet reads row-group-by-row-group →
// record data streamed to OPFS → headers prepended at download time.

import { FormatHandler } from './format_base.js';
import { OPFS_PREFIX_SHP_TMP, ScopedProgress, fileToAsyncBuffer, parseWkbHex } from './utils.js';
import { parquetRead, parquetMetadataAsync, parquetSchema } from 'https://esm.sh/hyparquet@1.25.0';
import { compressors } from 'https://esm.sh/hyparquet-compressors@1';
import {
  promoteGeometry, resolveShpTypeMapping, truncateFieldNames,
  ShpWriter, DbfWriter,
  PRJ_WGS84, SHP_TYPE_LABELS,
} from './shp_writer.js';

// DuckDB type name → DBF field type
function duckdbTypeToDbf(type) {
  const t = type.toUpperCase();
  if (t === 'BOOLEAN') return 'logical';
  if (['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
       'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
       'FLOAT', 'DOUBLE', 'DECIMAL'].some(n => t.startsWith(n))) return 'number';
  return 'character';
}

// Internal columns added by our COPY query (not user attributes)
const INTERNAL_COLS = new Set(['geom_wkb', '_geom_type']);

export class ShapefileFormatHandler extends FormatHandler {
  constructor({ ...opts } = {}) {
    super(opts);
  }

  getExpectedBrowserStorageUsage() { return this.estimatedBytes * 2.5; }
  getTotalExpectedDiskUsage() { return this.estimatedBytes * 3; }

  async _write({ onProgress, onStatus, cancelled }) {
    // Stage 1 (0–60%): DuckDB → intermediate parquet on OPFS
    const stage1 = new ScopedProgress(onProgress, 0, 60);
    const tempParquetPath = await this.createIntermediateParquet({
      prefix: OPFS_PREFIX_SHP_TMP,
      extraColumns: ["ST_GeometryType(geometry) AS _geom_type"],
      onProgress: stage1.callback, onStatus, cancelled,
    });

    // Discover columns and geometry types from local intermediate parquet via DuckDB
    onStatus?.('Reading schema...');
    const columns = await this.describeColumns(tempParquetPath, INTERNAL_COLS);
    const attrColumns = columns.map(c => ({ originalName: c.name, type: duckdbTypeToDbf(c.type) }));

    onProgress?.(65);

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    const typesResult = await this.duckdb.conn.query(
      `SELECT DISTINCT _geom_type FROM '${tempParquetPath}' WHERE _geom_type IS NOT NULL`
    );
    const allGeomTypes = new Set();
    for (let i = 0; i < typesResult.numRows; i++) {
      allGeomTypes.add(typesResult.getChildAt(0).get(i));
    }

    onProgress?.(70);

    // Release DuckDB hold on intermediate file
    await this.releaseDuckdbOpfsFile(tempParquetPath);

    if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

    const { shpTypes, typeMapping } = resolveShpTypeMapping(allGeomTypes);
    if (shpTypes.length === 0) throw new Error('No supported geometry types found');

    const fieldMapping = truncateFieldNames(attrColumns.map(c => c.originalName));
    const dbfFields = attrColumns.map((col, i) => ({
      originalName: col.originalName,
      dbfName: fieldMapping[i].dbfName,
      type: col.type,
    }));


    // Stage 2 (50–100%): Read parquet with hyparquet, stream records to OPFS
    onStatus?.('Writing shapefile records...');
    const stage2 = new ScopedProgress(onProgress, 70, 100);

    const file = await this.getOpfsFile(tempParquetPath);
    const asyncBuffer = fileToAsyncBuffer(file);
    const metadata = await parquetMetadataAsync(asyncBuffer);

    // Pre-compute column indices from hyparquet schema
    const hSchema = parquetSchema(metadata);
    const colIndex = {};
    hSchema.children.forEach((child, i) => { colIndex[child.element.name] = i; });
    const iWkb = colIndex['geom_wkb'];
    const iGeomType = colIndex['_geom_type'];
    const attrIndices = attrColumns.map(c => colIndex[c.originalName]);

    // Create writers for each shapefile type
    const writers = {};
    for (const shpType of shpTypes) {
      writers[shpType] = {
        shp: new ShpWriter(shpType),
        dbf: new DbfWriter(dbfFields),
      };
    }

    // Open OPFS writable streams for each type's .shp and .dbf body data
    const opfsStreams = {};
    for (const shpType of shpTypes) {
      const typeSuffix = shpTypes.length > 1 ? `_${SHP_TYPE_LABELS[shpType]}` : '';
      const prefix = `${OPFS_PREFIX_SHP_TMP}${this.tabId}_${Date.now()}`;
      const shpName = `${prefix}${typeSuffix}.shp.body`;
      const dbfName = `${prefix}${typeSuffix}.dbf.body`;
      const shpHandle = await this.getOpfsHandle(shpName, { create: true });
      const dbfHandle = await this.getOpfsHandle(dbfName, { create: true });
      opfsStreams[shpType] = {
        shpName, dbfName, typeSuffix, prefix,
        shpStream: await shpHandle.createWritable(),
        dbfStream: await dbfHandle.createWritable(),
      };
    }

    // Stream records row-group by row-group, flushing to OPFS after each
    const totalRows = metadata.row_groups.reduce((sum, rg) => sum + Number(rg.num_rows), 0);
    let processedRows = 0;
    let rowOffset = 0;

    try {
      for (const rg of metadata.row_groups) {
        if (cancelled()) throw new DOMException('Download cancelled', 'AbortError');

        const rgEnd = rowOffset + Number(rg.num_rows);
        let rows;
        await parquetRead({
          file: asyncBuffer, compressors,
          rowStart: rowOffset, rowEnd: rgEnd,
          onComplete: (data) => { rows = data; },
        });

        for (const row of rows) {
          const wkbHex = row[iWkb];
          const duckdbType = row[iGeomType];
          if (!wkbHex || !duckdbType) continue;

          const mapping = typeMapping.get(duckdbType);
          if (!mapping || !writers[mapping.shpType]) continue;

          let geom = parseWkbHex(wkbHex);
          if (mapping.needsPromote) geom = promoteGeometry(geom);

          const { shp, dbf } = writers[mapping.shpType];
          shp.writeRecord(geom);

          const props = {};
          for (let ci = 0; ci < attrIndices.length; ci++) {
            const val = row[attrIndices[ci]];
            props[dbfFields[ci].originalName] = val != null && typeof val === 'object' ? JSON.stringify(val) : val;
          }
          dbf.writeRecord(props);
        }

        // Flush record data to OPFS and free memory
        for (const shpType of shpTypes) {
          const { shp, dbf } = writers[shpType];
          const { shpStream, dbfStream } = opfsStreams[shpType];
          for (const chunk of shp.flushChunks()) await shpStream.write(chunk);
          for (const chunk of dbf.flushChunks()) await dbfStream.write(chunk);
        }

        processedRows += rows.length;
        stage2.report((processedRows / totalRows) * 100);
        onStatus?.(`Writing shapefile records... (${processedRows}/${totalRows})`);
        rowOffset = rgEnd;
      }
    } finally {
      for (const { shpStream, dbfStream } of Object.values(opfsStreams)) {
        try { await shpStream.close(); } catch (e) { /* may already be closed */ }
        try { await dbfStream.close(); } catch (e) { /* may already be closed */ }
      }
    }

    // Store headers and file references for getDownloadMap
    this._shpOutputFiles = {};
    for (const [shpType, { shp, dbf }] of Object.entries(writers)) {
      if (shp.recNum === 0) continue;
      const { shpName, dbfName, typeSuffix, prefix } = opfsStreams[shpType];

      // .shx is small (header + 8 bytes per record) — write fully now
      const shxParts = shp.generateShxParts();
      const shxName = `${prefix}${typeSuffix}.shx`;
      await this._writeOpfsFile(shxName, [shxParts.header, shxParts.records]);

      // .prj is tiny
      const prjName = `${prefix}${typeSuffix}.prj`;
      await this._writeOpfsFile(prjName, [new TextEncoder().encode(PRJ_WGS84)]);

      this._shpOutputFiles[shpType] = {
        shpBody: shpName,
        shpHeader: shp.generateShpHeader(),
        dbfBody: dbfName,
        dbfHeader: dbf.generateHeader(),
        dbfTerminator: dbf.generateTerminator(),
        shx: shxName,
        prj: prjName,
      };
    }

    // Clean up intermediate parquet
    await this.removeOpfsFile(tempParquetPath.replace('opfs://', ''));

    onProgress?.(100);
    onStatus?.('Shapefile complete');
  }

  async _writeOpfsFile(fileName, parts) {
    const handle = await this.getOpfsHandle(fileName, { create: true });
    const writable = await handle.createWritable();
    for (const part of parts) await writable.write(part);
    await writable.close();
  }

  async getDownloadMap(baseName) {
    if (!this._shpOutputFiles) return [];

    const typeEntries = Object.entries(this._shpOutputFiles);
    const entries = [];

    for (const [shpType, info] of typeEntries) {
      const typeSuffix = typeEntries.length > 1 ? `_${SHP_TYPE_LABELS[shpType]}` : '';
      const name = `${baseName}${typeSuffix}`;

      // .shp: header + body
      const shpBodyFile = await this.getOpfsFile(info.shpBody);
      entries.push({ downloadName: `${name}.shp`, blobParts: [info.shpHeader, shpBodyFile] });

      // .shx: already complete on disk
      const shxFile = await this.getOpfsFile(info.shx);
      entries.push({ downloadName: `${name}.shx`, blobParts: [shxFile] });

      // .dbf: header + body + terminator
      const dbfBodyFile = await this.getOpfsFile(info.dbfBody);
      entries.push({ downloadName: `${name}.dbf`, blobParts: [info.dbfHeader, dbfBodyFile, info.dbfTerminator] });

      // .prj: already complete on disk
      const prjFile = await this.getOpfsFile(info.prj);
      entries.push({ downloadName: `${name}.prj`, blobParts: [prjFile] });
    }

    return entries;
  }
}
