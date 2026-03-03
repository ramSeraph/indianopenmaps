// Partial download handler using DuckDB WASM with OPFS
// Writes to OPFS via COPY TO, then streams to user-chosen file

const DUCKDB_BASE = 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir';
// JS API from custom build with OPFS temp directory spillover support
import * as duckdb from 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir/duckdb-browser.mjs';

// Unique tab ID to avoid OPFS conflicts between tabs
const TAB_ID = `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// Memory config: 50% of device RAM, clamped to [512MB, maxMB], step 128MB
const MEMORY_STEP = 128;
const MEMORY_MIN_MB = 512;

export function getDeviceMaxMemoryMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  return Math.max(MEMORY_MIN_MB, Math.floor(deviceMemGB * 1024 * 0.75 / MEMORY_STEP) * MEMORY_STEP);
}

export function getDefaultMemoryLimitMB() {
  const deviceMemGB = navigator.deviceMemory || 4;
  const halfMB = Math.floor(deviceMemGB * 1024 * 0.5 / MEMORY_STEP) * MEMORY_STEP;
  return Math.max(MEMORY_MIN_MB, Math.min(halfMB, getDeviceMaxMemoryMB()));
}

export { MEMORY_STEP, MEMORY_MIN_MB };

export class PartialDownloadHandler {
  constructor() {
    this.db = null;
    this.conn = null;
    this.initialized = false;
    this.cancelled = false;
    this.currentDownload = null;
    this.currentOpfsPath = null;
  }

  async init() {
    if (this.initialized) return;

    try {
      const CUSTOM_BUNDLES = {
        mvp: {
          mainModule: `${DUCKDB_BASE}/duckdb-mvp.wasm`,
          mainWorker: `${DUCKDB_BASE}/duckdb-browser-mvp.worker.js`,
        },
        eh: {
          mainModule: `${DUCKDB_BASE}/duckdb-eh.wasm`,
          mainWorker: `${DUCKDB_BASE}/duckdb-browser-eh.worker.js`,
        },
        coi: {
          mainModule: `${DUCKDB_BASE}/duckdb-coi.wasm`,
          mainWorker: `${DUCKDB_BASE}/duckdb-browser-coi.worker.js`,
          pthreadWorker: `${DUCKDB_BASE}/duckdb-browser-coi.pthread.worker.js`,
        },
      };
      const bundle = await duckdb.selectBundle(CUSTOM_BUNDLES);
      
      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
      );
      const worker = new Worker(worker_url);
      const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
      
      this.db = new duckdb.AsyncDuckDB(logger, worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      URL.revokeObjectURL(worker_url);
      
      this.conn = await this.db.connect();
      await this.conn.query(`SET temp_directory = 'opfs://tmp_${TAB_ID}'`);
      await this.conn.query(`INSTALL spatial; LOAD spatial;`);
      
      this.initialized = true;
      console.log('[PartialDownload] DuckDB WASM initialized');
    } catch (error) {
      console.error('[PartialDownload] Failed to initialize DuckDB:', error);
      throw error;
    }
  }

  isDownloading() {
    return this.currentDownload !== null;
  }

  cancel() {
    this.cancelled = true;
    this.currentDownload = null;
  }

  getPartitionsForBbox(metaJson, bbox) {
    if (!metaJson.extents) return [];
    
    const partitions = [];
    for (const [filename, extent] of Object.entries(metaJson.extents)) {
      const [minx, miny, maxx, maxy] = extent;
      if (!(bbox.east < minx || bbox.west > maxx || 
            bbox.north < miny || bbox.south > maxy)) {
        partitions.push(filename);
      }
    }
    return partitions;
  }

  buildProxyUrl(url) {
    const origin = window.location.origin;
    return `${origin}/proxy?url=${encodeURIComponent(url)}`;
  }

  getFormatInfo(format) {
    switch (format) {
      case 'geojson':
        return {
          ext: '.geojson',
          description: 'GeoJSON',
          accept: { 'application/geo+json': ['.geojson', '.json'] }
        };
      case 'geojsonseq':
        return {
          ext: '.geojsonl',
          description: 'GeoJSON Sequence',
          accept: { 'application/geo+json-seq': ['.geojsonl', '.geojson'] }
        };
      case 'csv':
        return {
          ext: '.csv',
          description: 'CSV with WKT geometry',
          accept: { 'text/csv': ['.csv'] }
        };
      default:
        throw new Error(`Unsupported format: ${format}`);
    }
  }

  /**
   * Must be called directly from click handler (before any async work)
   * to satisfy user gesture requirement for showSaveFilePicker.
   */
  async promptSaveFile(sourceName, bbox, format) {
    const formatInfo = this.getFormatInfo(format);
    // Format: <source>.<west>--<south>--<east>--<north>.<ext>
    // Dots in coordinates replaced with dashes
    const coordStr = [bbox.west, bbox.south, bbox.east, bbox.north]
      .map(c => c.toFixed(4).replace(/\./g, '-'))
      .join('--');
    const baseName = sourceName.replace(/\s+/g, '_');
    const suggestedName = `${baseName}.${coordStr}${formatInfo.ext}`;

    return await window.showSaveFilePicker({
      suggestedName,
      types: [{
        description: formatInfo.description,
        accept: formatInfo.accept
      }]
    });
  }

  /**
   * 1. COPY TO opfs:// temp file (large file stays on disk via OPFS)
   * 2. copyFileToBuffer to read it back
   * 3. Stream buffer to user-chosen file handle
   */
  async download(options) {
    const { sourceName, parquetUrl, baseUrl, partitions, bbox, format, onProgress, onStatus, userFileHandle, memoryLimit } = options;

    if (!userFileHandle) {
      throw new Error('userFileHandle required - call promptSaveFile() first');
    }

    if (!this.initialized) {
      onStatus?.('Initializing DuckDB...');
      await this.init();
    }

    // Apply memory limit (can change between downloads)
    if (memoryLimit) {
      await this.conn.query(`SET memory_limit = '${memoryLimit}'`);
    }

    this.cancelled = false;
    this.currentDownload = { sourceName, bbox };
    const formatInfo = this.getFormatInfo(format);

    // OPFS temp file path
    const opfsPath = `opfs://partial_${TAB_ID}_${Date.now()}${formatInfo.ext}`;
    this.currentOpfsPath = opfsPath;

    try {
      onProgress?.(5);
      onStatus?.('Preparing...');

      // Register the OPFS file name with DuckDB and wait for it to be ready
      await this.db.registerOPFSFileName(opfsPath);
      await sleep(5);

      // Build list of URLs to query
      let urls = [];
      if (parquetUrl) {
        urls = [this.buildProxyUrl(parquetUrl)];
      } else if (partitions && partitions.length > 0) {
        urls = partitions.map(p => this.buildProxyUrl(baseUrl + p));
      }

      if (urls.length === 0) {
        throw new Error('No parquet files to query');
      }

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(10);
      onStatus?.(`Filtering ${urls.length} file(s) and writing to OPFS...`);

      const urlList = urls.map(u => `'${u}'`).join(', ');
      const bboxWkt = `POLYGON((${bbox.west} ${bbox.south}, ${bbox.east} ${bbox.south}, ${bbox.east} ${bbox.north}, ${bbox.west} ${bbox.north}, ${bbox.west} ${bbox.south}))`;

      // COPY TO the registered OPFS file
      // For JSON formats, first discover non-geometry columns, then build feature query
      let jsonFeatureQuery;
      if (format === 'geojsonseq' || format === 'geojson') {
        const schemaResult = await this.conn.query(
          `SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet([${urlList}], union_by_name=true)) WHERE column_name != 'geometry'`
        );
        const propCols = [];
        for (let i = 0; i < schemaResult.numRows; i++) {
          propCols.push(schemaResult.getChildAt(0).get(i));
        }
        // Build struct literal: {'col1': col1, 'col2': col2, ...}
        const structEntries = propCols.map(c => `'${c}', "${c}"`).join(', ');
        jsonFeatureQuery = `
          SELECT json_object(
            'type', 'Feature',
            'geometry', ST_AsGeoJSON(geometry)::JSON,
            'properties', json_object(${structEntries})
          ) as feature
          FROM read_parquet([${urlList}], union_by_name=true)
          WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
        `;
      }

      let copyQuery;
      
      if (format === 'csv') {
        copyQuery = `
          COPY (
            SELECT * EXCLUDE (geometry), ST_AsText(geometry) as geometry_wkt
            FROM read_parquet([${urlList}], union_by_name=true)
            WHERE ST_Intersects(geometry, ST_GeomFromText('${bboxWkt}'))
          ) TO '${opfsPath}' (FORMAT CSV, HEADER true)
        `;
      } else if (format === 'geojsonseq' || format === 'geojson') {
        copyQuery = `
          COPY (${jsonFeatureQuery}) TO '${opfsPath}' (FORMAT CSV, HEADER false, QUOTE '', DELIMITER E'\\x01')
        `;
      }

      await this.conn.query(copyQuery);

      if (this.cancelled) throw new DOMException('Download cancelled', 'AbortError');

      onProgress?.(70);
      onStatus?.('Streaming to your file...');

      // Drop the DuckDB registration so we can access the OPFS file directly
      await this.db.dropFile(opfsPath);
      this.currentOpfsPath = null;

      // Get the OPFS file handle - filename is the path without opfs:// prefix
      const opfsFileName = opfsPath.replace('opfs://', '');
      const opfsRoot = await navigator.storage.getDirectory();
      const opfsFileHandle = await opfsRoot.getFileHandle(opfsFileName);
      const opfsFile = await opfsFileHandle.getFile();

      const writableStream = await userFileHandle.createWritable();
      const totalSize = opfsFile.size;

      if (format === 'geojson') {
        // Wrap newline-delimited features into a GeoJSON FeatureCollection
        const encoder = new TextEncoder();
        await writableStream.write(encoder.encode('{"type":"FeatureCollection","features":['));
        
        const reader = opfsFile.stream().getReader();
        const decoder = new TextDecoder();
        let leftover = '';
        let firstFeature = true;
        let bytesRead = 0;

        while (true) {
          if (this.cancelled) { await writableStream.abort(); throw new DOMException('Download cancelled', 'AbortError'); }
          const { done, value } = await reader.read();
          if (done) break;
          bytesRead += value.length;

          const chunk = leftover + decoder.decode(value, { stream: true });
          const lines = chunk.split('\n');
          leftover = lines.pop(); // last partial line

          for (const line of lines) {
            if (line.length === 0) continue;
            await writableStream.write(encoder.encode((firstFeature ? '' : ',') + line));
            firstFeature = false;
          }
          onProgress?.(70 + Math.floor((bytesRead / totalSize) * 25));
        }
        // Handle any remaining leftover
        if (leftover.length > 0) {
          await writableStream.write(encoder.encode((firstFeature ? '' : ',') + leftover));
        }
        await writableStream.write(encoder.encode(']}'));
      } else {
        // CSV and GeoJSONSeq: stream directly
        const reader = opfsFile.stream().getReader();
        let bytesRead = 0;

        while (true) {
          if (this.cancelled) { await writableStream.abort(); throw new DOMException('Download cancelled', 'AbortError'); }
          const { done, value } = await reader.read();
          if (done) break;
          await writableStream.write(value);
          bytesRead += value.length;
          onProgress?.(70 + Math.floor((bytesRead / totalSize) * 25));
        }
      }

      await writableStream.close();

      // Cleanup OPFS file
      try { await opfsRoot.removeEntry(opfsFileName); } catch (e) { /* ignore */ }

      onProgress?.(100);
      onStatus?.('Download complete!');

      return true;

    } catch (error) {
      if (error.name === 'AbortError') {
        onStatus?.('Download cancelled');
      } else {
        console.error('[PartialDownload] Download failed:', error);
        onStatus?.(`Error: ${error.message}`);
      }
      throw error;
    } finally {
      await this.cleanup();
      this.currentDownload = null;
    }
  }

  async cleanup() {
    try {
      if (this.currentOpfsPath) {
        try {
          await this.db.dropFile(this.currentOpfsPath);
        } catch (e) {
          // May already be dropped
        }
      }
    } catch (e) {
      // Ignore cleanup errors
    } finally {
      this.currentOpfsPath = null;
    }
  }

  async dispose() {
    this.cancel();
    await this.cleanup();
    if (this.conn) {
      await this.conn.close();
      this.conn = null;
    }
    if (this.db) {
      await this.db.terminate();
      this.db = null;
    }
    this.initialized = false;
  }
}
