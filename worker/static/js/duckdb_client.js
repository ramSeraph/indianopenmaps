// Shared DuckDB WASM singleton: manages lifecycle and connection.
// Used by parquet_metadata.js (extent queries) and partial_download_handler.js (downloads).

const DUCKDB_BASE = 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir';
import * as duckdb from 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir/duckdb-browser.mjs';
import { proxyUrl } from './utils.js';

class DuckDBClient {
  constructor() {
    this.db = null;
    this.conn = null;
    this.initialized = false;
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
      try {
        const worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);

        this.db = new duckdb.AsyncDuckDB(logger, worker);
        await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      } finally {
        URL.revokeObjectURL(worker_url);
      }

      this.conn = await this.db.connect();
      await this.conn.query(`INSTALL spatial; LOAD spatial;`);
      // Disable built-in HTTP handler first, then load httpfs for proper range request support
      await this.conn.query(`SET builtin_httpfs = false;`);
      await this.conn.query(`INSTALL httpfs; LOAD httpfs;`);

      this.initialized = true;
      console.log('[DuckDB] Initialized with httpfs');
    } catch (error) {
      console.error('[DuckDB] Failed to initialize:', error);
      throw error;
    }
  }

  buildProxyUrl(url) {
    return `${window.location.origin}${proxyUrl(url)}`;
  }
}

export const duckdbClient = new DuckDBClient();
