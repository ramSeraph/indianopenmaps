// Shared DuckDB WASM singleton: manages lifecycle and connection.
// Used by parquet_metadata.js (extent queries) and partial_download_handler.js (downloads).

const DUCKDB_BASE = 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir';
import * as duckdb from 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir/duckdb-browser.mjs';

class DuckDBClient {
  constructor() {
    this.db = null;
    this.conn = null;
    this.worker = null;
    this.initialized = false;
    this._bundle = null;
  }

  async init() {
    if (this.initialized) return;

    try {
      if (!this._bundle) {
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
        this._bundle = await duckdb.selectBundle(CUSTOM_BUNDLES);
      }

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${this._bundle.mainWorker}");`], { type: 'text/javascript' })
      );
      try {
        this.worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);

        this.db = new duckdb.AsyncDuckDB(logger, this.worker);
        await this.db.instantiate(this._bundle.mainModule, this._bundle.pthreadWorker);
      } finally {
        URL.revokeObjectURL(worker_url);
      }

      this.conn = await this.db.connect();
      await this.conn.query(`INSTALL spatial; LOAD spatial;`);
      await this.conn.query(`SET builtin_httpfs = false;`);
      await this.conn.query(`INSTALL httpfs; LOAD httpfs;`);
      await this.conn.query(`SET arrow_large_buffer_size=true`);

      this.initialized = true;
      console.log('[DuckDB] Initialized with httpfs');
    } catch (error) {
      console.error('[DuckDB] Failed to initialize:', error);
      throw error;
    }
  }

  /** Kill the DuckDB worker immediately. Next init() call will reinitialize. */
  terminate() {
    if (this.worker) {
      this.worker.terminate();
      console.log('[DuckDB] Worker terminated');
    }
    this.worker = null;
    this.conn = null;
    this.db = null;
    this.initialized = false;
  }
}

export const duckdbClient = new DuckDBClient();
