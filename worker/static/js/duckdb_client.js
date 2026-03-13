// DuckDB WASM client: manages lifecycle and connection.
// Each consumer creates its own instance for independent lifecycle control.

const DUCKDB_BASE = 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir';
import * as duckdb from 'https://ramseraph.github.io/duckdb-wasm/v1.33.0-opfs-tempdir/duckdb-browser.mjs';

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

// Resolved once, shared across all DuckDBClient instances
let bundlePromise = null;

class DuckDBClient {
  constructor() {
    this._db = null;
    this._conn = null;
    this._worker = null;
    this._initialized = false;
  }

  get db() {
    if (this._db) return this._db;
    throw new DOMException('DuckDB client unavailable', 'AbortError');
  }

  get conn() {
    if (this._conn) return this._conn;
    throw new DOMException('DuckDB client unavailable', 'AbortError');
  }

  async init() {
    if (this._initialized) return;

    try {
      if (!bundlePromise) {
        bundlePromise = duckdb.selectBundle(CUSTOM_BUNDLES);
      }
      const bundle = await bundlePromise;

      const worker_url = URL.createObjectURL(
        new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
      );
      try {
        this._worker = new Worker(worker_url);
        const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);

        this._db = new duckdb.AsyncDuckDB(logger, this._worker);
        await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
      } finally {
        URL.revokeObjectURL(worker_url);
      }

      this._conn = await this._db.connect();
      await this._conn.query(`INSTALL spatial; LOAD spatial;`);
      await this._conn.query(`SET builtin_httpfs = false;`);
      await this._conn.query(`INSTALL httpfs; LOAD httpfs;`);
      await this._conn.query(`SET arrow_large_buffer_size=true`);

      this._initialized = true;
      console.log('[DuckDB] Initialized with httpfs');
    } catch (error) {
      console.error('[DuckDB] Failed to initialize:', error);
      throw error;
    }
  }

  /** Kill the DuckDB worker immediately. Next init() call will reinitialize. */
  terminate() {
    if (this._worker) {
      this._worker.terminate();
      console.log('[DuckDB] Worker terminated');
    }
    this._worker = null;
    this._conn = null;
    this._db = null;
    this._initialized = false;
  }
}

export { DuckDBClient };
