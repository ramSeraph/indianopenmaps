// Web Worker for wa-sqlite with OPFSAdaptiveVFS (OPFS-backed)
// Receives SQL commands via postMessage, executes them, responds with results

import SQLiteESMFactory from 'https://ramseraph.github.io/sqwab/dist/wa-sqlite-async.mjs';
import * as SQLite from 'https://ramseraph.github.io/sqwab/src/sqlite-api.js';
import { OPFSAdaptiveVFS } from 'https://ramseraph.github.io/sqwab/src/examples/OPFSAdaptiveVFS.js';

let sqlite3 = null;
let db = null;

async function init(dbPath) {
  const module = await SQLiteESMFactory();
  sqlite3 = SQLite.Factory(module);

  const vfs = await OPFSAdaptiveVFS.create('opfs-adaptive', module);
  sqlite3.vfs_register(vfs, true);

  db = await sqlite3.open_v2(dbPath);
  return true;
}

async function exec(sql) {
  if (!db) throw new Error('Database not initialized');
  await sqlite3.exec(db, sql);
}

// Execute a prepared statement with multiple parameter sets (batch insert)
// sqlite3.statements() is an async generator that handles prepare/finalize
async function insertBatch(sql, paramSets) {
  if (!db) throw new Error('Database not initialized');
  for await (const stmt of sqlite3.statements(db, sql)) {
    for (const params of paramSets) {
      await sqlite3.reset(stmt);
      sqlite3.bind_collection(stmt, params);
      await sqlite3.step(stmt);
    }
    // Don't call finalize — the statements() generator handles it
  }
}

async function close() {
  if (db) {
    await sqlite3.close(db);
    db = null;
  }
}

self.onmessage = async (e) => {
  const { id, method, args } = e.data;
  try {
    let result;
    switch (method) {
      case 'init':
        result = await init(args.dbPath);
        break;
      case 'exec':
        result = await exec(args.sql);
        break;
      case 'insertBatch':
        result = await insertBatch(args.sql, args.paramSets);
        break;
      case 'close':
        result = await close();
        break;
      default:
        throw new Error(`Unknown method: ${method}`);
    }
    self.postMessage({ id, result });
  } catch (error) {
    self.postMessage({ id, error: error.message || String(error) });
  }
};
