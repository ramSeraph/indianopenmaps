import { initDuckDB } from 'geoparquet-extractor';

const DUCKDB_DIST = 'https://cdn.jsdelivr.net/npm/duckdb-wasm-opfs-tempdir@1.33.0/dist';

export function bootstrapDuckDB() {
  return initDuckDB(DUCKDB_DIST);
}

export function proxyUrl(url, { absolute = false } = {}) {
  const path = `/proxy?url=${encodeURIComponent(url)}`;
  return absolute ? `${window.location.origin}${path}` : path;
}

