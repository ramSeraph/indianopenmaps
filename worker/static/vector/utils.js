import { initDuckDB } from 'geoparquet-extractor';

export const DUCKDB_MEMORY_MIN_MB = 512;
export const DUCKDB_MEMORY_MAX_MB = 2048;

export function bootstrapDuckDB() {
  return initDuckDB();
}

export function chooseDuckDBMemoryLimitMB() {
  const ramGB = navigator.deviceMemory ?? 4; // coarse; often maxes out at 8
  const mb = Math.floor(ramGB * 1024 * 0.25);
  return Math.max(DUCKDB_MEMORY_MIN_MB, Math.min(DUCKDB_MEMORY_MAX_MB, mb));
}

export function proxyUrl(url, { absolute = false } = {}) {
  const path = `/proxy?url=${encodeURIComponent(url)}`;
  return absolute ? `${window.location.origin}${path}` : path;
}
