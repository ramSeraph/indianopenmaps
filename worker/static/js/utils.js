import { initDuckDB } from 'geoparquet-extractor';

export function bootstrapDuckDB() {
  return initDuckDB();
}

export function proxyUrl(url, { absolute = false } = {}) {
  const path = `/proxy?url=${encodeURIComponent(url)}`;
  return absolute ? `${window.location.origin}${path}` : path;
}

