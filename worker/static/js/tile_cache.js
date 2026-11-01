// One-time migration away from responses cached for 1,000 days. Keep this token
// stable across data publications; HTTP freshness and ETags handle later updates.
const CACHE_MIGRATION = 'iomaps-cache=2';

export function tileCacheUrl(url) {
  if (!globalThis.IOMAPS_CONFIG?.tileCacheRevalidation) return url;
  // Avoid URL serialization: it escapes MapLibre/Leaflet's {z}/{x}/{y} tokens.
  const hash = url.indexOf('#');
  const base = hash < 0 ? url : url.slice(0, hash);
  const fragment = hash < 0 ? '' : url.slice(hash);
  if (/(?:\?|&)iomaps-cache=/.test(base)) return url;
  return `${base}${base.includes('?') ? '&' : '?'}${CACHE_MIGRATION}${fragment}`;
}

// Only used for Indian Open Maps sources, not third-party basemap metadata.
export async function fetchTileJSON(url) {
  const response = await fetchTileMetadata(url);
  if (!response.ok) throw new Error(`TileJSON request failed (${response.status})`);
  const config = await response.json();
  return { ...config, tiles: (config.tiles || []).map(tileCacheUrl) };
}

export function fetchTileMetadata(url) {
  return fetch(tileCacheUrl(url), globalThis.IOMAPS_CONFIG?.tileCacheRevalidation ? { cache: 'no-cache' } : undefined);
}
