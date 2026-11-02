export const DEFAULT_SOURCE_REFRESH_MS = 60 * 60 * 1000;
const DEFAULT_TILE_MAX_AGE_SECONDS = 300;

function seconds(value, fallback) {
  // Missing or malformed settings use the defaults; only an explicit 0 disables
  // refresh. Reject fractions, negatives and durations that overflow milliseconds.
  if (!/^\d+$/.test(String(value ?? '').trim())) return fallback;
  const result = Number(value);
  return Number.isSafeInteger(result * 1000) ? result : fallback;
}

export function cachePolicy(env) {
  const refreshMs = seconds(env?.CACHE_REVALIDATION_SECONDS, DEFAULT_SOURCE_REFRESH_MS / 1000) * 1000;
  const tileMaxAge = seconds(env?.TILE_CACHE_MAX_AGE_SECONDS, DEFAULT_TILE_MAX_AGE_SECONDS);
  return {
    refreshMs,
    revalidate: refreshMs > 0,
    tileCacheControl: `public, max-age=${tileMaxAge}, must-revalidate`,
  };
}
