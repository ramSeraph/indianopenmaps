export const DEFAULT_SOURCE_REFRESH_MS = 60 * 60 * 1000;

function seconds(value, fallback) {
  // Missing or malformed settings use the defaults; only an explicit 0 disables
  // refresh. Reject fractions, negatives and durations that overflow milliseconds.
  if (!/^\d+$/.test(String(value ?? '').trim())) return fallback;
  const result = Number(value);
  return Number.isSafeInteger(result * 1000) ? result : fallback;
}

export function cachePolicy(env) {
  const periodSeconds = seconds(env?.CACHE_REVALIDATION_SECONDS, DEFAULT_SOURCE_REFRESH_MS / 1000);
  const refreshMs = periodSeconds * 1000;
  return {
    refreshMs,
    revalidate: refreshMs > 0,
    tileCacheControl: `public, max-age=${periodSeconds}, must-revalidate`,
  };
}
