import { AsyncLocalStorage } from 'node:async_hooks';
import { CacheMissError, UpstreamHttpError } from './errors.js';

const MAX_RETRIES = 2;
const MAX_RETRY_WAIT_MS = 10_000;
const INITIAL_RETRY_WAIT_MS = 1_000;
const RETRYABLE_STATUSES = new Set([429, 503]);
// Scope cache probes to one async operation; concurrent requests can still fetch.
const cacheOnly = new AsyncLocalStorage();

export function fetchUpstream(input, init) {
  if (cacheOnly.getStore()) throw new CacheMissError();
  return fetch(input, init);
}

// RFC 9110 permits either non-negative integer seconds or an HTTP-date.
export function parseRetryAfter(value, now = Date.now()) {
  if (value == null) return null;
  value = value.trim();
  if (/^\d+$/.test(value)) return Number(value) * 1000;
  // Date.parse also accepts numbers such as "-1" and "1.5" as dates.
  if (!/^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)/i.test(value)) return null;
  // The obsolete asctime HTTP-date omits its timezone but still means GMT.
  const date = Date.parse(/GMT$/i.test(value) ? value : `${value} GMT`);
  return Number.isFinite(date) ? Math.max(0, date - now) : null;
}

function waitForRetry(ms, signal) {
  signal?.throwIfAborted();
  if (ms === 0) return;
  return new Promise((resolve, reject) => {
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * Retry upstream reads through their existing caches.
 * Operations must use fetchUpstream for network I/O so an immediate cache probe
 * stops at a cache miss. The normal attempt checks those caches again after waiting.
 * Timers and pending I/O stay local to the calling Worker request.
 */
export async function withRetries(operation, { signal } = {}) {
  let waited = 0;
  for (let attempt = 0; ; attempt++) {
    signal?.throwIfAborted();
    try {
      return await operation();
    } catch (error) {
      if (!(error instanceof UpstreamHttpError) || !RETRYABLE_STATUSES.has(error.upstreamStatus)) throw error;

      signal?.throwIfAborted();
      try {
        // A peer may have filled the cache while the failed request was in flight.
        // The fetch guard prevents a cache miss from retrying the upstream early.
        const cached = await cacheOnly.run(true, operation);
        await error.response.body?.cancel().catch(() => {});
        signal?.throwIfAborted();
        return cached;
      } catch (cacheError) {
        if (!(cacheError instanceof CacheMissError)) throw cacheError;
      }

      const delay = parseRetryAfter(error.retryAfter) ?? INITIAL_RETRY_WAIT_MS * 2 ** attempt;
      // Never shorten the server's requested delay to fit our waiting budget.
      if (attempt === MAX_RETRIES || waited + delay > MAX_RETRY_WAIT_MS) throw error;

      // Release the connection before sleeping: Workers limit concurrent fetches.
      await error.response.body?.cancel().catch(() => {});
      await waitForRetry(delay, signal);
      waited += delay;
    }
  }
}

/** Fetch with the same retry policy, returning the final response on exhaustion. */
export async function fetchWithRetry(input, init = {}) {
  const method = (init.method ?? input.method ?? 'GET').toUpperCase();
  const signal = init.signal === undefined ? input.signal : init.signal;
  signal?.throwIfAborted();
  if (method !== 'GET' && method !== 'HEAD') return fetchUpstream(input, init);

  try {
    return await withRetries(async () => {
      const response = await fetchUpstream(input, init);
      if (RETRYABLE_STATUSES.has(response.status)) throw new UpstreamHttpError(response);
      return response;
    }, { signal });
  } catch (error) {
    if (error instanceof UpstreamHttpError) return error.response;
    throw error;
  }
}
