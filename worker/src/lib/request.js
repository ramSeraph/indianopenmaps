import { CacheMissError, UpstreamHttpError } from './errors.js';
import { requestContext } from './request_context.js';
import { logger, parseLogLevel } from './logger.js';

const MAX_RETRIES = 2;
const MAX_RETRY_WAIT_MS = 10_000;
const INITIAL_RETRY_WAIT_MS = 1_000;
const RETRYABLE_STATUSES = new Set([429, 503, 522]);
const SLOW_FETCH_MS = 5_000;

export function isCacheOnly() { return requestContext.getStore()?.cacheOnly === true; }

export function withRequestContext(request, operation, { logLevel } = {}) {
  const context = {
    requestId: crypto.randomUUID(), requestPath: new URL(request.url).pathname,
    logLevel: parseLogLevel(logLevel),
  };
  return requestContext.run(context, () => operation(context.requestId));
}

export async function fetchUpstream(input, init) {
  const context = requestContext.getStore();
  if (context?.cacheOnly) throw new CacheMissError();
  const start = Date.now();
  let response, failure;
  try {
    response = await fetch(input, init);
    return response;
  } catch (error) {
    failure = error;
    throw error;
  } finally {
    const elapsedMs = Date.now() - start;
    const slow = elapsedMs >= SLOW_FETCH_MS;
    const failed = failure || response?.status >= 400;
    const level = failed ? 'error' : slow ? 'warn' : 'info';
    // Avoid constructing diagnostic records on the normal production path.
    if (logger.enabled(level)) {
      // Release redirects contain signed query strings. Log source paths and host
      // names only, never full redirect URLs, credentials, or arbitrary headers.
      let source;
      try { source = new URL(input.url ?? input); } catch { /* Preserve invalid-URL fetch errors. */ }
      logger[level]({
        event: 'upstream_fetch',
        message: response?.status === 429 ? 'Upstream rate limited'
          : failed ? 'Upstream fetch failed' : slow ? 'Slow upstream fetch' : 'Upstream fetch',
        requestId: context?.requestId ?? null,
        requestPath: context?.requestPath ?? null,
        status: response?.status ?? null,
        method: (init?.method ?? input.method ?? 'GET').toUpperCase(),
        host: source?.host ?? null,
        path: source?.pathname ?? null,
        range: new Headers(init?.headers ?? input.headers).get('Range'),
        responseHost: response?.url ? new URL(response.url).host : null,
        attempt: context?.attempt ?? 1,
        retryAfter: response?.headers.get('Retry-After') ?? null,
        startedAt: start,
        // fetch resolves at response headers; body transfer/decoding are separate.
        elapsedMs,
        githubRequestId: response?.headers.get('X-GitHub-Request-Id') ?? null,
        ray: response?.headers.get('CF-Ray') ?? null,
        error: failure?.name ?? null,
      });
    }
  }
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
      return await requestContext.run({ ...requestContext.getStore(), attempt: attempt + 1 }, operation);
    } catch (error) {
      if (!(error instanceof UpstreamHttpError) || !RETRYABLE_STATUSES.has(error.upstreamStatus)) throw error;

      signal?.throwIfAborted();
      try {
        // A peer may have filled the cache while the failed request was in flight.
        // The fetch guard prevents a cache miss from retrying the upstream early.
        const cached = await requestContext.run({ ...requestContext.getStore(), cacheOnly: true, attempt: attempt + 1 }, operation);
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
