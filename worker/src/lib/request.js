const MAX_RETRIES = 2;
const MAX_RETRY_WAIT_MS = 10_000;
const INITIAL_RETRY_WAIT_MS = 1_000;

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
 * Fetch with bounded retries for rate limiting and temporary unavailability.
 * Returns the final Response, including its status and Retry-After header.
 * Timers and pending I/O stay local to the calling Worker request.
 */
export async function fetchWithRetry(input, init = {}) {
  const method = (init.method ?? input.method ?? 'GET').toUpperCase();
  const signal = init.signal === undefined ? input.signal : init.signal;
  const canRetry = method === 'GET' || method === 'HEAD';
  let waited = 0;

  for (let attempt = 0; ; attempt++) {
    signal?.throwIfAborted();
    const response = await fetch(input, init);
    if (!canRetry || ![429, 503].includes(response.status) || attempt === MAX_RETRIES) return response;

    const delay = parseRetryAfter(response.headers.get('Retry-After')) ?? INITIAL_RETRY_WAIT_MS * 2 ** attempt;
    // Never shorten the server's requested delay to fit our waiting budget.
    if (waited + delay > MAX_RETRY_WAIT_MS) return response;

    // Release the connection before sleeping: Workers limit concurrent fetches.
    await response.body?.cancel().catch(() => {});
    await waitForRetry(delay, signal);
    waited += delay;
  }
}
