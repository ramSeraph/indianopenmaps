import { logger } from './logger.js';
import { requestContext } from './request_context.js';

const DEFAULT_SAMPLE_RATE = 0.01;

function parseSampleRate(value) {
  if ((typeof value !== 'string' && typeof value !== 'number') ||
      (typeof value === 'string' && value.trim() === '')) return DEFAULT_SAMPLE_RATE;
  const rate = Number(value);
  return Number.isFinite(rate) && rate >= 0 && rate <= 1 ? rate : DEFAULT_SAMPLE_RATE;
}

export function logAccess(request, response, { sampleRate, elapsedMs }) {
  if (!logger.enabled('error')) return;
  const rate = parseSampleRate(sampleRate);
  if (rate === 0 || (rate < 1 && Math.random() >= rate)) return;

  // Keep the referring site/page useful for traffic queries without storing
  // credentials, search parameters, fragments, or malformed header contents.
  let referer = null;
  let refererHost = null;
  try {
    const url = new URL(request.headers.get('Referer'));
    if (url.protocol === 'https:' || url.protocol === 'http:') {
      referer = url.origin + url.pathname;
      refererHost = url.host;
    }
  } catch { /* Missing or invalid referrer. */ }

  logger.access({
    event: 'access',
    requestId: requestContext.getStore()?.requestId ?? null,
    method: request.method,
    path: new URL(request.url).pathname,
    status: response.status,
    referer,
    refererHost,
    elapsedMs,
    sampleRate: rate,
  });
}
