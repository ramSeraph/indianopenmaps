import { requestContext } from './request_context.js';

const levels = { off: -1, error: 0, warn: 1, info: 2, debug: 3 };

export function parseLogLevel(value) {
  const level = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return Object.hasOwn(levels, level) ? level : 'error';
}

function enabled(level) {
  return levels[level] <= levels[requestContext.getStore()?.logLevel ?? 'error'];
}

// Resolve the level at each call, including in handlers shared across requests.
// Preserve structured objects for Cloudflare's field indexing.
export const logger = {
  enabled,
  // Access logs have their own sampling policy, independent of severity.
  // The global off switch still suppresses all application output.
  access: record => { if (enabled('error')) console.log(record); },
  error: (...args) => { if (enabled('error')) console.error(...args); },
  warn: (...args) => { if (enabled('warn')) console.warn(...args); },
  info: (...args) => { if (enabled('info')) console.log(...args); },
  debug: (...args) => { if (enabled('debug')) console.debug(...args); },
};
