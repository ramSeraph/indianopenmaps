import { Hono } from 'hono';

import { registerTileRoutes } from './routes/tiles.js';
import { registerStacRoutes } from './routes/stac.js';
import { registerProxyRoutes } from './routes/proxy.js';
import { registerStaticRoutes } from './routes/static.js';
import { registerCogRoutes } from './routes/cog.js';
import { withRequestContext } from './lib/request.js';
import { logger } from './lib/logger.js';
import { logAccess } from './lib/access_log.js';

const app = new Hono();

// Error logging is unsampled; access logging has a separate per-request sample.
app.use('*', (c, next) => withRequestContext(c.req.raw, async requestId => {
  const startedAt = Date.now();
  if (logger.enabled('debug')) logger.debug({
    message: 'Request headers',
    requestId,
    method: c.req.method,
    path: c.req.path,
    requestedWith: c.req.header('x-requested-with') ?? null,
    userAgent: c.req.header('user-agent') ?? null,
    referer: c.req.header('referer') ?? null,
  });
  await next();
  logAccess(c.req.raw, c.res, {
    sampleRate: c.env?.ACCESS_LOG_SAMPLE_RATE, elapsedMs: Date.now() - startedAt,
  });
}, { logLevel: c.env?.LOG_LEVEL }));

// Hono's default error handler writes directly to console.error, bypassing off.
app.onError((error, c) => {
  if ('getResponse' in error) return error.getResponse();
  logger.error({
    event: 'request_error', message: error.message, name: error.name,
    stack: error.stack, method: c.req.method, path: c.req.path,
  });
  return c.text('Internal Server Error', 500, {
    'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store',
  });
});

// Determine server URL from request
function getServerUrl(c) {
  const url = new URL(c.req.url);
  return `${url.protocol}//${url.host}`;
}

async function addRoutes() {
  registerStaticRoutes(app);
  registerProxyRoutes(app, logger);
  await registerStacRoutes(app, logger);
  registerTileRoutes(app, getServerUrl, logger);
  registerCogRoutes(app, logger);
}

// Initialize routes
await addRoutes();

export default app;
