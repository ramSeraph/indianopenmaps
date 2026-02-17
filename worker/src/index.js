import { Hono } from 'hono';

import { registerTileRoutes } from './routes/tiles.js';
import { registerStacRoutes } from './routes/stac.js';
import { registerProxyRoutes } from './routes/proxy.js';
import { registerStaticRoutes } from './routes/static.js';
import { registerCogRoutes } from './routes/cog.js';

const app = new Hono();

// Simple console logger for Workers
const logger = {
  info: (...args) => console.log('[INFO]', ...args),
  error: (...args) => console.error('[ERROR]', ...args),
  warn: (...args) => console.warn('[WARN]', ...args),
};

// Determine server URL from request
function getServerUrl(c) {
  const url = new URL(c.req.url);
  return `${url.protocol}//${url.host}`;
}

async function addRoutes() {
  logger.info('adding routes');

  registerStaticRoutes(app);
  registerProxyRoutes(app, logger);
  await registerStacRoutes(app, logger);
  registerTileRoutes(app, getServerUrl, logger);
  registerCogRoutes(app, logger);

  logger.info('done adding routes');
}

// Initialize routes
await addRoutes();

export default app;
