import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import pino from 'pino';

import { registerTileRoutes } from './routes/tiles.js';
import { registerStacRoutes } from './routes/stac.js';
import { registerCogRoutes } from './routes/cog.js';
import { registerCorsProxyRoutes } from './routes/cors_proxy.js';
import { registerStaticRoutes } from './routes/static.js';

import corsWhitelist from './cors_whitelist.json' with { type: 'json' };

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const logger = pino();

const app = new Hono();

// Request logging with pino
app.use('*', async (c, next) => {
  const start = Date.now();
  logger.info({ method: c.req.method, path: c.req.path, msg: 'incoming request' });
  await next();
  const ms = Date.now() - start;
  logger.info({ method: c.req.method, path: c.req.path, status: c.res.status, ms, msg: 'request completed' });
});

const port = 3000;
const staticDir = path.join(__dirname, '..', 'static');

let serverUrl = process.env.FLY_APP_NAME;
if (!serverUrl) {
    serverUrl = 'http://localhost:3000';
} else {
    serverUrl = `https://${serverUrl}.fly.dev`;
}
console.log('server url:', serverUrl);

async function addRoutes() {
  logger.info('adding routes');

  registerStaticRoutes(app, staticDir);
  registerCorsProxyRoutes(app, corsWhitelist, logger);
  registerCogRoutes(app, logger);
  await registerStacRoutes(app, logger);
  registerTileRoutes(app, serverUrl, logger);

  // Static file serving - must be last
  app.use('/*', serveStatic({ root: './static' }));

  logger.info('done adding routes');
}

async function start() {
  try {
    await addRoutes();
    
    serve({
      fetch: app.fetch,
      port: port,
      hostname: '0.0.0.0'
    }, (info) => {
      logger.info(`Server listening at http://${info.address}:${info.port}`);
    });
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
}

start();

