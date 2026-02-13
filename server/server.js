import path from 'node:path';
import { fileURLToPath } from 'node:url';
import Fastify from 'fastify';
import fastifyStatic from '@fastify/static';

import { registerTileRoutes } from './routes/tiles.js';
import { registerStacRoutes } from './routes/stac.js';
import { registerCogRoutes } from './routes/cog.js';
import { registerCorsProxyRoutes } from './routes/cors_proxy.js';
import { registerStaticRoutes } from './routes/static.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const fastify = Fastify({ logger: true });
const logger = fastify.log;

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

  registerStaticRoutes(fastify);
  registerCorsProxyRoutes(fastify, logger);
  registerCogRoutes(fastify, logger);
  await registerStacRoutes(fastify, logger);
  registerTileRoutes(fastify, serverUrl, logger);

  logger.info('done adding routes');
}

async function start() {
  try {
    await fastify.register(fastifyStatic, {
      root: staticDir,
    });

    await addRoutes();
    await fastify.listen({ host: '0.0.0.0', port: port });
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
}

start();
