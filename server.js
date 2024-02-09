const path = require('node:path')
const fastify = require('fastify')({ logger: true });

const fastifyStatic = require('@fastify/static');
const MosaicHandler = require('./mosaic_handler');
const PMTilesHandler = require('./pmtiles_handler');

const routes = require('./routes.json');

const logger = fastify.log;

const handlerMap = {};

const port = 3000;

var serverUrl = process.env.FLY_APP_NAME;
if (!serverUrl) {
    serverUrl = 'http://localhost:3000';
} else {
    serverUrl = `https://${serverUrl}.fly.dev`;
}
console.log('server url:', serverUrl);

async function getTile(handler, request, reply) {
  const { z, x, y } = request.params;
  const [ arr, mimeType ] = await handler.getTile(z,x,y);
  if (arr) {
    return reply.header('Content-Type', mimeType)
                .header('Cache-Control', 'max-age=86400000')
                .header('Access-Control-Allow-Origin', "*")
                .send(new Uint8Array(arr.data));
  }
  return reply.code(404)
              .header('Access-Control-Allow-Origin', "*")
              .send('');
}

async function getTileJson(handler, request, reply) {
  const config = await handler.getConfig();
  const tileJsonUrl = request.url;
  const baseUrl = tileJsonUrl.replace(/\/tiles\.json.*$/, '');
  config['tiles'] = [ serverUrl + baseUrl + `/{z}/{x}/{y}.${handler.tileSuffix}` ];

  return reply.header('Content-Type', 'application/json')
              .header('Cache-Control', 'max-age=86400000')
              .header('Access-Control-Allow-Origin', "*")
              .send(JSON.stringify(config));
}

async function initializeHandlers() {
  logger.info('initializing handlers');
  const promises = Object.keys(handlerMap).map(async (k) => {
    logger.info(`initializing ${k}`);
    try {
      await handlerMap[k].init();
    }
    catch(err) {
      console.log(`failed to initialize ${k}, error: ${err}`);
    }
  });
  await Promise.all(promises);
  logger.info('done initializing handlers');
}

function addRoutes() {
  logger.info('adding routes');
  Object.keys(handlerMap).forEach((rPrefix, _) => {
    const handler = handlerMap[rPrefix];
    const tileSuffix = handler.tileSuffix;
    fastify.get(`${rPrefix}:z/:x/:y.${tileSuffix}`, getTile.bind(null, handler));
    fastify.get(`${rPrefix}tiles.json`, getTileJson.bind(null, handler));
    fastify.get(`${rPrefix}view`, async (request, reply) => {
      return reply.sendFile("view.html");
    });
    fastify.get(`${rPrefix}view.js`, async (request, reply) => {
      return reply.sendFile("view.js");
    });
    fastify.get(`${rPrefix}view.css`, async (request, reply) => {
      return reply.sendFile("view.css");
    });
  });
  logger.info('done adding routes');
}

function createHandlers() {
  Object.keys(routes).forEach((rPrefix, _) => {
    const rInfo = routes[rPrefix];
    var datameetAttribution = true;
    if ('datameet_attribution' in rInfo) {
      datameetAttribution = rInfo['datameet_attribution'];
    }
    if (rInfo['type'] == 'mosaic') {
        handlerMap[rPrefix] = new MosaicHandler(rInfo['url'], 'pbf', logger, datameetAttribution);
    } else {
        handlerMap[rPrefix] = new PMTilesHandler(rInfo['url'], 'pbf', logger, datameetAttribution);
    }
  });
}

async function start() {
  try {
    createHandlers();
    fastify.register(fastifyStatic, {
      root: path.join(__dirname, 'static'),
    });

    fastify.addHook('onReady', initializeHandlers);
    addRoutes();
    await fastify.listen({ host: '0.0.0.0', port: port });
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
}

start();
