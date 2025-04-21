const path = require('node:path')
const fastify = require('fastify')({ logger: true });

const fastifyStatic = require('@fastify/static');
const MosaicHandler = require('./mosaic_handler');
const PMTilesHandler = require('./pmtiles_handler');

const routes = require('./routes.json');

const getLocaterPage = require('./wikidata_locater');

const ancillary_routes = require('./ancillary_routes');

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
  var { z, x, y } = request.params;
  try {
    z = parseInt(z);
    x = parseInt(x);
    y = parseInt(y);
  } catch(err) {
    return reply.code(400)
                .header('Access-Control-Allow-Origin', "*")
                .send(`non integer values in tile url`);
  }

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

async function getTitle(handler, request, reply) {
  const title = handler.getTitle();

  return reply.header('Content-Type', 'application/json')
              .header('Cache-Control', 'max-age=86400000')
              .header('Access-Control-Allow-Origin', "*")
              .send(JSON.stringify({ 'title': title }));
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

function addRoutes() {
  logger.info('adding routes');

  fastify.get('/', async (request, reply) => {
      return reply.header('Content-Type', 'text/html; charset=utf-8')
                  .send(getLocaterPage(request, false));
  });
  fastify.get('/wikidata-locater', async (request, reply) => {
      return reply.header('Content-Type', 'text/html; charset=utf-8')
                  .send(getLocaterPage(request, true));
  });
  fastify.get('/main-dark.css', async (request, reply) => {
    return reply.sendFile("main-dark.css");
  });
  fastify.get('/view.css', async (request, reply) => {
    return reply.sendFile("view.css");
  });
  fastify.get('/view.js', async (request, reply) => {
    return reply.sendFile("view.js");
  });
  fastify.get('/raster_view.css', async (request, reply) => {
    return reply.sendFile("raster_view.css");
  });
  fastify.get('/static_view.js', async (request, reply) => {
    return reply.sendFile("raster_view.js");
  });

  Object.keys(handlerMap).forEach((rPrefix, _) => {
    const handler = handlerMap[rPrefix];
    const tileSuffix = handler.tileSuffix;
    fastify.get(`${rPrefix}:z/:x/:y.${tileSuffix}`, getTile.bind(null, handler));
    fastify.get(`${rPrefix}tiles.json`, getTileJson.bind(null, handler));
    fastify.get(`${rPrefix}title`, getTitle.bind(null, handler));
    if (handler.type == 'raster') {
      fastify.get(`${rPrefix}rasterview`, async (request, reply) => {
        return reply.sendFile("raster_view.html");
      });
    } else {
      fastify.get(`${rPrefix}view`, async (request, reply) => {
        return reply.sendFile("view.html");
      });
    }
  });

  // abusing this server for all my cors proxy requirements
  ancillary_routes.addSOIRoutes(fastify);
  ancillary_routes.addLGDRoutes(fastify);
  ancillary_routes.addLGDWikidataRoutes(fastify);
  ancillary_routes.addEGazetteRoutes(fastify);
  logger.info('done adding routes');
}

function createHandlers() {
  Object.keys(routes).forEach((rPrefix, _) => {
    const rInfo = routes[rPrefix];
    var datameetAttribution = true;
    if ('datameet_attribution' in rInfo) {
      datameetAttribution = rInfo['datameet_attribution'];
    }
    var type = 'vector';
    if ('type' in rInfo) {
      type = rInfo['type'];
    }
    var tilesuffix = 'pbf';
    if (type === 'raster') {
      tilesuffix = 'webp';
    }
    if (rInfo['handlertype'] === 'mosaic') {
      handlerMap[rPrefix] = new MosaicHandler(rInfo['url'], type, tilesuffix, logger, datameetAttribution);
    } else {
      handlerMap[rPrefix] = new PMTilesHandler(rInfo['url'], type, tilesuffix, logger, datameetAttribution);
    }
    handlerMap[rPrefix].setTitle(rInfo['name']);
  });
}

async function start() {
  try {
    createHandlers();
    fastify.register(fastifyStatic, {
      root: path.join(__dirname, '..', 'static'),
    });

    addRoutes();
    await fastify.listen({ host: '0.0.0.0', port: port });
  } catch (err) {
    logger.error(err);
    process.exit(1);
  }
}

start();
