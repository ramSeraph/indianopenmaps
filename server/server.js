const path = require('node:path')
const fastify = require('fastify')({ logger: true });

const fastifyStatic = require('@fastify/static');
const sharp = require('sharp');
const MosaicHandler = require('./mosaic_handler');
const PMTilesHandler = require('./pmtiles_handler');
const STACHandler = require('./stac_handler');
const COGHandler = require('./cog_handler');
const { HttpError, UnknownError } = require('./errors');

const routes = require('./routes.json');
const corsWhitelist = require('./cors_whitelist.json');

const logger = fastify.log;

const handlerMap = {};
let stacHandler = null;
let cogHandler = null;

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

async function getTilePng(handler, request, reply) {
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
    const webpBuffer = Buffer.from(arr.data);
    const pngBuffer = await sharp(webpBuffer).png().toBuffer();
    
    return reply.header('Content-Type', 'image/png')
                .header('Cache-Control', 'max-age=86400000')
                .header('Access-Control-Allow-Origin', "*")
                .send(pngBuffer);
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
      return reply.sendFile("index.html");
  });
  
  fastify.get('/vectors', async (request, reply) => {
      return reply.sendFile("vectors.html");
  });
  
  fastify.get('/rasters', async (request, reply) => {
      return reply.sendFile("rasters.html");
  });
  
  fastify.get('/api/routes', async (request, reply) => {
      return reply.header('Content-Type', 'application/json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(routes);
  });
  
  fastify.get('/stac-viewer', async (request, reply) => {
      return reply.sendFile("stac_viewer.html");
  });
  
  fastify.get('/cog-viewer', async (request, reply) => {
      return reply.sendFile("cog-viewer.html");
  });
  
  fastify.get('/wikidata-locater', async (request, reply) => {
      // Redirect to /viewer with query parameters preserved
      const queryString = new URLSearchParams(request.query).toString();
      const redirectUrl = queryString ? `/viewer?${queryString}` : '/viewer';
      return reply.redirect(redirectUrl);
  });

  fastify.get('/viewer', async (request, reply) => {
      return reply.sendFile("viewer.html");
  });

  fastify.get('/cors-proxy', async (request, reply) => {
    const { url } = request.query;

    if (!url) {
      return reply.code(400)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: 'URL parameter is required' });
    }

    // Check if URL is in whitelist
    const isAllowed = corsWhitelist.allowedPrefixes.some(prefix => url.startsWith(prefix));
    
    if (!isAllowed) {
      return reply.code(403)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: 'URL not allowed. Must start with an allowed prefix.' });
    }

    try {
      const response = await fetch(url);
      
      if (!response.ok) {
        return reply.code(response.status)
                    .header('Access-Control-Allow-Origin', '*')
                    .send({ error: `Upstream server returned ${response.status}` });
      }

      const contentType = response.headers.get('content-type');
      const buffer = await response.arrayBuffer();

      return reply.header('Content-Type', contentType || 'application/octet-stream')
                  .header('Access-Control-Allow-Origin', '*')
                  .header('Cache-Control', 'max-age=3600')
                  .send(Buffer.from(buffer));
    } catch (err) {
      logger.error({ err }, 'Error proxying request');
      return reply.code(500)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: err.message });
    }
  });

  fastify.get('/cog-info', async (request, reply) => {
    const { url } = request.query;

    if (!url) {
      return reply.code(400)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: 'URL parameter is required' });
    }

    try {
      const info = await cogHandler.getInfo(url);
      
      return reply.header('Content-Type', 'application/json')
                  .header('Cache-Control', 'max-age=86400')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(info);
    } catch (err) {
      logger.error({ err }, 'Error getting COG info');
      
      const statusCode = err.statusCode || 500;
      
      return reply.code(statusCode)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: err.message });
    }
  });

  fastify.get('/cog-tiles/:z/:x/:y', async (request, reply) => {
    const { z, x, y } = request.params;
    const { url, format } = request.query;

    if (!url) {
      return reply.code(400)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: 'URL parameter is required' });
    }

    try {
      const zNum = parseInt(z);
      const xNum = parseInt(x);
      const yNum = parseInt(y);
      
      // Default to png, allow webp
      const outputFormat = (format === 'webp') ? 'webp' : 'png';

      const result = await cogHandler.getTile(url, zNum, xNum, yNum, outputFormat);
      
      if (!result) {
        return reply.code(404)
                    .header('Access-Control-Allow-Origin', '*')
                    .send('');
      }

      const { tile, mimeType } = result;

      return reply.header('Content-Type', mimeType)
                  .header('Cache-Control', 'max-age=86400000')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(tile);
    } catch (err) {
      logger.error({ err }, 'Error processing COG tile');
      
      // Use error's statusCode if it's an HttpError, otherwise default to 500
      const statusCode = err.statusCode || 500;
      
      return reply.code(statusCode)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: err.message });
    }
  });

  if (stacHandler) {
    fastify.get('/stac', async (request, reply) => {
      const landing = await stacHandler.getLandingPage();
      return reply.header('Content-Type', 'application/json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(landing);
    });

    fastify.get('/stac/conformance', async (request, reply) => {
      const conformance = stacHandler.getConformance();
      return reply.header('Content-Type', 'application/json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(conformance);
    });

    fastify.get('/stac/collections', async (request, reply) => {
      const { limit = 100, offset = 0 } = request.query;
      const collections = await stacHandler.getCollections(parseInt(limit), parseInt(offset));
      return reply.header('Content-Type', 'application/json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(collections);
    });

    fastify.get('/stac/collections/:collectionId', async (request, reply) => {
      const { collectionId } = request.params;
      const collection = await stacHandler.getCollection(collectionId);
      if (!collection) {
        return reply.code(404)
                    .header('Access-Control-Allow-Origin', '*')
                    .send({ error: 'Collection not found' });
      }
      return reply.header('Content-Type', 'application/json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(collection);
    });

    fastify.get('/stac/collections/:collectionId/items', async (request, reply) => {
      const { collectionId } = request.params;
      const { limit = 10, offset = 0, bbox } = request.query;
      const items = await stacHandler.getItems(collectionId, parseInt(limit), parseInt(offset), bbox);
      if (!items) {
        return reply.code(404)
                    .header('Access-Control-Allow-Origin', '*')
                    .send({ error: 'Collection not found' });
      }
      return reply.header('Content-Type', 'application/geo+json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(items);
    });

    fastify.get('/stac/collections/:collectionId/items/:itemId', async (request, reply) => {
      const { collectionId, itemId } = request.params;
      const item = await stacHandler.getItem(collectionId, itemId);
      if (!item) {
        return reply.code(404)
                    .header('Access-Control-Allow-Origin', '*')
                    .send({ error: 'Item not found' });
      }
      return reply.header('Content-Type', 'application/geo+json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(item);
    });

    fastify.get('/stac/search', async (request, reply) => {
      const results = await stacHandler.search(request.query);
      return reply.header('Content-Type', 'application/geo+json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(results);
    });

    fastify.post('/stac/search', async (request, reply) => {
      const results = await stacHandler.search(request.body);
      return reply.header('Content-Type', 'application/geo+json')
                  .header('Access-Control-Allow-Origin', '*')
                  .send(results);
    });

    logger.info('STAC API routes added');
  }
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
    if (tileSuffix === 'webp') {
      fastify.get(`${rPrefix}:z/:x/:y.png`, getTilePng.bind(null, handler));
    }
    fastify.get(`${rPrefix}tiles.json`, getTileJson.bind(null, handler));
    fastify.get(`${rPrefix}title`, getTitle.bind(null, handler));
    if (handler.type == 'raster') {
      fastify.get(`${rPrefix}rasterview`, async (request, reply) => {
        return reply.sendFile("raster_view.html");
      });
    } else {
      fastify.get(`${rPrefix}view`, async (request, reply) => {
        // Redirect to /viewer with source in hash parameter
        const hashParams = new URLSearchParams();
        hashParams.set('source', rPrefix);
        
        // Preserve any existing hash parameters from the request
        const existingHash = request.url.includes('#') ? request.url.split('#')[1] : '';
        if (existingHash) {
          const existingHashParams = new URLSearchParams(existingHash);
          for (const [key, value] of existingHashParams) {
            if (key !== 'source') {
              hashParams.set(key, value);
            }
          }
        }
        
        // Preserve query parameters (like markerLat/markerLon)
        const queryString = request.url.includes('?') && !request.url.includes('?#') 
          ? '?' + request.url.split('?')[1].split('#')[0]
          : '';
        
        const redirectUrl = `/viewer${queryString}#${hashParams.toString()}`;
        return reply.redirect(redirectUrl);
      });
    }
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
    var type = 'vector';
    if ('type' in rInfo) {
      type = rInfo['type'];
    }

    var tilesuffix = 'pbf';
    if (type === 'raster') {
      tilesuffix = 'webp';
      if ('tilesuffix' in rInfo) {
        tilesuffix = rInfo['tilesuffix'];
      }
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
    cogHandler = new COGHandler(logger);
    
    createHandlers();
    
    const catalogPath = path.join(__dirname, 'stac_catalog.json');
    if (require('fs').existsSync(catalogPath)) {
      stacHandler = new STACHandler(catalogPath, logger);
      await stacHandler.init();
      logger.info('STAC handler initialized');
    } else {
      logger.warn('No stac_catalog.json found, STAC API will not be available');
    }

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
