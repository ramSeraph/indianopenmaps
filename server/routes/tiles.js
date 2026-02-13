import sharp from 'sharp';
import MosaicHandler from '../mosaic_handler.js';
import PMTilesHandler from '../pmtiles_handler.js';

import routes from './listing.json' with { type: 'json' };

const handlerMap = {};

function createHandlers(logger) {
  Object.keys(routes).forEach((rPrefix) => {
    const rInfo = routes[rPrefix];
    let datameetAttribution = true;
    if ('datameet_attribution' in rInfo) {
      datameetAttribution = rInfo['datameet_attribution'];
    }
    let type = 'vector';
    if ('type' in rInfo) {
      type = rInfo['type'];
    }

    let tilesuffix = 'pbf';
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
  });
}

async function getTile(handler, request, reply) {
  let { z, x, y } = request.params;
  try {
    z = parseInt(z);
    x = parseInt(x);
    y = parseInt(y);
  } catch(err) {
    return reply.code(400)
                .header('Access-Control-Allow-Origin', '*')
                .send('non integer values in tile url');
  }

  const [ arr, mimeType ] = await handler.getTile(z, x, y);
  if (arr) {
    return reply.header('Content-Type', mimeType)
                .header('Cache-Control', 'max-age=86400000')
                .header('Access-Control-Allow-Origin', '*')
                .send(Buffer.from(arr.data));
  }
  return reply.code(404)
              .header('Access-Control-Allow-Origin', '*')
              .send('');
}

async function getTilePng(handler, request, reply) {
  let { z, x, y } = request.params;
  try {
    z = parseInt(z);
    x = parseInt(x);
    y = parseInt(y);
  } catch(err) {
    return reply.code(400)
                .header('Access-Control-Allow-Origin', '*')
                .send('non integer values in tile url');
  }

  const [ arr, mimeType ] = await handler.getTile(z, x, y);
  if (arr) {
    const webpBuffer = Buffer.from(arr.data);
    const pngBuffer = await sharp(webpBuffer).png().toBuffer();
    
    return reply.header('Content-Type', 'image/png')
                .header('Cache-Control', 'max-age=86400000')
                .header('Access-Control-Allow-Origin', '*')
                .send(pngBuffer);
  }
  return reply.code(404)
              .header('Access-Control-Allow-Origin', '*')
              .send('');
}

async function getTileJson(handler, request, reply, serverUrl) {
  const config = await handler.getConfig();
  const tileJsonUrl = request.url;
  const baseUrl = tileJsonUrl.replace(/\/tiles\.json.*$/, '');
  config['tiles'] = [ serverUrl + baseUrl + `/{z}/{x}/{y}.${handler.tileSuffix}` ];

  return reply.header('Content-Type', 'application/json')
              .header('Cache-Control', 'max-age=86400000')
              .header('Access-Control-Allow-Origin', '*')
              .send(config);
}

export function registerTileRoutes(fastify, serverUrl, logger) {
  createHandlers(logger);

  // API route for listing all tile sources
  fastify.get('/api/routes', async (request, reply) => {
    return reply.header('Content-Type', 'application/json')
                .header('Access-Control-Allow-Origin', '*')
                .send(routes);
  });

  Object.keys(handlerMap).forEach((rPrefix) => {
    const handler = handlerMap[rPrefix];
    const tileSuffix = handler.tileSuffix;

    fastify.get(`${rPrefix}:z/:x/:y.${tileSuffix}`, (request, reply) => getTile(handler, request, reply));

    if (tileSuffix === 'webp') {
      fastify.get(`${rPrefix}:z/:x/:y.png`, (request, reply) => getTilePng(handler, request, reply));
    }

    fastify.get(`${rPrefix}tiles.json`, (request, reply) => getTileJson(handler, request, reply, serverUrl));

    if (handler.type == 'raster') {
      fastify.get(`${rPrefix}rasterview`, async (request, reply) => {
        const hashParams = new URLSearchParams();
        hashParams.set('left', rPrefix);
        
        let queryString = new URLSearchParams(request.query).toString();
        if (queryString) {
          queryString = '?' + queryString;
        }

        const redirectUrl = `/raster-viewer${queryString}#${hashParams.toString()}`;
        return reply.redirect(redirectUrl);
      });
    } else {
      fastify.get(`${rPrefix}view`, async (request, reply) => {
        const hashParams = new URLSearchParams();
        hashParams.set('source', rPrefix);
        
        let queryString = new URLSearchParams(request.query).toString();
        if (queryString) {
          queryString = '?' + queryString;
        }

        const redirectUrl = `/viewer${queryString}#${hashParams.toString()}`;
        return reply.redirect(redirectUrl);
      });
    }
  });
}
