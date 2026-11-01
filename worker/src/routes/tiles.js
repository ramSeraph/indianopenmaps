import { PhotonImage } from '@cf-wasm/photon';
import MosaicHandler from '../lib/mosaic_handler.js';
import PMTilesHandler from '../lib/pmtiles_handler.js';
import { UpstreamHttpError } from '../lib/errors.js';
import { conditionalResponse, cacheRevalidationEnabled, TILE_CACHE_CONTROL, METADATA_CACHE_CONTROL } from '../lib/http_cache.js';

import routes from './listing.json' with { type: 'json' };

const corsHeaders = { 'Access-Control-Allow-Origin': '*' };
const errorHeaders = { ...corsHeaders, 'Cache-Control': 'no-store' };

function createHandlers(logger, revalidate) {
  const handlerMap = {};
  Object.keys(routes).forEach((rPrefix) => {
    const rInfo = routes[rPrefix];
    const datameetAttribution = !('license' in rInfo);
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
      handlerMap[rPrefix] = new MosaicHandler(rInfo['url'], type, tilesuffix, logger, datameetAttribution, { revalidate });
    } else {
      handlerMap[rPrefix] = new PMTilesHandler(rInfo['url'], type, tilesuffix, logger, datameetAttribution, { revalidate });
    }
  });
  return handlerMap;
}

function getTileParams(c) {
  const params = c.req.param();
  const z = Number(params.z);
  const x = Number(params.x);
  const [yStr, ext] = (params.tile || '').split('.');
  const y = Number(yStr);
  return { z, x, y, ext };
}

async function getTile(handler, c) {
  const { z, x, y, ext } = getTileParams(c);
  if (!Number.isInteger(z) || z < 0 || z > 26 ||
      !Number.isInteger(x) || !Number.isInteger(y) ||
      x < 0 || y < 0 || x >= 2 ** z || y >= 2 ** z) {
    return c.text('invalid XYZ tile coordinates', 400, errorHeaders);
  }

  const tileSuffix = handler.tileSuffix;
  const validExts = tileSuffix === 'webp' ? ['webp', 'png'] : [tileSuffix];
  if (!validExts.includes(ext)) {
    return c.text('', 404, errorHeaders);
  }

  const [ arr, mimeType ] = await handler.getTile(z,x,y);
  if (!arr) {
    return c.text('', 404, errorHeaders);
  }

  // PNG conversion requested for WebP tiles
  if (ext === 'png' && tileSuffix === 'webp') {
    try {
      const webpBytes = new Uint8Array(arr.data);
      const photonImage = PhotonImage.new_from_byteslice(webpBytes);
      const pngBytes = photonImage.get_bytes(); // Returns PNG by default
      photonImage.free(); // Free WASM memory

      return conditionalResponse(c.req.raw, pngBytes, {
        contentType: 'image/png', cacheControl: TILE_CACHE_CONTROL, revalidate: handler.revalidate,
      });
    } catch (err) {
      console.error('WebP to PNG conversion failed:', err);
      return c.text('Image conversion failed', 500, errorHeaders);
    }
  }

  return conditionalResponse(c.req.raw, new Uint8Array(arr.data), {
    contentType: mimeType, cacheControl: TILE_CACHE_CONTROL, revalidate: handler.revalidate,
  });
}

async function getTileJson(handler, c, serverUrl) {
  const config = await handler.getConfig();
  const tileJsonUrl = c.req.path;
  const baseUrl = tileJsonUrl.replace(/\/tiles\.json.*$/, '');
  config['tiles'] = [ serverUrl + baseUrl + `/{z}/{x}/{y}.${handler.tileSuffix}` ];

  return conditionalResponse(c.req.raw, JSON.stringify(config), {
    contentType: 'application/json; charset=utf-8', cacheControl: METADATA_CACHE_CONTROL, revalidate: handler.revalidate,
  });
}

function getTileErrorResponse(err) {
  if (!(err instanceof UpstreamHttpError)) return { status: 500, message: 'Internal Server Error' };
  const messages = { 502: 'Bad Gateway', 503: 'Service Unavailable', 504: 'Gateway Timeout' };
  return { status: err.statusCode, message: messages[err.statusCode], upstreamStatus: err.upstreamStatus };
}

function withTileErrorHandling(serve, sourceUrl, logger) {
  return async (c) => {
    try {
      return await serve(c);
    } catch (err) {
      const response = getTileErrorResponse(err);
      // Workers Logs can retain only stack frames when passed a native Error.
      // Serialize explicitly so the upstream status and context survive together.
      logger.error(JSON.stringify({
        message: `Tile source request failed: ${err?.message ?? String(err)}`,
        name: err?.name,
        stack: err?.stack,
        method: c.req.method,
        path: c.req.path,
        sourceUrl,
        upstreamStatus: response.upstreamStatus,
        retryAfter: err?.retryAfter,
        responseStatus: response.status,
      }));
      return c.text(response.message, response.status, {
        ...corsHeaders,
        'Cache-Control': 'no-store',
        ...(err?.retryAfter ? { 'Retry-After': err.retryAfter, 'Access-Control-Expose-Headers': 'Retry-After' } : {}),
      });
    }
  };
}

export function registerTileRoutes(app, getServerUrl, logger) {
  const handlerMaps = new Map();
  const getHandler = (c, prefix) => {
    const enabled = cacheRevalidationEnabled(c.env);
    if (!handlerMaps.has(enabled)) handlerMaps.set(enabled, createHandlers(logger, enabled));
    return handlerMaps.get(enabled)[prefix];
  };

  // API route for listing all tile sources
  app.get('/api/routes', (c) => {
    return c.json(routes, { headers: corsHeaders });
  });

  Object.keys(routes).forEach((rPrefix) => {
    const sourceUrl = routes[rPrefix].url;

    app.get(`${rPrefix}:z/:x/:tile{[0-9]+\\.[a-z]+}`,
      withTileErrorHandling((c) => getTile(getHandler(c, rPrefix), c), sourceUrl, logger));
    app.get(`${rPrefix}tiles.json`,
      withTileErrorHandling((c) => getTileJson(getHandler(c, rPrefix), c, getServerUrl(c)), sourceUrl, logger));

    if (routes[rPrefix].type == 'raster') {
      app.get(`${rPrefix}rasterview`, (c) => {
        const hashParams = new URLSearchParams();
        hashParams.set('left', rPrefix);
        
        let queryString = new URLSearchParams(c.req.query()).toString();
        if (queryString) {
          queryString = '?' + queryString;
        }

        const redirectUrl = `/raster-viewer${queryString}#${hashParams.toString()}`;
        return c.redirect(redirectUrl);
      });
    } else {
      app.get(`${rPrefix}view`, (c) => {
        const hashParams = new URLSearchParams();
        hashParams.set('source', rPrefix);
        
        let queryString = new URLSearchParams(c.req.query()).toString();
        if (queryString) {
          queryString = '?' + queryString;
        }

        const redirectUrl = `/viewer${queryString}#${hashParams.toString()}`;
        return c.redirect(redirectUrl);
      });
    }
  });
}
