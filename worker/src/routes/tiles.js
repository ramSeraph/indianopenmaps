import { PhotonImage } from '@cf-wasm/photon';
import MosaicHandler from '../lib/mosaic_handler.js';
import PMTilesHandler from '../lib/pmtiles_handler.js';

import routes from './listing.json' with { type: 'json' };

const corsHeaders = { 'Access-Control-Allow-Origin': '*' };

const handlerMap = {};

function createHandlers(logger) {
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
      handlerMap[rPrefix] = new MosaicHandler(rInfo['url'], type, tilesuffix, logger, datameetAttribution);
    } else {
      handlerMap[rPrefix] = new PMTilesHandler(rInfo['url'], type, tilesuffix, logger, datameetAttribution);
    }
  });
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
    return c.text('invalid XYZ tile coordinates', 400, corsHeaders);
  }

  const tileSuffix = handler.tileSuffix;
  const validExts = tileSuffix === 'webp' ? ['webp', 'png'] : [tileSuffix];
  if (!validExts.includes(ext)) {
    return c.text('', 404, corsHeaders);
  }

  const [ arr, mimeType ] = await handler.getTile(z,x,y);
  if (!arr) {
    return c.text('', 404, corsHeaders);
  }

  // PNG conversion requested for WebP tiles
  if (ext === 'png' && tileSuffix === 'webp') {
    try {
      const webpBytes = new Uint8Array(arr.data);
      const photonImage = PhotonImage.new_from_byteslice(webpBytes);
      const pngBytes = photonImage.get_bytes(); // Returns PNG by default
      photonImage.free(); // Free WASM memory

      return new Response(pngBytes, {
        headers: {
          'Content-Type': 'image/png',
          'Cache-Control': 'max-age=86400000',
          ...corsHeaders
        }
      });
    } catch (err) {
      console.error('WebP to PNG conversion failed:', err);
      return c.text('Image conversion failed', 500, corsHeaders);
    }
  }

  return new Response(new Uint8Array(arr.data), {
    headers: {
      'Content-Type': mimeType,
      'Cache-Control': 'max-age=86400000',
      ...corsHeaders
    }
  });
}

async function getTileJson(handler, c, serverUrl) {
  const config = await handler.getConfig();
  const tileJsonUrl = c.req.path;
  const baseUrl = tileJsonUrl.replace(/\/tiles\.json.*$/, '');
  config['tiles'] = [ serverUrl + baseUrl + `/{z}/{x}/{y}.${handler.tileSuffix}` ];

  return c.json(config, {
    headers: {
      'Cache-Control': 'max-age=86400000',
      ...corsHeaders
    }
  });
}

function getTileErrorResponse(err) {
  // PMTiles and pmtiles-mosaic expose upstream HTTP status only in these messages.
  const match = /^(?:Bad response code: |Failed to fetch mosaic .+: HTTP )([3-5]\d{2})$/.exec(err?.message ?? '');
  if (!match) return { status: 500, message: 'Internal Server Error' };

  const upstreamStatus = Number(match[1]);
  if (upstreamStatus === 429 || upstreamStatus === 503) {
    return { status: 503, message: 'Service Unavailable', upstreamStatus };
  }
  if (upstreamStatus === 408 || upstreamStatus === 504) {
    return { status: 504, message: 'Gateway Timeout', upstreamStatus };
  }
  // An unavailable/forbidden archive is a gateway failure, not a missing tile
  // or a permissions problem for the client of this Worker.
  return { status: 502, message: 'Bad Gateway', upstreamStatus };
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
        responseStatus: response.status,
      }));
      return c.text(response.message, response.status, {
        ...corsHeaders,
        'Cache-Control': 'no-store',
      });
    }
  };
}

export function registerTileRoutes(app, getServerUrl, logger) {
  createHandlers(logger);

  // API route for listing all tile sources
  app.get('/api/routes', (c) => {
    return c.json(routes, { headers: corsHeaders });
  });

  Object.keys(handlerMap).forEach((rPrefix) => {
    const handler = handlerMap[rPrefix];
    const sourceUrl = routes[rPrefix].url;

    app.get(`${rPrefix}:z/:x/:tile{[0-9]+\\.[a-z]+}`,
      withTileErrorHandling((c) => getTile(handler, c), sourceUrl, logger));
    app.get(`${rPrefix}tiles.json`,
      withTileErrorHandling((c) => getTileJson(handler, c, getServerUrl(c)), sourceUrl, logger));

    if (handler.type == 'raster') {
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
