import { PhotonImage } from '@cf-wasm/photon';
import MosaicHandler from '../mosaic_handler.js';
import PMTilesHandler from '../pmtiles_handler.js';

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
  const z = parseInt(params.z);
  const x = parseInt(params.x);
  const [yStr, ext] = (params.tile || '').split('.');
  const y = parseInt(yStr);
  return { z, x, y, ext };
}

async function getTile(handler, c) {
  const { z, x, y, ext } = getTileParams(c);
  if (isNaN(z) || isNaN(x) || isNaN(y)) {
    return c.text('non integer values in tile url', 400, corsHeaders);
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

export function registerTileRoutes(app, getServerUrl, logger) {
  createHandlers(logger);

  // API route for listing all tile sources
  app.get('/api/routes', (c) => {
    return c.json(routes, { headers: corsHeaders });
  });

  Object.keys(handlerMap).forEach((rPrefix) => {
    const handler = handlerMap[rPrefix];

    app.get(`${rPrefix}:z/:x/:tile{[0-9]+\\.[a-z]+}`, (c) => getTile(handler, c));
    app.get(`${rPrefix}tiles.json`, (c) => getTileJson(handler, c, getServerUrl(c)));

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
