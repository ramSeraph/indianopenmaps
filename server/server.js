import path from 'node:path';
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import pino from 'pino';
import sharp from 'sharp';
import MosaicHandler from './mosaic_handler.js';
import PMTilesHandler from './pmtiles_handler.js';
import STACHandler from './stac_handler.js';
import COGHandler from './cog_handler.js';
import { HttpError, UnknownError } from './errors.js';

import routes from './routes.json' with { type: 'json' };
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

// CORS headers helper
const corsHeaders = { 'Access-Control-Allow-Origin': '*' };

// Helper to extract tile params - use regex pattern :tile{[0-9]+\.[a-z]+}
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

  // Convert webp to png if requested
  if (ext === 'png' && tileSuffix === 'webp') {
    const pngBuffer = await sharp(Buffer.from(arr.data)).png().toBuffer();
    return new Response(pngBuffer, {
      headers: {
        'Content-Type': 'image/png',
        'Cache-Control': 'max-age=86400000',
        ...corsHeaders
      }
    });
  }

  return new Response(new Uint8Array(arr.data), {
    headers: {
      'Content-Type': mimeType,
      'Cache-Control': 'max-age=86400000',
      ...corsHeaders
    }
  });
}

async function getTileJson(handler, c) {
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

function addRoutes() {
  logger.info('adding routes');

  // HTML page routes (without .html extension)
  const htmlPages = [
    ['/', 'index.html'],
    ['/vectors', 'vectors.html'],
    ['/rasters', 'rasters.html'],
    ['/viewer', 'viewer.html'],
    ['/stac-viewer', 'stac_viewer.html'],
    ['/cog-viewer', 'cog-viewer.html'],
    ['/data-help', 'data-help.html'],
  ];
  
  for (const [route, file] of htmlPages) {
    app.get(route, async (c) => {
      const filePath = path.join(__dirname, '..', 'static', file);
      const content = await fs.promises.readFile(filePath, 'utf-8');
      return c.html(content);
    });
  }

  // API routes
  app.get('/api/routes', (c) => {
    return c.json(routes, { headers: corsHeaders });
  });

  // Redirects
  app.get('/wikidata-locater', (c) => {
    const queryString = new URLSearchParams(c.req.query()).toString();
    const redirectUrl = queryString ? `/viewer?${queryString}` : '/viewer';
    return c.redirect(redirectUrl);
  });

  // CORS proxy validation
  function validateCorsProxyUrl(url) {
    if (!url) {
      return { error: 'URL parameter is required', status: 400 };
    }
    const isAllowed = corsWhitelist.allowedPrefixes.some(prefix => url.startsWith(prefix));
    if (!isAllowed) {
      return { error: 'URL not allowed. Must start with an allowed prefix.', status: 403 };
    }
    return null;
  }

  app.get('/cors-proxy', async (c) => {
    const url = c.req.query('url');
    const validation = validateCorsProxyUrl(url);
    if (validation) {
      return c.json({ error: validation.error }, validation.status, corsHeaders);
    }

    try {
      const response = await fetch(url);
      
      if (!response.ok) {
        return c.json({ error: `Upstream server returned ${response.status}` }, response.status, corsHeaders);
      }

      const contentType = response.headers.get('content-type');
      const buffer = await response.arrayBuffer();

      return new Response(Buffer.from(buffer), {
        headers: {
          'Content-Type': contentType || 'application/octet-stream',
          'Cache-Control': 'max-age=3600',
          ...corsHeaders
        }
      });
    } catch (err) {
      logger.error({ err }, 'Error proxying request');
      return c.json({ error: err.message }, 500, corsHeaders);
    }
  });

  app.on('HEAD', '/cors-proxy', async (c) => {
    const url = c.req.query('url');
    const validation = validateCorsProxyUrl(url);
    if (validation) {
      return new Response(null, { status: validation.status, headers: corsHeaders });
    }

    try {
      const response = await fetch(url, { method: 'HEAD' });
      
      if (!response.ok) {
        return new Response(null, { status: response.status, headers: corsHeaders });
      }

      const headers = { ...corsHeaders, 'Cache-Control': 'max-age=3600' };
      const contentType = response.headers.get('content-type');
      const contentLength = response.headers.get('content-length');
      if (contentType) headers['Content-Type'] = contentType;
      if (contentLength) headers['Content-Length'] = contentLength;

      return new Response(null, { headers });
    } catch (err) {
      logger.error({ err }, 'Error proxying HEAD request');
      return new Response(null, { status: 500, headers: corsHeaders });
    }
  });

  app.get('/cog-info', async (c) => {
    const url = c.req.query('url');

    if (!url) {
      return c.json({ error: 'URL parameter is required' }, 400, corsHeaders);
    }

    try {
      const info = await cogHandler.getInfo(url);
      
      return c.json(info, {
        headers: {
          'Cache-Control': 'max-age=86400',
          ...corsHeaders
        }
      });
    } catch (err) {
      logger.error({ err }, 'Error getting COG info');
      const statusCode = err.statusCode || 500;
      return c.json({ error: err.message }, statusCode, corsHeaders);
    }
  });

  app.get('/cog-tiles/:z/:x/:y', async (c) => {
    const { z, x, y } = c.req.param();
    const url = c.req.query('url');
    const format = c.req.query('format');

    if (!url) {
      return c.json({ error: 'URL parameter is required' }, 400, corsHeaders);
    }

    try {
      const zNum = parseInt(z);
      const xNum = parseInt(x);
      const yNum = parseInt(y);
      
      const outputFormat = (format === 'webp') ? 'webp' : 'png';

      const result = await cogHandler.getTile(url, zNum, xNum, yNum, outputFormat);
      
      if (!result) {
        return c.text('', 404, corsHeaders);
      }

      const { tile, mimeType } = result;

      return new Response(tile, {
        headers: {
          'Content-Type': mimeType,
          'Cache-Control': 'max-age=86400000',
          ...corsHeaders
        }
      });
    } catch (err) {
      logger.error({ err }, 'Error processing COG tile');
      const statusCode = err.statusCode || 500;
      return c.json({ error: err.message }, statusCode, corsHeaders);
    }
  });

  if (stacHandler) {
    app.get('/stac', async (c) => {
      const landing = await stacHandler.getLandingPage();
      return c.json(landing, { headers: corsHeaders });
    });

    app.get('/stac/conformance', (c) => {
      const conformance = stacHandler.getConformance();
      return c.json(conformance, { headers: corsHeaders });
    });

    app.get('/stac/collections', async (c) => {
      const limit = parseInt(c.req.query('limit') || '100');
      const offset = parseInt(c.req.query('offset') || '0');
      const collections = await stacHandler.getCollections(limit, offset);
      return c.json(collections, { headers: corsHeaders });
    });

    app.get('/stac/collections/:collectionId', async (c) => {
      const collectionId = c.req.param('collectionId');
      const collection = await stacHandler.getCollection(collectionId);
      if (!collection) {
        return c.json({ error: 'Collection not found' }, 404, corsHeaders);
      }
      return c.json(collection, { headers: corsHeaders });
    });

    app.get('/stac/collections/:collectionId/items', async (c) => {
      const collectionId = c.req.param('collectionId');
      const limit = parseInt(c.req.query('limit') || '10');
      const offset = parseInt(c.req.query('offset') || '0');
      const bbox = c.req.query('bbox');
      const items = await stacHandler.getItems(collectionId, limit, offset, bbox);
      if (!items) {
        return c.json({ error: 'Collection not found' }, 404, corsHeaders);
      }
      return c.json(items, {
        headers: {
          'Content-Type': 'application/geo+json',
          ...corsHeaders
        }
      });
    });

    app.get('/stac/collections/:collectionId/items/:itemId', async (c) => {
      const collectionId = c.req.param('collectionId');
      const itemId = c.req.param('itemId');
      const item = await stacHandler.getItem(collectionId, itemId);
      if (!item) {
        return c.json({ error: 'Item not found' }, 404, corsHeaders);
      }
      return c.json(item, {
        headers: {
          'Content-Type': 'application/geo+json',
          ...corsHeaders
        }
      });
    });

    app.get('/stac/search', async (c) => {
      const results = await stacHandler.search(Object.fromEntries(new URLSearchParams(c.req.url.split('?')[1] || '')));
      return c.json(results, {
        headers: {
          'Content-Type': 'application/geo+json',
          ...corsHeaders
        }
      });
    });

    app.post('/stac/search', async (c) => {
      const body = await c.req.json();
      const results = await stacHandler.search(body);
      return c.json(results, {
        headers: {
          'Content-Type': 'application/geo+json',
          ...corsHeaders
        }
      });
    });

    logger.info('STAC API routes added');
  }

  // Dynamic tile routes from routes.json
  Object.keys(handlerMap).forEach((rPrefix) => {
    const handler = handlerMap[rPrefix];

    // Single route for all tile extensions - validation inside handler
    app.get(`${rPrefix}:z/:x/:tile{[0-9]+\\.[a-z]+}`, (c) => getTile(handler, c));

    app.get(`${rPrefix}tiles.json`, (c) => getTileJson(handler, c));

    if (handler.type == 'raster') {
      app.get(`${rPrefix}rasterview`, async (c) => {
        const filePath = path.join(__dirname, '..', 'static', 'raster_view.html');
        const content = await fs.promises.readFile(filePath, 'utf-8');
        return c.html(content);
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

  // Static file serving - must be last
  app.use('/*', serveStatic({ root: './static' }));

  logger.info('done adding routes');
}

function createHandlers() {
  Object.keys(routes).forEach((rPrefix) => {
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
  });
}

async function start() {
  try {
    cogHandler = new COGHandler(logger);
    
    createHandlers();
    
    const catalogPath = path.join(__dirname, 'stac_catalog.json');
    if (fs.existsSync(catalogPath)) {
      stacHandler = new STACHandler(catalogPath, logger);
      await stacHandler.init();
      logger.info('STAC handler initialized');
    } else {
      logger.warn('No stac_catalog.json found, STAC API will not be available');
    }

    addRoutes();
    
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
