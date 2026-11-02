import COGHandler from '../lib/cog_handler.js';
import { conditionalResponse, METADATA_CACHE_CONTROL } from '../lib/http_cache.js';
import { cachePolicy } from '../lib/cache_policy.js';

let cogHandler = null;
const errorHeaders = { 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' };

export function registerCogRoutes(app, logger) {
  cogHandler = new COGHandler(logger);

  app.get('/cog-info', async (c) => {
    const url = c.req.query('url');

    if (!url) {
      return c.json({ error: 'URL parameter is required' }, 400, errorHeaders);
    }

    try {
      const policy = cachePolicy(c.env);
      const info = await cogHandler.getInfo(url, { ...policy, signal: c.req.raw.signal });
      
      return conditionalResponse(c.req.raw, JSON.stringify(info), {
        contentType: 'application/json; charset=utf-8', cacheControl: METADATA_CACHE_CONTROL,
        revalidate: policy.revalidate, legacyMaxAge: 86400,
      });
    } catch (err) {
      logger.error({ err }, 'Error getting COG info');
      const statusCode = err.statusCode || 500;
      return c.json({ error: err.message }, statusCode, errorHeaders);
    }
  });

  app.get('/cog-tiles/:z/:x/:y', async (c) => {
    const { z, x, y } = c.req.param();
    const url = c.req.query('url');
    const format = c.req.query('format');

    if (!url) {
      return c.json({ error: 'URL parameter is required' }, 400, errorHeaders);
    }

    try {
      const policy = cachePolicy(c.env);
      const zNum = parseInt(z);
      const xNum = parseInt(x);
      const yNum = parseInt(y);
      
      const outputFormat = (format === 'webp') ? 'webp' : 'png';

      const result = await cogHandler.getTile(url, zNum, xNum, yNum, outputFormat,
        { ...policy, signal: c.req.raw.signal });
      
      if (!result) {
        return c.body('', 404, errorHeaders);
      }

      const { tile, mimeType } = result;

      return conditionalResponse(c.req.raw, tile, {
        contentType: mimeType, cacheControl: policy.tileCacheControl,
        revalidate: policy.revalidate,
      });
    } catch (err) {
      logger.error({ err }, 'Error processing COG tile');
      const statusCode = err.statusCode || 500;
      return c.json({ error: err.message }, statusCode, errorHeaders);
    }
  });
}
