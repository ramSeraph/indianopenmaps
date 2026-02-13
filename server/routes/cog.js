import COGHandler from '../cog_handler.js';

const corsHeaders = { 'Access-Control-Allow-Origin': '*' };

let cogHandler = null;

export function registerCogRoutes(app, logger) {
  cogHandler = new COGHandler(logger);

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
}
