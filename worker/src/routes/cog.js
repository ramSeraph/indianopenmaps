import COGHandler from '../lib/cog_handler.js';

let cogHandler = null;

export function registerCogRoutes(app, logger) {
  cogHandler = new COGHandler(logger);

  app.get('/cog-info', async (c) => {
    const url = c.req.query('url');

    if (!url) {
      return c.json({ error: 'URL parameter is required' }, 400, {
        'Access-Control-Allow-Origin': '*'
      });
    }

    try {
      const info = await cogHandler.getInfo(url);
      
      return c.json(info, 200, {
        'Cache-Control': 'max-age=86400',
        'Access-Control-Allow-Origin': '*'
      });
    } catch (err) {
      logger.error({ err }, 'Error getting COG info');
      const statusCode = err.statusCode || 500;
      return c.json({ error: err.message }, statusCode, {
        'Access-Control-Allow-Origin': '*'
      });
    }
  });

  app.get('/cog-tiles/:z/:x/:y', async (c) => {
    const { z, x, y } = c.req.param();
    const url = c.req.query('url');
    const format = c.req.query('format');

    if (!url) {
      return c.json({ error: 'URL parameter is required' }, 400, {
        'Access-Control-Allow-Origin': '*'
      });
    }

    try {
      const zNum = parseInt(z);
      const xNum = parseInt(x);
      const yNum = parseInt(y);
      
      const outputFormat = (format === 'webp') ? 'webp' : 'png';

      const result = await cogHandler.getTile(url, zNum, xNum, yNum, outputFormat);
      
      if (!result) {
        return c.body('', 404, {
          'Access-Control-Allow-Origin': '*'
        });
      }

      const { tile, mimeType } = result;

      return c.body(tile, 200, {
        'Content-Type': mimeType,
        'Cache-Control': 'max-age=86400000',
        'Access-Control-Allow-Origin': '*'
      });
    } catch (err) {
      logger.error({ err }, 'Error processing COG tile');
      const statusCode = err.statusCode || 500;
      return c.json({ error: err.message }, statusCode, {
        'Access-Control-Allow-Origin': '*'
      });
    }
  });
}
