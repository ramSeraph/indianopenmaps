import COGHandler from '../cog_handler.js';

let cogHandler = null;

export function registerCogRoutes(fastify, logger) {
  cogHandler = new COGHandler(logger);

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
      const statusCode = err.statusCode || 500;
      return reply.code(statusCode)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: err.message });
    }
  });
}
