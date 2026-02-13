import corsWhitelist from './cors_whitelist.json' with { type: 'json' };

export function registerCorsProxyRoutes(fastify, logger) {
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

  fastify.get('/cors-proxy', async (request, reply) => {
    const { url } = request.query;
    const validation = validateCorsProxyUrl(url);
    if (validation) {
      return reply.code(validation.status)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: validation.error });
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

  fastify.head('/cors-proxy', async (request, reply) => {
    const { url } = request.query;
    const validation = validateCorsProxyUrl(url);
    if (validation) {
      return reply.code(validation.status)
                  .header('Access-Control-Allow-Origin', '*')
                  .send();
    }

    try {
      const response = await fetch(url, { method: 'HEAD' });
      
      if (!response.ok) {
        return reply.code(response.status)
                    .header('Access-Control-Allow-Origin', '*')
                    .send();
      }

      const contentType = response.headers.get('content-type');
      const contentLength = response.headers.get('content-length');

      reply.header('Access-Control-Allow-Origin', '*')
           .header('Cache-Control', 'max-age=3600');
      
      if (contentType) {
        reply.header('Content-Type', contentType);
      }
      if (contentLength) {
        reply.header('Content-Length', contentLength);
      }

      return reply.send();
    } catch (err) {
      logger.error({ err }, 'Error proxying HEAD request');
      return reply.code(500)
                  .header('Access-Control-Allow-Origin', '*')
                  .send();
    }
  });
}
