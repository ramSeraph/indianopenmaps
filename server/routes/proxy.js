import proxyWhitelist from './proxy_whitelist.json' with { type: 'json' };

export function registerProxyRoutes(fastify, logger) {
  function validateProxyUrl(url) {
    if (!url) {
      return { error: 'URL parameter is required', status: 400 };
    }
    const isAllowed = proxyWhitelist.allowedPrefixes.some(prefix => url.startsWith(prefix));
    if (!isAllowed) {
      return { error: 'URL not allowed. Must start with an allowed prefix.', status: 403 };
    }
    return null;
  }

  fastify.get('/proxy', async (request, reply) => {
    const { url } = request.query;
    const validation = validateProxyUrl(url);
    if (validation) {
      return reply.code(validation.status)
                  .send({ error: validation.error });
    }

    try {
      const response = await fetch(url);
      
      if (!response.ok) {
        return reply.code(response.status)
                    .send({ error: `Upstream server returned ${response.status}` });
      }

      const contentType = response.headers.get('content-type');
      const buffer = await response.arrayBuffer();

      return reply.header('Content-Type', contentType || 'application/octet-stream')
                  .header('Cache-Control', 'max-age=3600')
                  .send(Buffer.from(buffer));
    } catch (err) {
      logger.error({ err }, 'Error proxying request');
      return reply.code(500)
                  .send({ error: err.message });
    }
  });

  fastify.head('/proxy', async (request, reply) => {
    const { url } = request.query;
    const validation = validateProxyUrl(url);
    if (validation) {
      return reply.code(validation.status)
                  .send();
    }

    try {
      const response = await fetch(url, { method: 'HEAD' });
      
      if (!response.ok) {
        return reply.code(response.status)
                    .send();
      }

      const contentType = response.headers.get('content-type');
      const contentLength = response.headers.get('content-length');

      reply.header('Cache-Control', 'max-age=3600');
      
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
                  .send();
    }
  });
}
