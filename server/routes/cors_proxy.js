import corsWhitelist from './cors_whitelist.json' with { type: 'json' };

const corsHeaders = { 'Access-Control-Allow-Origin': '*' };

export function registerCorsProxyRoutes(app, logger) {
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
}
