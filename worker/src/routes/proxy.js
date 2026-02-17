import proxyWhitelist from './proxy_whitelist.json' with { type: 'json' };

// Options to bypass Cloudflare cache for HEAD and Range requests
const noCacheOptions = { cf: { cacheTtl: -1, cacheEverything: false } };

export function registerProxyRoutes(app, logger) {
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

  app.get('/proxy', async (c) => {
    const url = c.req.query('url');
    const validation = validateProxyUrl(url);
    if (validation) {
      return c.json({ error: validation.error }, validation.status);
    }

    try {
      const rangeHeader = c.req.header('Range');
      const fetchOptions = rangeHeader ? { headers: { Range: rangeHeader }, ...noCacheOptions } : {};
      
      const response = await fetch(url, fetchOptions);
      
      if (!response.ok && response.status !== 206) {
        return c.json({ error: `Upstream server returned ${response.status}` }, response.status);
      }

      const responseHeaders = {
        'Content-Type': response.headers.get('content-type') || 'application/octet-stream',
        'Cache-Control': 'max-age=3600',
      };

      // Forward range-related headers
      if (response.status === 206) {
        const contentRange = response.headers.get('content-range');
        if (contentRange) responseHeaders['Content-Range'] = contentRange;
      }
      const contentLength = response.headers.get('content-length');
      if (contentLength) responseHeaders['Content-Length'] = contentLength;
      const acceptRanges = response.headers.get('accept-ranges');
      if (acceptRanges) responseHeaders['Accept-Ranges'] = acceptRanges;

      return new Response(response.body, {
        status: response.status,
        headers: responseHeaders
      });
    } catch (err) {
      logger.error({ err }, 'Error proxying request');
      return c.json({ error: err.message }, 500);
    }
  });

  app.on('HEAD', '/proxy', async (c) => {
    const url = c.req.query('url');
    const validation = validateProxyUrl(url);
    if (validation) {
      return new Response(null, { status: validation.status });
    }

    try {
      const response = await fetch(url, { method: 'HEAD', ...noCacheOptions });
      
      if (!response.ok) {
        return new Response(null, { status: response.status });
      }

      const headers = { 'Cache-Control': 'max-age=3600' };
      const contentType = response.headers.get('content-type');
      const contentLength = response.headers.get('content-length');
      const acceptRanges = response.headers.get('accept-ranges');
      if (contentType) headers['Content-Type'] = contentType;
      if (contentLength) headers['Content-Length'] = contentLength;
      if (acceptRanges) headers['Accept-Ranges'] = acceptRanges;

      return new Response(null, { headers });
    } catch (err) {
      logger.error({ err }, 'Error proxying HEAD request');
      return new Response(null, { status: 500 });
    }
  });
}
