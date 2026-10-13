// HTML page routes (without .html extension)
const htmlPages = {
  '/': 'index.html',
  '/vectors': 'vectors.html',
  '/rasters': 'rasters.html',
  '/viewer': 'viewer.html',
  '/raster-viewer': 'raster_view.html',
  '/stac-viewer': 'stac_viewer.html',
  '/cog-viewer': 'cog-viewer.html',
  '/data-help': 'data-help.html',
};

export function registerStaticRoutes(app) {
  // Public browser configuration; Wrangler loads local overrides from .dev.vars.
  app.get('/api/viewer-config.js', (c) => {
    const config = c.env.PROTOMAPS_API_KEY
      ? { protomapsApiKey: c.env.PROTOMAPS_API_KEY }
      : {};
    return c.body(`globalThis.IOMAPS_CONFIG = ${JSON.stringify(config)};`, 200, {
      'Content-Type': 'application/javascript; charset=utf-8',
      'Cache-Control': 'no-store',
    });
  });

  // HTML page routes - rewrite path to actual file
  for (const [route, file] of Object.entries(htmlPages)) {
    app.get(route, async (c) => {
      const url = new URL(c.req.url);
      url.pathname = '/' + file;
      const request = new Request(url.toString(), c.req.raw);
      return c.env.ASSETS.fetch(request);
    });
  }

  // Redirect /wikidata-locater to /viewer
  app.get('/wikidata-locater', (c) => {
    const queryString = new URLSearchParams(c.req.query()).toString();
    const redirectUrl = queryString ? `/viewer?${queryString}` : '/viewer';
    return c.redirect(redirectUrl);
  });
}
