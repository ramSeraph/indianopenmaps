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
