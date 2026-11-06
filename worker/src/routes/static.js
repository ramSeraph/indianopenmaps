import { cacheRevalidationEnabled } from '../lib/http_cache.js';

// Assets uses extensionless HTML paths. Keep public page URLs and legacy aliases
// independent of the files' locations within static/.
const htmlPages = {
  '/': { asset: '/pages/home', aliases: ['/index', '/index.html'] },
  '/vectors': { asset: '/pages/vectors', aliases: ['/vectors.html'] },
  '/rasters': { asset: '/pages/rasters', aliases: ['/rasters.html'] },
  '/viewer': { asset: '/vector/viewer', aliases: ['/viewer.html'] },
  '/raster-viewer': { asset: '/raster/viewer', aliases: ['/raster_view', '/raster_view.html'] },
  '/stac-viewer': { asset: '/stac/viewer', aliases: ['/stac_viewer', '/stac_viewer.html'] },
  '/cog-viewer': { asset: '/cog/viewer', aliases: ['/cog-viewer.html'] },
  '/data-help': { asset: '/pages/data-help', aliases: ['/data-help.html'] },
};

export function registerStaticRoutes(app) {
  // Public browser configuration; Wrangler loads local overrides from .dev.vars.
  app.get('/api/viewer-config.js', (c) => {
    const config = c.env?.PROTOMAPS_API_KEY
      ? { protomapsApiKey: c.env.PROTOMAPS_API_KEY }
      : {};
    config.tileCacheRevalidation = cacheRevalidationEnabled(c.env);
    return c.body(`globalThis.IOMAPS_CONFIG = ${JSON.stringify(config)};`, 200, {
      'Content-Type': 'application/javascript; charset=utf-8',
      'Cache-Control': 'no-store',
    });
  });

  for (const [route, { asset, aliases }] of Object.entries(htmlPages)) {
    for (const path of [route, ...aliases]) {
      app.get(path, (c) => {
        const url = new URL(c.req.url);
        url.pathname = asset;
        return c.env.ASSETS.fetch(new Request(url.toString(), c.req.raw));
      });
    }
  }

  // Redirect /wikidata-locater to /viewer
  app.get('/wikidata-locater', (c) => {
    const queryString = new URLSearchParams(c.req.query()).toString();
    const redirectUrl = queryString ? `/viewer?${queryString}` : '/viewer';
    return c.redirect(redirectUrl);
  });
}
