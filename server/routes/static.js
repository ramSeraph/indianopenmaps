import path from 'node:path';
import fs from 'node:fs';

export function registerStaticRoutes(app, staticDir) {
  // HTML page routes (without .html extension)
  const htmlPages = [
    ['/', 'index.html'],
    ['/vectors', 'vectors.html'],
    ['/rasters', 'rasters.html'],
    ['/viewer', 'viewer.html'],
    ['/raster-viewer', 'raster_view.html'],
    ['/stac-viewer', 'stac_viewer.html'],
    ['/cog-viewer', 'cog-viewer.html'],
    ['/data-help', 'data-help.html'],
  ];
  
  for (const [route, file] of htmlPages) {
    app.get(route, async (c) => {
      const filePath = path.join(staticDir, file);
      const content = await fs.promises.readFile(filePath, 'utf-8');
      return c.html(content);
    });
  }

  // Redirects
  app.get('/wikidata-locater', (c) => {
    const queryString = new URLSearchParams(c.req.query()).toString();
    const redirectUrl = queryString ? `/viewer?${queryString}` : '/viewer';
    return c.redirect(redirectUrl);
  });
}
