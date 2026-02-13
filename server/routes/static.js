export function registerStaticRoutes(fastify) {
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
    fastify.get(route, async (request, reply) => {
      return reply.sendFile(file);
    });
  }

  // Redirects
  fastify.get('/wikidata-locater', async (request, reply) => {
    const queryString = new URLSearchParams(request.query).toString();
    const redirectUrl = queryString ? `/viewer?${queryString}` : '/viewer';
    return reply.redirect(redirectUrl);
  });
}
