import { cors } from 'hono/cors';
import STACHandler from '../lib/stac_handler.js';
import { stacOpenAPI } from '../lib/stac_openapi.js';
import { BadRequestError, NotFoundError } from '../lib/errors.js';
import stacCatalog from './stac_catalog.json' with { type: 'json' };

export async function registerStacRoutes(app, logger, catalog = stacCatalog) {
  const handler = new STACHandler(catalog, logger);
  await handler.init();
  const corsMiddleware = cors({ origin: '*', allowMethods: ['GET', 'POST', 'OPTIONS'], allowHeaders: ['Content-Type'] });
  app.use('/stac', corsMiddleware);
  app.use('/stac/*', corsMiddleware);
  const base = c => `${new URL(c.req.url).origin}/stac`;
  const respond = (action, geojson = false) => async c => {
    try {
      const data = await action(c);
      if (data === null) throw new NotFoundError('Collection or item not found');
      c.header('Content-Type', geojson ? 'application/geo+json' : 'application/json');
      return c.body(JSON.stringify(data));
    } catch (error) {
      if (!error.statusCode) logger.error(error);
      c.header('Cache-Control', 'no-store');
      return c.json({ code: error.name || 'Error', description: error.statusCode ? error.message : 'STAC request failed' }, error.statusCode || 500);
    }
  };
  app.get('/stac', respond(c => handler.getLandingPage(base(c))));
  app.get('/stac/', respond(c => handler.getLandingPage(base(c))));
  app.get('/stac/conformance', respond(() => handler.getConformance()));
  app.get('/stac/api.json', c => c.body(JSON.stringify(stacOpenAPI(base(c))), 200,
    { 'Content-Type': 'application/vnd.oai.openapi+json;version=3.0' }));
  app.get('/stac/api.html', c => c.html(`<!doctype html><html lang="en"><meta charset="utf-8"><title>Indian Open Maps STAC API</title>
    <h1>Indian Open Maps STAC API</h1><p>Discover raster collections and download individual map sheets.</p>
    <p><a href="/stac/api.json">OpenAPI document</a> · <a href="/stac/collections">Collections</a> · <a href="/stac-viewer">Map viewer</a></p>
    <p>Search with GET or POST /stac/search. Supported parameters: collections, ids, bbox, intersects, datetime and limit.
    GET uses comma-separated arrays; POST uses JSON arrays. Follow the next link for further pages.</p></html>`));
  app.get('/stac/collections', respond(c => handler.getCollections(c.req.query(), base(c))));
  app.get('/stac/collections/:collectionId', respond(c => handler.getCollection(c.req.param('collectionId'), base(c))));
  app.get('/stac/collections/:collectionId/items', respond(c => handler.getItems(c.req.param('collectionId'), c.req.query(), base(c)), true));
  app.get('/stac/collections/:collectionId/items/:itemId', respond(c => handler.getItem(c.req.param('collectionId'), c.req.param('itemId'), base(c)), true));
  app.get('/stac/search', respond(c => handler.search(c.req.query(), base(c)), true));
  app.post('/stac/search', respond(async c => {
    let body;
    try { body = await c.req.json(); } catch { throw new BadRequestError('Expected a JSON search object'); }
    for (const key of ['collections', 'ids', 'bbox']) {
      if (body?.[key] !== undefined && !Array.isArray(body[key])) throw new BadRequestError(`${key} must be a JSON array`);
    }
    for (const key of ['limit', 'offset']) {
      if (body?.[key] !== undefined && typeof body[key] !== 'number') throw new BadRequestError(`${key} must be a JSON number`);
    }
    if (typeof body?.intersects === 'string') throw new BadRequestError('intersects must be a GeoJSON object');
    return handler.search(body, base(c), 'POST');
  }, true));
  return handler;
}
