import { cors } from 'hono/cors';
import STACHandler from '../lib/stac_handler.js';
import { stacOpenAPI } from '../lib/stac_openapi.js';
import { BadRequestError, NotFoundError } from '../lib/errors.js';
import stacCatalog from './stac_catalog.json' with { type: 'json' };
import { cachePolicy, DEFAULT_SOURCE_REFRESH_MS } from '../lib/cache_policy.js';

export async function registerStacRoutes(app, logger, catalog = stacCatalog) {
  const handler = new STACHandler(catalog, logger);
  await handler.init();
  const handlers = new Map([[DEFAULT_SOURCE_REFRESH_MS, handler]]);
  const getHandler = c => {
    const { refreshMs } = cachePolicy(c.env);
    if (!handlers.has(refreshMs)) {
      const configured = new STACHandler(catalog, logger, { refreshMs });
      configured.init();
      handlers.set(refreshMs, configured);
    }
    return handlers.get(refreshMs);
  };
  const corsMiddleware = cors({ origin: '*', allowMethods: ['GET', 'POST', 'OPTIONS'], allowHeaders: ['Content-Type'] });
  app.use('/stac', corsMiddleware);
  app.use('/stac/*', corsMiddleware);
  const base = c => `${new URL(c.req.url).origin}/stac`;
  const respond = (action, geojson = false) => async c => {
    try {
      const data = await action(c, getHandler(c));
      if (data === null) throw new NotFoundError('Collection or item not found');
      c.header('Content-Type', geojson ? 'application/geo+json' : 'application/json');
      return c.body(JSON.stringify(data));
    } catch (error) {
      if (!error.statusCode) logger.error(error);
      c.header('Cache-Control', 'no-store');
      return c.json({ code: error.name || 'Error', description: error.statusCode ? error.message : 'STAC request failed' }, error.statusCode || 500);
    }
  };
  app.get('/stac', respond((c, handler) => handler.getLandingPage(base(c))));
  app.get('/stac/', respond((c, handler) => handler.getLandingPage(base(c))));
  app.get('/stac/conformance', respond((c, handler) => handler.getConformance()));
  app.get('/stac/api.json', c => c.body(JSON.stringify(stacOpenAPI(base(c))), 200,
    { 'Content-Type': 'application/vnd.oai.openapi+json;version=3.0' }));
  app.get('/stac/api.html', c => c.html(`<!doctype html><html lang="en"><meta charset="utf-8"><title>Indian Open Maps STAC API</title>
    <h1>Indian Open Maps STAC API</h1><p>Discover raster collections and download individual map sheets.</p>
    <p><a href="/stac/api.json">OpenAPI document</a> · <a href="/stac/collections">Collections</a> · <a href="/stac-viewer">Map viewer</a></p>
    <p>Search with GET or POST /stac/search. Supported parameters: collections, ids, bbox, intersects, datetime and limit.
    GET uses comma-separated arrays; POST uses JSON arrays. Follow the next link for further pages.</p>
    <h2>Search directly on GitHub</h2>
    <p>The published indexes are STAC GeoParquet. Clients such as iomaps and Rustac can search these files locally,
    then read raster assets from GitHub releases, without calling this HTTP API.</p>
    <p>Find an index in a collection's <code>assets.index.href</code>, or use the <code>geoparquet</code> links in the
    <a href="https://raw.githubusercontent.com/ramSeraph/indianopenmaps/main/worker/src/routes/stac_catalog.json">raw GitHub catalog</a>.</p>
    <pre>uvx --from rustac==0.9.19 rustac search INDEX_URL matches.json --bbox 77.6,13.0,77.61,13.01</pre>
    <p>Replace <code>INDEX_URL</code> with the release Parquet URL. This searches metadata; use iomaps or GDAL to extract pixels.
    A Parquet URL is not an API root for clients such as PySTAC Client. The current indexes are also distinct from a linked static STAC JSON catalog.</p>
    <h2>Browser access</h2>
    <p>STAC metadata and the <code>/cog-info</code> and <code>/cog-tiles/...</code> endpoints allow any browser origin.
    Asset links retain their direct GitHub URLs; each file server controls CORS for its own responses.
    Rendered tiles are distinct from raw TIFF bytes. Successful raw-file <code>/proxy</code> responses omit CORS headers,
    so another website's JavaScript cannot read them. This is a browser restriction, not authentication.</p>
    <p><a href="https://github.com/ramSeraph/indianopenmaps/blob/main/python/README.md#search-release-indexes-with-other-stac-clients">Complete tested example and client comparison</a></p></html>`));
  app.get('/stac/collections', respond((c, handler) => handler.getCollections(c.req.query(), base(c))));
  app.get('/stac/collections/:collectionId', respond((c, handler) => handler.getCollection(c.req.param('collectionId'), base(c))));
  app.get('/stac/collections/:collectionId/items', respond((c, handler) => handler.getItems(c.req.param('collectionId'), c.req.query(), base(c)), true));
  app.get('/stac/collections/:collectionId/items/:itemId', respond((c, handler) => handler.getItem(c.req.param('collectionId'), c.req.param('itemId'), base(c)), true));
  app.get('/stac/search', respond((c, handler) => handler.search(c.req.query(), base(c)), true));
  app.post('/stac/search', respond(async (c, handler) => {
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
