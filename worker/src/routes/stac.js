import STACHandler from '../lib/stac_handler.js';
import stacCatalog from './stac_catalog.json' with { type: 'json' };

const corsHeaders = { 'Access-Control-Allow-Origin': '*' };

let stacHandler = null;

export async function registerStacRoutes(app, logger) {
  stacHandler = new STACHandler(stacCatalog, logger);
  await stacHandler.init();
  logger.info('STAC handler initialized');

  app.get('/stac', async (c) => {
    const landing = await stacHandler.getLandingPage();
    return c.json(landing, { headers: corsHeaders });
  });

  app.get('/stac/conformance', (c) => {
    const conformance = stacHandler.getConformance();
    return c.json(conformance, { headers: corsHeaders });
  });

  app.get('/stac/collections', async (c) => {
    const limit = parseInt(c.req.query('limit') || '100');
    const offset = parseInt(c.req.query('offset') || '0');
    const collections = await stacHandler.getCollections(limit, offset);
    return c.json(collections, { headers: corsHeaders });
  });

  app.get('/stac/collections/:collectionId', async (c) => {
    const collectionId = c.req.param('collectionId');
    const collection = await stacHandler.getCollection(collectionId);
    if (!collection) {
      return c.json({ error: 'Collection not found' }, 404, corsHeaders);
    }
    return c.json(collection, { headers: corsHeaders });
  });

  app.get('/stac/collections/:collectionId/items', async (c) => {
    const collectionId = c.req.param('collectionId');
    const limit = parseInt(c.req.query('limit') || '10');
    const offset = parseInt(c.req.query('offset') || '0');
    const bbox = c.req.query('bbox');
    const items = await stacHandler.getItems(collectionId, limit, offset, bbox);
    if (!items) {
      return c.json({ error: 'Collection not found' }, 404, corsHeaders);
    }
    return c.json(items, {
      headers: {
        'Content-Type': 'application/geo+json',
        ...corsHeaders
      }
    });
  });

  app.get('/stac/collections/:collectionId/items/:itemId', async (c) => {
    const collectionId = c.req.param('collectionId');
    const itemId = c.req.param('itemId');
    const item = await stacHandler.getItem(collectionId, itemId);
    if (!item) {
      return c.json({ error: 'Item not found' }, 404, corsHeaders);
    }
    return c.json(item, {
      headers: {
        'Content-Type': 'application/geo+json',
        ...corsHeaders
      }
    });
  });

  app.get('/stac/search', async (c) => {
    const results = await stacHandler.search(Object.fromEntries(new URLSearchParams(c.req.url.split('?')[1] || '')));
    return c.json(results, {
      headers: {
        'Content-Type': 'application/geo+json',
        ...corsHeaders
      }
    });
  });

  app.post('/stac/search', async (c) => {
    const body = await c.req.json();
    const results = await stacHandler.search(body);
    return c.json(results, {
      headers: {
        'Content-Type': 'application/geo+json',
        ...corsHeaders
      }
    });
  });

  logger.info('STAC API routes added');
}
