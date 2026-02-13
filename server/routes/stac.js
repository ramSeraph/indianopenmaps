import STACHandler from '../stac_handler.js';
import stacCatalog from './stac_catalog.json' with { type: 'json' };

let stacHandler = null;

export async function registerStacRoutes(fastify, logger) {
  stacHandler = new STACHandler(stacCatalog, logger);
  await stacHandler.init();
  logger.info('STAC handler initialized');

  fastify.get('/stac', async (request, reply) => {
    const landing = await stacHandler.getLandingPage();
    return reply.header('Content-Type', 'application/json')
                .header('Access-Control-Allow-Origin', '*')
                .send(landing);
  });

  fastify.get('/stac/conformance', async (request, reply) => {
    const conformance = stacHandler.getConformance();
    return reply.header('Content-Type', 'application/json')
                .header('Access-Control-Allow-Origin', '*')
                .send(conformance);
  });

  fastify.get('/stac/collections', async (request, reply) => {
    const { limit = 100, offset = 0 } = request.query;
    const collections = await stacHandler.getCollections(parseInt(limit), parseInt(offset));
    return reply.header('Content-Type', 'application/json')
                .header('Access-Control-Allow-Origin', '*')
                .send(collections);
  });

  fastify.get('/stac/collections/:collectionId', async (request, reply) => {
    const { collectionId } = request.params;
    const collection = await stacHandler.getCollection(collectionId);
    if (!collection) {
      return reply.code(404)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: 'Collection not found' });
    }
    return reply.header('Content-Type', 'application/json')
                .header('Access-Control-Allow-Origin', '*')
                .send(collection);
  });

  fastify.get('/stac/collections/:collectionId/items', async (request, reply) => {
    const { collectionId } = request.params;
    const { limit = 10, offset = 0, bbox } = request.query;
    const items = await stacHandler.getItems(collectionId, parseInt(limit), parseInt(offset), bbox);
    if (!items) {
      return reply.code(404)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: 'Collection not found' });
    }
    return reply.header('Content-Type', 'application/geo+json')
                .header('Access-Control-Allow-Origin', '*')
                .send(items);
  });

  fastify.get('/stac/collections/:collectionId/items/:itemId', async (request, reply) => {
    const { collectionId, itemId } = request.params;
    const item = await stacHandler.getItem(collectionId, itemId);
    if (!item) {
      return reply.code(404)
                  .header('Access-Control-Allow-Origin', '*')
                  .send({ error: 'Item not found' });
    }
    return reply.header('Content-Type', 'application/geo+json')
                .header('Access-Control-Allow-Origin', '*')
                .send(item);
  });

  fastify.get('/stac/search', async (request, reply) => {
    const results = await stacHandler.search(request.query);
    return reply.header('Content-Type', 'application/geo+json')
                .header('Access-Control-Allow-Origin', '*')
                .send(results);
  });

  fastify.post('/stac/search', async (request, reply) => {
    const results = await stacHandler.search(request.body);
    return reply.header('Content-Type', 'application/geo+json')
                .header('Access-Control-Allow-Origin', '*')
                .send(results);
  });

  logger.info('STAC API routes added');
}
