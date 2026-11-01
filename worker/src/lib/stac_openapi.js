import { PAGE_LIMIT } from './stac_query.js';

export function stacOpenAPI(base) {
  const properties = {
    limit: { type: 'integer', minimum: 1, maximum: PAGE_LIMIT, default: 100 },
    offset: { type: 'integer', minimum: 0, default: 0 },
    collections: { type: 'array', items: { type: 'string' } },
    ids: { type: 'array', items: { type: 'string' } },
    bbox: { type: 'array', items: { type: 'number' }, minItems: 4, maxItems: 6 },
    datetime: { type: 'string', description: 'RFC3339 timestamp or interval; use .. for an open endpoint' },
    intersects: { type: 'object', description: 'GeoJSON geometry; mutually exclusive with bbox' },
  };
  const parameters = Object.entries(properties).map(([name, schema]) => ({ name, in: 'query', required: false,
    schema: name === 'intersects' ? { type: 'string', description: 'JSON-encoded GeoJSON geometry' } : schema,
    ...(schema.type === 'array' ? { style: 'form', explode: false } : {}) }));
  const response = type => ({ description: 'Successful response', content: { [type]: { schema: { type: 'object' } } } });
  const operation = (summary, geojson = false, parameters = []) => ({ summary, parameters, responses: {
    200: response(geojson ? 'application/geo+json' : 'application/json'),
    400: { description: 'Invalid request' }, 404: { description: 'Collection or item not found' },
    424: { description: 'Source index unavailable' },
  } });
  const collection = { name: 'collectionId', in: 'path', required: true, schema: { type: 'string' } };
  const item = { name: 'itemId', in: 'path', required: true, schema: { type: 'string' } };
  return { openapi: '3.0.3', info: { title: 'Indian Open Maps STAC API', version: '1.0.0' }, servers: [{ url: base }], paths: {
    '/': { get: operation('STAC landing page') },
    '/conformance': { get: operation('Conformance classes') },
    '/collections': { get: operation('List collections', false, parameters.filter(p => ['limit', 'offset'].includes(p.name))) },
    '/collections/{collectionId}': { get: operation('Collection metadata', false, [collection]) },
    '/collections/{collectionId}/items': { get: operation('Collection items', true, [collection, ...parameters]) },
    '/collections/{collectionId}/items/{itemId}': { get: operation('Individual item', true, [collection, item]) },
    '/search': { get: operation('Search items', true, parameters), post: { ...operation('Search items', true),
      requestBody: { required: true, content: { 'application/json': { schema: { type: 'object', properties, additionalProperties: false } } } } } },
  } };
}
