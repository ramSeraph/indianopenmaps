const fetch = require('node-fetch');

const releaseBaseUrl = 'https://github.com/ramSeraph/opendata/releases/download';

function getCorsProxyFn(targetUrl, request, reply) {
  return async function(request, reply) {
    const tResp = await fetch(targetUrl)
    const stream = tResp.body;
    return reply.header("Access-Control-Allow-Origin", "*")
                .send(stream);
  }
}

function addLGDRoutes(fastify) {
  fastify.get('/lgd/site_map.json', getCorsProxyFn(`${releaseBaseUrl}/lgd-latest/site_map.json`));
  fastify.get('/lgd/listing_archives.txt', getCorsProxyFn(`${releaseBaseUrl}/lgd-latest/listing_archives.txt`));
  fastify.get('/lgd/archive_mapping.json', getCorsProxyFn(`${releaseBaseUrl}/lgd-archive/archive_mapping.json`));
}

function addLGDWikidataRoutes(fastify) {
  const entities = [ 'state', 'division', 'district', 'subdivision', 'subdistrict', 'district_panchayat' ];
  for (const entity of entities) {
    fastify.get(`/lgd/wikidata/reports/${entity}s.json`, getCorsProxyFn(`${releaseBaseUrl}/lgd-wikidata-sync/${entity}s.json`));
  }
  fastify.get('/lgd/wikidata/reports/status.json`, getCorsProxyFn(`${releaseBaseUrl}/lgd-wikidata-sync/status.json`));
}

function addSOIRoutes(fastify) {
  fastify.get('/soi/osm/index.geojson', getCorsProxyFn(`${releaseBaseUrl}/soi-ancillary/index.geojson`));
  fastify.get('/soi/india_boundary.geojson', getCorsProxyFn(`${releaseBaseUrl}/soi-ancillary/polymap15m_area.geojson`));
  fastify.get('/soi/pdf_list.txt', getCorsProxyFn(`${releaseBaseUrl}/soi-pdfs/list.txt`));
  fastify.get('/soi/tiff_list.txt', getCorsProxyFn(`${releaseBaseUrl}/soi-tiffs/list.txt`));
}

module.exports =  {
    addSOIRoutes,
    addLGDRoutes,
    addLGDWikidataRoutes
}
