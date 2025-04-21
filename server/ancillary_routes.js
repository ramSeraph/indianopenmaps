const fetch = require('node-fetch');

const releaseBaseUrl = 'https://github.com/ramSeraph/opendata/releases/download';
const egazStatusReleaseUrl = 'https://github.com/ramSeraph/egazette/releases/download/status'

function getCorsProxyFn(targetUrl) {
  return async function(request, reply) {
    const tResp = await fetch(targetUrl);
    const stream = tResp.body;
    return reply.header("Access-Control-Allow-Origin", "*")
                .send(stream);
  }
}

function getUrlRewriteCorsProxyFn(rewriteBase, paramName) {
  return async function(request, reply) {
    filePath = request.params[paramName];
    targetUrl = `${rewriteBase}/${filePath}`;
    const tResp = await fetch(targetUrl);
    const stream = tResp.body;
    return reply.header("Access-Control-Allow-Origin", "*")
                .send(stream);
  }
}

function addLGDRoutes(fastify) {
  fastify.get('/lgd/site_map.json', getCorsProxyFn(`${releaseBaseUrl}/lgd-latest/site_map.json`));
  fastify.get('/lgd/listing.txt', getCorsProxyFn(`${releaseBaseUrl}/lgd-latest/listing_archives.txt`));
  fastify.get('/lgd/archive/mapping.json', getCorsProxyFn(`${releaseBaseUrl}/lgd-archive/archive_mapping.json`));
  fastify.get('/lgd/archive/listing.txt', getCorsProxyFn(`${releaseBaseUrl}/lgd-archive/listing_archive.txt`));
}

function addLGDWikidataRoutes(fastify) {
  const entities = [ 'state', 'division', 'district', 'subdivision', 'subdistrict', 'district_panchayat' ];
  for (const entity of entities) {
    fastify.get(`/lgd/wikidata/reports/${entity}s.json`, getCorsProxyFn(`${releaseBaseUrl}/lgd-wikidata-sync/${entity}s.json`));
  }
  fastify.get('/lgd/wikidata/reports/status.json', getCorsProxyFn(`${releaseBaseUrl}/lgd-wikidata-sync/status.json`));
}

function addSOIRoutes(fastify) {
  fastify.get('/soi/osm/index.geojson', getCorsProxyFn(`${releaseBaseUrl}/soi-ancillary/index.geojson`));
  fastify.get('/soi/india_boundary.geojson', getCorsProxyFn(`${releaseBaseUrl}/soi-ancillary/polymap15m_area.geojson`));
  fastify.get('/soi/pdf_list.txt', getCorsProxyFn(`${releaseBaseUrl}/soi-pdfs/list.txt`));
  fastify.get('/soi/tiff_list.txt', getCorsProxyFn(`${releaseBaseUrl}/soi-tiffs/list.txt`));
}

function addEGazetteRoutes(fastify) {
  fastify.get('/egazstatus/:statusfile', getUrlRewriteCorsProxyFn(egazStatusReleaseUrl, 'statusfile'));
}

module.exports =  {
    addSOIRoutes,
    addLGDRoutes,
    addLGDWikidataRoutes,
    addEGazetteRoutes
}
