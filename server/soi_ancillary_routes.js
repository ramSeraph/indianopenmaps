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

function addSOIAncillaryRoutes(fastify) {
  fastify.get('/soi/osm/index.geojson', getCorsProxyFn(`${releaseBaseUrl}/soi-ancillary/index.geojson`));
  fastify.get('/soi/india_boundary.geojson', getCorsProxyFn(`${releaseBaseUrl}/soi-ancillary/polymap15m_area.geojson`));
  fastify.get('/soi/pdf_list.txt', getCorsProxyFn(`${releaseBaseUrl}/soi-pdfs/list.txt`));
  fastify.get('/soi/tiff_list.txt', getCorsProxyFn(`${releaseBaseUrl}/soi-tiffs/list.txt`));
}

module.exports = addSOIAncillaryRoutes;
