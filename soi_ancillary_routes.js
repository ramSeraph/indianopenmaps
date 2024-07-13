const fetch = require('node-fetch');

const ancillaryUrl = 'https://github.com/ramSeraph/opendata/releases/download/soi-ancillary/';

function getCorsProxyFn(targetUrl, request, reply) {
  return async function(request, reply) {
    const tResp = await fetch(targetUrl)
    const stream = tResp.body;
    return reply.header("Access-Control-Allow-Origin", "*")
                .send(stream);
  }
}

function addSOIAncillaryRoutes(fastify) {
  fastify.get('/soi/osm/index.geojson', getCorsProxyFn(ancillaryUrl + 'index.geojson'));
  fastify.get('/soi/india_boundary.geojson', getCorsProxyFn(ancillaryUrl + 'polymap15m_area.geojson'));
}

module.exports = addSOIAncillaryRoutes;
