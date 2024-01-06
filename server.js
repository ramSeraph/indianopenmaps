const path = require('node:path')
const fastify = require('fastify')({ logger: true });

const fastifyStatic = require('@fastify/static');
const MosaicHandler = require('./mosaic_handler');
const PMTilesHandler = require('./pmtiles_handler');

const GOBIReleaseUrl = 'https://github.com/ramSeraph/google_buildings_india/releases/download/GOBI-latest/';
const MSBIReleaseUrl = 'https://github.com/ramSeraph/ms_buildings_india/releases/download/MSBI/';
const OPReleaseUrl = 'https://github.com/ramSeraph/overture_places_india/releases/tag/overture-places/';

function getTilesUrl(rname, fname) {
  return `https://github.com/ramSeraph/indian_admin_boundaries/releases/download/${rname}/${fname}`;
}

function getWaterTilesUrl(rname, fname) {
  return `https://github.com/ramSeraph/indian_water_features/releases/download/${rname}/${fname}`;
}

function getRoadsTilesUrl(rname, fname) {
  return `https://github.com/ramSeraph/indian_roads/releases/download/${rname}/${fname}`;
}

function getCommsTilesUrl(rname, fname) {
  return `https://github.com/ramSeraph/indian_communications/releases/download/${rname}/${fname}`;
}

const logger = fastify.log;

const handlerMap = {
  '/google-buildings/': new MosaicHandler(GOBIReleaseUrl + 'mosaic.json', 'pbf', logger),
  '/ms-buildings/': new MosaicHandler(MSBIReleaseUrl + 'mosaic.json', 'pbf', logger),

  '/overture-places/': new MosaicHandler(OPReleaseUrl + 'mosaic.json', 'pbf', logger),

  '/not-so-open/cell-towers/tarangsanchar/': new PMTilesHandler(getCommsTilesUrl('cell-towers', 'TS_Celltower_locations.pmtiles'), 'pbf', logger),
  '/not-so-open/cell-towers/nic/': new PMTilesHandler(getCommsTilesUrl('cell-towers', 'NIC_Celltower_locations.pmtiles'), 'pbf', logger),
  '/not-so-open/bharatnet/': new PMTilesHandler(getCommsTilesUrl('bharatnet', 'BBNL_points.pmtiles'), 'pbf', logger),

  '/fb-roads/': new PMTilesHandler(getRoadsTilesUrl('fb-roads', 'fb_roads_india.pmtiles'), 'pbf', logger),
  '/ms-roads/': new PMTilesHandler(getRoadsTilesUrl('ms-roads', 'ms_roads_india.pmtiles'), 'pbf', logger),
  '/pmgsy-roads/': new PMTilesHandler(getRoadsTilesUrl('pmgsy-roads', 'pmgsy_roads.pmtiles'), 'pbf', logger),
  '/pmgsy-roads-candidates/': new PMTilesHandler(getRoadsTilesUrl('pmgsy-roads', 'pmgsy_roads_candidates.pmtiles'), 'pbf', logger),
  '/pmgsy-roads-proposals-i/': new PMTilesHandler(getRoadsTilesUrl('pmgsy-roads', 'pmgsy_roads_proposals_i.pmtiles'), 'pbf', logger),
  '/pmgsy-roads-proposals-ii/': new PMTilesHandler(getRoadsTilesUrl('pmgsy-roads', 'pmgsy_roads_proposals_ii.pmtiles'), 'pbf', logger),
  '/pmgsy-roads-proposals-iii/': new PMTilesHandler(getRoadsTilesUrl('pmgsy-roads', 'pmgsy_roads_proposals_iii.pmtiles'), 'pbf', logger),
  '/pmgsy-roads-proposals-rcplwea/': new PMTilesHandler(getRoadsTilesUrl('pmgsy-roads', 'pmgsy_roads_proposals_rcplwea.pmtiles'), 'pbf', logger),
  '/not-so-open/soi-roads/': new PMTilesHandler(getRoadsTilesUrl('soi-roads', 'SOI_Roads.pmtiles'), 'pbf', logger),
  '/not-so-open/soi-tracks/': new PMTilesHandler(getRoadsTilesUrl('soi-roads', 'SOI_Tracks.pmtiles'), 'pbf', logger),
  '/not-so-open/nic-roads/': new PMTilesHandler(getRoadsTilesUrl('nic-roads', 'NIC_Roads.pmtiles'), 'pbf', logger),

  '/not-so-open/census2011/districts/': new PMTilesHandler(getTilesUrl('census-2011', 'Districts_2011.pmtiles'), 'pbf', logger),
  '/not-so-open/census2011/subdistricts/': new PMTilesHandler(getTilesUrl('census-2011', 'SubDistricts_2011.pmtiles'), 'pbf', logger),
  '/not-so-open/census2011/village-points/': new PMTilesHandler(getTilesUrl('census-2011', 'Census_Villages.pmtiles'), 'pbf', logger),
  '/shrug-census2011/districts/': new PMTilesHandler(getTilesUrl('census-2011', 'shrug-district-pc11.pmtiles'), 'pbf', logger),
  '/shrug-census2011/subdistricts/': new PMTilesHandler(getTilesUrl('census-2011', 'shrug-subdistrict-pc11.pmtiles'), 'pbf', logger),
  '/shrug-census2011/villages/': new PMTilesHandler(getTilesUrl('census-2011', 'shrug-village-pc11.pmtiles'), 'pbf', logger),

  '/not-so-open/states/lgd/': new PMTilesHandler(getTilesUrl('states', 'LGD_States.pmtiles'), 'pbf', logger),
  '/not-so-open/states/bhuvan/': new PMTilesHandler(getTilesUrl('states', 'bhuvan_states.pmtiles'), 'pbf', logger),
  '/states/soi/': new PMTilesHandler(getTilesUrl('states', 'SOI_States.pmtiles'), 'pbf', logger),

  '/not-so-open/districts/lgd/': new PMTilesHandler(getTilesUrl('districts', 'LGD_Districts.pmtiles'), 'pbf', logger),
  '/not-so-open/districts/bhuvan/': new PMTilesHandler(getTilesUrl('districts', 'bhuvan_districts.pmtiles'), 'pbf', logger),
  '/districts/soi/': new PMTilesHandler(getTilesUrl('districts', 'SOI_Districts.pmtiles'), 'pbf', logger),

  '/not-so-open/subdistricts/lgd/': new PMTilesHandler(getTilesUrl('subdistricts', 'LGD_Subdistricts.pmtiles'), 'pbf', logger),
  '/subdistricts/soi/': new PMTilesHandler(getTilesUrl('subdistricts', 'SOI_Subdistricts.pmtiles'), 'pbf', logger),

  '/not-so-open/blocks/lgd/': new PMTilesHandler(getTilesUrl('blocks', 'LGD_Blocks.pmtiles'), 'pbf', logger),
  '/not-so-open/blocks/bhuvan/': new PMTilesHandler(getTilesUrl('blocks', 'bhuvan_blocks.pmtiles'), 'pbf', logger),
  '/blocks/pmgsy/': new PMTilesHandler(getTilesUrl('blocks', 'PMGSY_Blocks.pmtiles'), 'pbf', logger),

  '/not-so-open/panchayats/lgd/': new PMTilesHandler(getTilesUrl('panchayats', 'LGD_panchayats.pmtiles'), 'pbf', logger),
  '/not-so-open/panchayats/bhuvan/': new PMTilesHandler(getTilesUrl('panchayats', 'bhuvan_panchayats.pmtiles'), 'pbf', logger),

  '/not-so-open/villages/lgd/': new PMTilesHandler(getTilesUrl('villages', 'LGD_Villages.pmtiles'), 'pbf', logger),
  '/not-so-open/villages/bhuvan/': new PMTilesHandler(getTilesUrl('villages', 'bhuvan_villages.pmtiles'), 'pbf', logger),
  '/villages/soi/': new PMTilesHandler(getTilesUrl('villages', 'SOI_villages.pmtiles'), 'pbf', logger),
  '/not-so-open/village-points/soi/': new PMTilesHandler(getTilesUrl('villages', 'SOI_VILLAGE_POINT.pmtiles'), 'pbf', logger),

  '/not-so-open/habitations/soi/': new PMTilesHandler(getTilesUrl('habitations', 'SOI_places.pmtiles'), 'pbf', logger),
  '/not-so-open/habitations/soi-village-blocks/': new PMTilesHandler(getTilesUrl('habitations', 'SOI_VILLAGE_BLOCKS.pmtiles'), 'pbf', logger),
  '/habitations/pmgsy/': new PMTilesHandler(getTilesUrl('habitations', 'PMGSY_Habitations.pmtiles'), 'pbf', logger),
  '/habitations/karmashapes-polys/': new PMTilesHandler(getTilesUrl('habitations', 'karmashapes_polygons_v0.pmtiles'), 'pbf', logger),
  '/habitations/karmashapes-points/': new PMTilesHandler(getTilesUrl('habitations', 'karmashapes_points_v0.pmtiles'), 'pbf', logger),

  '/not-so-open/constituencies/parliament/lgd/': new PMTilesHandler(getTilesUrl('constituencies', 'LGD_Parliament_Constituencies.pmtiles'), 'pbf', logger),
  '/not-so-open/constituencies/assembly/lgd/': new PMTilesHandler(getTilesUrl('constituencies', 'LGD_Assembly_Constituencies.pmtiles'), 'pbf', logger),

  '/not-so-open/forests/circles/fsi/': new PMTilesHandler(getTilesUrl('forests', 'FSI_Circles.pmtiles'), 'pbf', logger),
  '/not-so-open/forests/divisions/fsi/': new PMTilesHandler(getTilesUrl('forests', 'FSI_Divisions.pmtiles'), 'pbf', logger),
  '/not-so-open/forests/ranges/fsi/': new PMTilesHandler(getTilesUrl('forests', 'FSI_Ranges.pmtiles'), 'pbf', logger),
  '/not-so-open/forests/beats/fsi/': new PMTilesHandler(getTilesUrl('forests', 'FSI_Beats.pmtiles'), 'pbf', logger),

  '/not-so-open/pincodes/': new PMTilesHandler(getTilesUrl('postal', 'PincodeBoundaries.pmtiles'), 'pbf', logger),

  '/basins/wris/': new PMTilesHandler(getWaterTilesUrl('hydro-boundaries', 'WRIS_Basin.pmtiles'), 'pbf', logger),
  '/sub-basins/wris/': new PMTilesHandler(getWaterTilesUrl('hydro-boundaries', 'WRIS_SubBasin.pmtiles'), 'pbf', logger),
  '/watersheds/wris/': new PMTilesHandler(getWaterTilesUrl('hydro-boundaries', 'WRIS_Watershed.pmtiles'), 'pbf', logger),

  '/waterbodies/wris/': new PMTilesHandler(getWaterTilesUrl('waterbodies', 'WRIS_Waterbodies.pmtiles'), 'pbf', logger),
  '/river-polygons/wris/': new PMTilesHandler(getWaterTilesUrl('rivers', 'WRIS_River_Polygons.pmtiles'), 'pbf', logger),
  '/rivers/wris/': new PMTilesHandler(getWaterTilesUrl('rivers', 'WRIS_Rivers.pmtiles'), 'pbf', logger),
  '/streams/wris/': new PMTilesHandler(getWaterTilesUrl('rivers', 'WRIS_Streams.pmtiles'), 'pbf', logger),
};

const port = 3000;

async function getTile(handler, request, reply) {
  const { z, x, y } = request.params;
  const [ arr, mimeType ] = await handler.getTile(z,x,y);
  if (arr) {
    return reply.header('Content-Type', mimeType)
                .header('Cache-Control', 'max-age=86400000')
                .header('Access-Control-Allow-Origin', "*")
                .send(new Uint8Array(arr.data));
  }
  return reply.code(404)
              .header('Access-Control-Allow-Origin', "*")
              .send('');
}

async function initializeHandlers() {
  fastify.log.info('initializing handlers');
  const promises = Object.keys(handlerMap).map((k) => handlerMap[k].init());
  await Promise.all(promises);
  fastify.log.info('done initializing handlers');
}

function addRoutes() {
  fastify.log.info('adding routes');
  Object.keys(handlerMap).forEach((rPrefix, _) => {
    const handler = handlerMap[rPrefix];
    const tileSuffix = handler.tileSuffix;
    fastify.get(`${rPrefix}:z/:x/:y.${tileSuffix}`, getTile.bind(null, handler));
  });
}

async function start() {
  try {
    fastify.register(fastifyStatic, {
      root: path.join(__dirname, 'home'),
    })

    fastify.addHook('onReady', initializeHandlers);
    addRoutes();
    await fastify.listen({ host: '0.0.0.0', port: port });
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
}

start();
