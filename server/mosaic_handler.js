const fetch = require('node-fetch');
const pmtiles = require('pmtiles');
const tilebelt = require('@mapbox/tilebelt');
const common = require('./common');

COORD_SCALER = 10000000;

function _isInSource(header, bounds) {
  // tilebelt.tileToBBOX returns [w, s, e, n]
  bounds = bounds.map((v) => Math.round(v * COORD_SCALER));
  const w = bounds[0];
  const s = bounds[1];
  const e = bounds[2];
  const n = bounds[3];
  
  // TODO: Not happy with this check..
  // this checks if the tile is perfectly inside the source bounds.
  // should that be the check?
  if (s > header['maxLat'] ||
      e > header['maxLon'] ||
      n < header['minLat'] ||
      w < header['minLon']) {
      //console.log('Tile is not in source bounds:', bounds, header);
      return false;
  }
  return true;
}

function _merge(config) {
  var merged = {};
  var commonFeaturesAdded = false;
  Object.keys(config).forEach((key, _) => {
    if (!commonFeaturesAdded) {
      merged['metadata'] = config[key]['metadata'];
      merged['header'] = {};
      const hkeys = [
        'tile_type',
        'tile_compression',
        'centerLon',
        'centerLat'
      ];
      for (const hkey of hkeys) {
        merged['header'][hkey] = config[key]['header'][hkey];
      }
      commonFeaturesAdded = true;
    }
    const minKeys = [ 'minLon', 'minLat', 'minZoom' ];
    const maxKeys = [ 'maxLon', 'maxLat', 'maxZoom', 'centerZoom' ];
    for (const hkey of minKeys) {
      if (!(hkey in merged['header'])) {
        merged['header'][hkey] = config[key]['header'][hkey];
      } else {
        if (merged['header'][hkey] > config[key]['header'][hkey]) {
          merged['header'][hkey] = config[key]['header'][hkey];
        }
      }
    }
    for (const hkey of maxKeys) {
      if (!(hkey in merged['header'])) {
        merged['header'][hkey] = config[key]['header'][hkey];
      } else {
        if (merged['header'][hkey] < config[key]['header'][hkey]) {
          merged['header'][hkey] = config[key]['header'][hkey];
        }
      }
    }
  });
  return merged;
}

// from https://nodejs.org/api/url.html#class-url
function resolve(from, to) {
  const resolvedUrl = new URL(to, new URL(from, 'resolve://'));
  if (resolvedUrl.protocol === 'resolve:') {
    // `from` is a relative URL.
    const { pathname, search, hash } = resolvedUrl;
    return pathname + search + hash;
  }
  return resolvedUrl.toString();
}

class MosaicHandler {
  constructor(url, type, tileSuffix, logger, datameetAttribution) {
    this.url = url;
    this.tileSuffix = tileSuffix;
    this.type = type;
    this.logger = logger;
    this.pmtilesDict = null;
    this.mimeTypes = null;
    this.datameetAttribution = datameetAttribution;
    this.title = null;
    this.inited = false;
  }

  _resolveKey(key) {
    // this is to undo prior stupidity where i have put ../ in the mosaic keys
    // given that i was the only idiot who is using this.. should be fine. 
    if (key.startsWith('../')) {
      key = key.slice(3);
    }
    const resolvedUrl = resolve(this.url, key);
    return resolvedUrl;
  }

  async _populateMosaic() {
    let res = await fetch(this.url);
    let data = await res.json();
    this.pmtilesDict = {};
    this.mimeTypes = {};
    for (const [key, entry] of Object.entries(data)) {
      var header = entry.header;
      var resolvedUrl = this._resolveKey(key);
      var archive = new pmtiles.PMTiles(resolvedUrl);
      header['minLat'] = header['min_lat_e7'];
      header['minLon'] = header['min_lon_e7'];
      header['maxLat'] = header['max_lat_e7'];
      header['maxLon'] = header['max_lon_e7'];
      header['centerLat'] = header['center_lat_e7'];
      header['centerLon'] = header['center_lon_e7'];
      header['maxZoom'] = header['max_zoom'];
      header['minZoom'] = header['min_zoom'];
      header['centerZoom'] = header['center_zoom'];
      this.pmtilesDict[key] = { 'pmtiles': archive, 'header': header, 'metadata': entry.metadata };
      this.mimeTypes[key] = common.getMimeType(header.tile_type);
    }
  }

  async init() {
    await this._populateMosaic();
    this.inited = true;
  }

  async initIfNeeded() {
    if (this.inited) {
        return;
    }
    // TODO: multiple simultaneous calls should lead to a single pull from pmtiles
    await this.init();
  }

  _getSource(key) {
    return this.pmtilesDict[key].pmtiles;
  }

  _getSourceKey(z, x, y) {
    let k = null;
    const bounds = tilebelt.tileToBBOX([x, y, z]);
    for (const [key, entry] of Object.entries(this.pmtilesDict)) {
      if (z > entry.header.max_zoom || z < entry.header.min_zoom) {
        continue;
      }
      if (!_isInSource(entry.header, bounds)) {
        continue;
      }
      k = key;
      break;
    }
    return k;
  }

  async getTile(z, x, y) {
    await this.initIfNeeded();

    const k = this._getSourceKey(z, x, y);
    if (k === null) {
      return [null, null];
    }
    //console.log('Fetching tile from source:', k, 'z:', z, 'x:', x, 'y:', y);

    let source = this.pmtilesDict[k].pmtiles;
    let arr = await source.getZxy(z,x,y);
    return [ arr, this.mimeTypes[k] ]
  }

  async getConfig() {
    await this.initIfNeeded();

    const config = _merge(this.pmtilesDict);
    const header = config.header;
    const metadata = config.metadata;

    return {
      tilejson: "3.0.0",
      scheme: "xyz",
      vector_layers: metadata.vector_layers,
      attribution: common.extendAttribution(metadata.attribution, this.datameetAttribution),
      description: metadata.description,
      name: metadata.name,
      version: metadata.version,
      bounds: [header.minLon, header.minLat, header.maxLon, header.maxLat].map((v) => v / COORD_SCALER),
      center: [header.centerLon, header.centerLat, header.centerZoom].map((v) => v / COORD_SCALER),
      minzoom: header.minZoom,
      maxzoom: header.maxZoom,
    };
  }

  getTitle() { return this.title; }
  setTitle(title) { this.title = title; }
}

module.exports = MosaicHandler;
