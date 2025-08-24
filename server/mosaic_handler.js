const fetch = require('node-fetch');
const pmtiles = require('pmtiles');
const tilebelt = require('@mapbox/tilebelt');
const Flatbush = require('flatbush').default;
const common = require('./common');

COORD_SCALER = 10000000;
// TODO: measure performance and adjust this threshold
FLATBUSH_THRESHOLD = 10;

function _isInSource(header, bounds) {

  // tilebelt.tileToBBOX returns [w, s, e, n]
  const w = bounds[0];
  const s = bounds[1];
  const e = bounds[2];
  const n = bounds[3];
  
  // TODO: Not happy with this check..
  // this checks if the tile is perfectly inside the source bounds.
  // should that be the check?
  if (s > header['max_lat_e7'] ||
      e > header['max_lon_e7'] ||
      n < header['min_lat_e7'] ||
      w < header['min_lon_e7']) {
      return false;
  }
  return true;
}

// TODO: This is probably broken but only applies to old mosaics and will be deprecated
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
        'center_lon_e7',
        'center_lat_e7',
      ];
      for (const hkey of hkeys) {
        merged['header'][hkey] = config[key]['header'][hkey];
      }
      commonFeaturesAdded = true;
    }

    const minKeys = [ 'min_lon_e7', 'min_lon_e7', 'min_zoom' ];
    for (const hkey of minKeys) {
      if (!(hkey in merged['header'])) {
        merged['header'][hkey] = config[key]['header'][hkey];
      } else {
        if (merged['header'][hkey] > config[key]['header'][hkey]) {
          merged['header'][hkey] = config[key]['header'][hkey];
        }
      }
    }

    const maxKeys = [ 'max_lon_e7', 'max_lat_e7', 'max_zoom', 'center_zoom' ];
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
    this.mimeType = null;
    this.pmtilesDict = {};
    this.mimeTypes = null;
    this.datameetAttribution = datameetAttribution;
    this.title = null;
    this.inited = false;
    this.initializingPromise = null;
    this.mosaicVersion = '0';
    this.index_map = {};
    this.keys_map = {};
  }

  _resolveKey(key) {
    // to deal with some old mosaics that have keys starting with '../'
    // version 0 will be deprecated soon
    if (this.mosaicVersion === '0' && key.startsWith('../')) {
      key = key.slice(3);
    }
    const resolvedUrl = resolve(this.url, key);
    return resolvedUrl;
  }

  async _populateMosaic() {
    let res = await fetch(this.url);
    let data = await res.json();

    let slices = null;
    if (data && data.hasOwnProperty('version')) {
      this.mosaicVersion = data.version;
    }

    if (this.mosaicVersion === '0') {
      slices = data;
    } else {
      slices = data.slices;
    }

    for (const [key, entry] of Object.entries(slices)) {
      var header = entry.header;
      var resolvedUrl = this._resolveKey(key);
      var archive = new pmtiles.PMTiles(resolvedUrl);
      header = Object.assign({}, entry.header);
      this.pmtilesDict[key] = { 'pmtiles': archive, 'header': header };
      if (this.mosaicVersion === '0') {
        this.pmtilesDict[key]['metadata'] = await archive.getMetadata();
      }

      for (let z = header.min_zoom; z <= header.max_zoom; z++) {
        if (!this.keys_map.hasOwnProperty(z)) {
          this.keys_map[z] = [];
        }
        this.keys_map[z].push(key);
      }
    }

    if (this.mosaicVersion === '0') {
      this.mosaicConfig = _merge(this.pmtilesDict);
    } else {
      this.mosaicConfig = { 'header': data.header, 'metadata': data.metadata };
    }

    this.mimeType = common.getMimeType(this.mosaicConfig.header.tile_type);
  }

  async _populateIndices() {
    for (const [z, keys] of Object.entries(this.keys_map)) {
      if (keys.length < FLATBUSH_THRESHOLD) {
        this.index_map[z] = null;
        continue;
      }

      const index = new Flatbush(keys.length, 16, Int32Array);
      for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        const entry = this.pmtilesDict[key];
        const header = entry.header;
        index.add(header.min_lon_e7, header.min_lat_e7, header.max_lon_e7, header.max_lat_e7);
      }
      index.finish();
      this.index_map[z] = index;
    }
  }

  async init() {
    await this._populateMosaic();
    this._populateIndices();
    this.inited = true;
  }

  async initIfNeeded() {
    if (this.inited) {
        return;
    }
    if (this.initializingPromise) {
        await this.initializingPromise;
        return;
    }
    this.initializingPromise = this.init();
    try {
        await this.initializingPromise;
    } catch (e) {
        this.initializingPromise = null;
        throw e;
    }
  }

  _getSource(key) {
    return this.pmtilesDict[key].pmtiles;
  }

  _getSourceKey(z, x, y) {
    if (!this.index_map.hasOwnProperty(z)) {
      return null;
    }

    const bounds = tilebelt.tileToBBOX([x, y, z]).map((v) => Math.round(v * COORD_SCALER));

    const zKeys = this.keys_map[z];
    const index = this.index_map[z];

    var foundKeys = zKeys;
    if (index !== null ) {
      foundKeys = index.search(bounds[0], bounds[1], bounds[2], bounds[3]).map((i) => zKeys[i]);
    }

    for (const key of foundKeys) {
      const entry = this.pmtilesDict[key];
      if (_isInSource(entry.header, bounds)) {
        return key;
      }
    }
    return null;
  }

  async getTile(z, x, y) {
    await this.initIfNeeded();

    const k = this._getSourceKey(z, x, y);
    if (k === null) {
      return [null, null];
    }

    let source = this.pmtilesDict[k].pmtiles;
    let arr = await source.getZxy(z,x,y);
    return [arr, this.mimeType]
  }

  async getConfig() {
    await this.initIfNeeded();

    const header = this.mosaicConfig.header;
    const metadata = this.mosaicConfig.metadata;

    var out = {
      tilejson: "3.0.0",
      scheme: "xyz",
      attribution: common.extendAttribution(metadata.attribution, this.datameetAttribution),
      description: metadata.description,
      name: metadata.name,
      version: metadata.version,
      bounds: [header.min_lon_e7, header.min_lat_e7, header.max_lon_e7, header.max_lat_e7].map((v) => v / COORD_SCALER),
      center: [...[header.center_lon_e7, header.center_lat_e7].map((v) => v / COORD_SCALER), header.center_zoom ],
      minzoom: header.min_zoom,
      maxzoom: header.max_zoom,
    };
    if (metadata.hasOwnProperty('vector_layers')) {
      out['vector_layers'] = metadata.vector_layers;
    }
    return out;
  }

  getTitle() { return this.title; }
  setTitle(title) { this.title = title; }
}

module.exports = MosaicHandler;
