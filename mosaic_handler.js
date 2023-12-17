const fetch = require('node-fetch');
const pmtiles = require('pmtiles');
const tilebelt = require('@mapbox/tilebelt');

function _getBounds(z,x,y) {
  const b = tilebelt.tileToBBOX([x, y, z]);
  const w = b[0], s = b[1], e = b[2], n = b[3];
  return [[s, w], [n, e]];
}

function _isInSource(header, bounds) {
  const corner0 = bounds[0];
  const corner1 = bounds[1];
  if (corner0[0] > header['maxLat'] ||
      corner0[1] > header['maxLon'] ||
      corner1[0] < header['minLat'] ||
      corner1[1] < header['minLon']) {
      return false;
  }
  return true;
}

function _getMimeType(t) {
  if (t == pmtiles.TileType.Png) {
    return "image/png";
  } else if (t == pmtiles.TileType.Jpeg) {
    return "image/jpeg";
  } else if (t == pmtiles.TileType.Webp) {
    return "image/webp";
  } else if (t == pmtiles.TileType.Avif) {
    return "image/avif";
  } else if (t == pmtiles.TileType.Mvt) {
    return "application/vnd.mapbox-vector-tile";
  }
  throw Error(`Unknown tiletype ${t}`);
}

class MosaicHandler {
  constructor(url, tileSuffix, logger) {
    this.url = url;
    this.tileSuffix = tileSuffix;
    this.logger = logger;
    this.pmtilesDict = null;
    this.mimeTypes = null;
  }

  _resolveKey(key) {
    if (key.startsWith('../')) {
      return this.url + '/' + key;
    }
    return key;
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
      header['minLat'] = header['min_lat_e7'] / 10000000;
      header['minLon'] = header['min_lon_e7'] / 10000000;
      header['maxLat'] = header['max_lat_e7'] / 10000000;
      header['maxLon'] = header['max_lon_e7'] / 10000000;
      this.pmtilesDict[key] = { 'pmtiles': archive, 'header': header };
      this.mimeTypes[key] = _getMimeType(header.tile_type);
    }
  }

  async init() {
      await this._populateMosaic();
  }

  _getSource(key) {
    return this.pmtilesDict[key].pmtiles;
  }

  _getSourceKey(z, x, y) {
    let k = null;
    const bounds = _getBounds(z, x, y);
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
    // this.logger.info(`key=${k} for  (${x} ${y} ${z})`);
    return k;
  }

  async getTile(z, x, y) {
    const k = this._getSourceKey(z, x, y);
    if (k === null) {
        return [null, null];
    }

    let source = this.pmtilesDict[k].pmtiles;
    let arr = await source.getZxy(z,x,y);
    return [ arr, this.mimeTypes[k] ]
  }

}

module.exports = MosaicHandler;
