const pmtiles = require('pmtiles');

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

class PMTilesHandler {
  constructor(url, tileSuffix, logger) {
    this.source = new pmtiles.PMTiles(url);
    this.tileSuffix = tileSuffix;
    this.mimeType = null;
    this.logger = logger;
  }

  async init() {
    const header = await this.source.getHeader();
    this.mimeType = _getMimeType(header.tileType);
  }

  async getTile(z, x, y) {
    let arr = await this.source.getZxy(z, x, y);
    return [ arr, this.mimeType ];
  }
}

module.exports = PMTilesHandler;
