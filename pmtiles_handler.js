const pmtiles = require('pmtiles');
const getMimeType = require('./common').getMimeType;

class PMTilesHandler {
  constructor(url, tileSuffix, logger) {
    this.source = new pmtiles.PMTiles(url);
    this.tileSuffix = tileSuffix;
    this.header = null;
    this.metadata = null;
    this.mimeType = null;
    this.logger = logger;
  }

  async init() {
    const header = await this.source.getHeader();
    this.header = header;
    this.mimeType = getMimeType(header.tileType);
    this.metadata = await this.source.getMetadata;
  }

  async getTile(z, x, y) {
    let arr = await this.source.getZxy(z, x, y);
    return [ arr, this.mimeType ];
  }

  async tileJSON() {
  }
}

module.exports = PMTilesHandler;
