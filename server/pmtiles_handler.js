const pmtiles = require('pmtiles');
const common = require('./common');

class PMTilesHandler {
  constructor(url, type, tileSuffix, logger, datameetAttribution) {
    this.source = new pmtiles.PMTiles(url);
    this.tileSuffix = tileSuffix;
    this.type = type;
    this.header = null;
    this.metadata = null;
    this.mimeType = null;
    this.logger = logger;
    this.datameetAttribution = datameetAttribution;
    this.title = null;
    this.inited = false;
  }

  async init() {
    const header = await this.source.getHeader();
    this.header = header;
    this.mimeType = common.getMimeType(header.tileType);
    this.metadata = await this.source.getMetadata();
    this.inited = true;
  }

  async initIfNeeded() {
    if (this.inited) {
        return;
    }
    await this.init();
  }

  async getTile(z, x, y) {
    await this.initIfNeeded();

    let arr = await this.source.getZxy(z, x, y);
    return [ arr, this.mimeType ];
  }

  async getConfig() {
    await this.initIfNeeded();

    return {
      tilejson: "3.0.0",
      scheme: "xyz",
      vector_layers: this.metadata.vector_layers,
      attribution: common.extendAttribution(this.metadata.attribution, this.datameetAttribution),
      description: this.metadata.description,
      name: this.metadata.name,
      version: this.metadata.version,
      bounds: [this.header.minLon, this.header.minLat, this.header.maxLon, this.header.maxLat],
      center: [this.header.centerLon, this.header.centerLat, this.header.centerZoom],
      minzoom: this.header.minZoom,
      maxzoom: this.header.maxZoom,
    };
  }

  getTitle() { return this.title; }
  setTitle(title) { this.title = title; }
}

module.exports = PMTilesHandler;
