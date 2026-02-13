import pmtiles from 'pmtiles';
import { getMimeType, extendAttribution } from './common.js';

// Shared cache across all PMTiles instances to reduce memory usage
const sharedCache = new pmtiles.SharedPromiseCache(100, true);

class PMTilesHandler {
  constructor(url, type, tileSuffix, logger, datameetAttribution) {
    this.url = url;
    this.source = null; // Lazy - created in init()
    this.tileSuffix = tileSuffix;
    this.type = type;
    this.header = null;
    this.metadata = null;
    this.mimeType = null;
    this.logger = logger;
    this.datameetAttribution = datameetAttribution;
    this.title = null;
    this.inited = false;
    this.initializingPromise = null;
  }

  async init() {
    this.source = new pmtiles.PMTiles(this.url, sharedCache);
    const header = await this.source.getHeader();
    this.header = header;
    this.mimeType = getMimeType(header.tileType);
    this.metadata = await this.source.getMetadata();
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
      attribution: extendAttribution(this.metadata.attribution, this.datameetAttribution),
      description: this.metadata.description,
      name: this.metadata.name,
      version: this.metadata.version,
      bounds: [this.header.minLon, this.header.minLat, this.header.maxLon, this.header.maxLat],
      center: [this.header.centerLon, this.header.centerLat, this.header.centerZoom],
      minzoom: this.header.minZoom,
      maxzoom: this.header.maxZoom,
    };
  }
}

export default PMTilesHandler;
