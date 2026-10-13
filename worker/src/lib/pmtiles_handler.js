import { PMTiles, ResolvedValueCache } from 'pmtiles';
import { getMimeType, extendAttribution } from './common.js';

// Cache resolved data only: pending I/O belongs to the request that started it.
const sharedCache = new ResolvedValueCache(100, true);

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
  }

  async init() {
    const source = new PMTiles(this.url, sharedCache);
    const header = await source.getHeader();
    const metadata = await source.getMetadata();
    const mimeType = getMimeType(header.tileType);

    // Publish complete state together; concurrent requests initialize independently.
    this.source = source;
    this.header = header;
    this.mimeType = mimeType;
    this.metadata = metadata;
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
