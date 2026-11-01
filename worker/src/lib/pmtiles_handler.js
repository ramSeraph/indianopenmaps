import { PMTiles, ResolvedValueCache } from 'pmtiles';
import { getMimeType, extendAttribution } from './common.js';
import { PMTilesSource } from './pmtiles_source.js';
import { withRetries } from './request.js';
import { RefreshCache } from './refresh_cache.js';

// Cache resolved data only: pending I/O belongs to the request that started it.
const sharedCache = new ResolvedValueCache(100, true);

class PMTilesHandler {
  constructor(url, type, tileSuffix, logger, datameetAttribution, { revalidate = false } = {}) {
    this.tileSuffix = tileSuffix;
    this.type = type;
    this.datameetAttribution = datameetAttribution;
    this.revalidate = revalidate;
    this.config = new RefreshCache({
      url, logger, revalidate,
      load: async previous => {
        const source = previous?.source ?? new PMTiles(new PMTilesSource(url, { revalidate }), sharedCache);
        // Metadata performs a range read, letting PMTiles detect replacements.
        // Read the header afterwards because getMetadata may invalidate it.
        const metadata = await source.getMetadata();
        const header = await source.getHeader();
        return { source, header, metadata };
      },
    });
  }

  async getTile(z, x, y) {
    return withRetries(async () => {
      const { source } = await this.config.get();
      const arr = await source.getZxy(z, x, y);
      return [arr, getMimeType((await source.getHeader()).tileType)];
    });
  }

  async getConfig() {
    const { header, metadata } = await withRetries(() => this.config.get());

    return {
      tilejson: "3.0.0",
      scheme: "xyz",
      vector_layers: metadata.vector_layers,
      attribution: extendAttribution(metadata.attribution, this.datameetAttribution),
      description: metadata.description,
      name: metadata.name,
      version: metadata.version,
      bounds: [header.minLon, header.minLat, header.maxLon, header.maxLat],
      center: [header.centerLon, header.centerLat, header.centerZoom],
      minzoom: header.minZoom,
      maxzoom: header.maxZoom,
    };
  }
}

export default PMTilesHandler;
