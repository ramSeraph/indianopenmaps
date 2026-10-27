import { ResolvedValueCache } from 'pmtiles';
import { PMTilesMosaic } from '@ramseraph/pmtiles-mosaic';
import { createPartitionAwareIndex } from '@ramseraph/pmtiles-mosaic/partition-aware';
import { extendAttribution } from './common.js';
import { UpstreamHttpError } from './errors.js';
import { PMTilesSource } from './pmtiles_source.js';
import { fetchUpstream, withRetries } from './request.js';

// Cache resolved data only: pending I/O belongs to the request that started it.
const sharedCache = new ResolvedValueCache(100, true);

export default class MosaicHandler {
  constructor(url, type, tileSuffix, logger, datameetAttribution) {
    this.type = type;
    this.tileSuffix = tileSuffix;
    this.datameetAttribution = datameetAttribution;
    this.mosaic = new PMTilesMosaic(url, {
      cache: sharedCache,
      createIndex: createPartitionAwareIndex,
      getSource: slice => new PMTilesSource(slice.location),
      loadMosaic: async signal => {
        const response = await fetchUpstream(url, { signal });
        if (!response.ok) {
          const error = new UpstreamHttpError(response);
          await response.body?.cancel();
          throw error;
        }
        return response.json();
      },
    });
  }

  async getTile(z, x, y) {
    // Re-enter the resolver and archive caches after each retry wait.
    const tile = await withRetries(() => this.mosaic.getTile(z, x, y));
    if (tile === undefined) return [null, null];
    return [tile, await this.mosaic.getMimeType()];
  }

  async getConfig() {
    // routes/tiles.js supplies the public URL template after this call.
    const config = await withRetries(() => this.mosaic.getTileJson([]));
    return {
      ...config,
      attribution: extendAttribution(config.attribution, this.datameetAttribution),
    };
  }
}
