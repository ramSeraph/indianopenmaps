import { ResolvedValueCache } from 'pmtiles';
import { PMTilesMosaic } from '@ramseraph/pmtiles-mosaic';
import { createPartitionAwareIndex } from '@ramseraph/pmtiles-mosaic/partition-aware';
import { extendAttribution } from './common.js';

// Cache resolved data only: pending I/O belongs to the request that started it.
const sharedCache = new ResolvedValueCache(100, true);

export default class MosaicHandler {
  constructor(url, type, tileSuffix, logger, datameetAttribution) {
    this.type = type;
    this.tileSuffix = tileSuffix;
    this.datameetAttribution = datameetAttribution;
    this.mosaic = new PMTilesMosaic(url, { cache: sharedCache, createIndex: createPartitionAwareIndex });
  }

  async getTile(z, x, y) {
    const tile = await this.mosaic.getTile(z, x, y);
    if (tile === undefined) return [null, null];
    return [tile, await this.mosaic.getMimeType()];
  }

  async getConfig() {
    // routes/tiles.js supplies the public URL template after this call.
    const config = await this.mosaic.getTileJson([]);
    return {
      ...config,
      attribution: extendAttribution(config.attribution, this.datameetAttribution),
    };
  }
}
