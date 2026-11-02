import { sharedPMTilesCache } from './pmtiles_cache.js';
import { PMTilesMosaic } from '@ramseraph/pmtiles-mosaic';
import { createPartitionAwareIndex } from '@ramseraph/pmtiles-mosaic/partition-aware';
import { extendAttribution } from './common.js';
import { UpstreamHttpError } from './errors.js';
import { PMTilesSource } from './pmtiles_source.js';
import { fetchUpstream, withRetries } from './request.js';
import { RefreshCache } from './refresh_cache.js';

export default class MosaicHandler {
  constructor(url, type, tileSuffix, logger, datameetAttribution, { revalidate = false } = {}) {
    this.type = type;
    this.tileSuffix = tileSuffix;
    this.datameetAttribution = datameetAttribution;
    this.revalidate = revalidate;
    this.manifest = new RefreshCache({
      url, logger, revalidate,
      load: async (previous, signal) => {
        const headers = new Headers();
        if (previous?.etag) headers.set('If-None-Match', previous.etag);
        const response = await fetchUpstream(url, { headers, signal, cache: revalidate ? 'no-cache' : undefined });
        if (response.status === 304 && previous) {
          return { ...previous, etag: response.headers.get('ETag') || previous.etag };
        }
        if (!response.ok) {
          const error = new UpstreamHttpError(response);
          await response.body?.cancel();
          throw error;
        }
        const document = await response.json();
        const mosaic = new PMTilesMosaic(url, {
          cache: sharedPMTilesCache,
          createIndex: createPartitionAwareIndex,
          getSource: slice => new PMTilesSource(slice.location, { revalidate }),
          loadMosaic: async () => document,
        });
        // Validate before replacing the current resolver and its spatial index.
        await mosaic.getResolver(signal);
        return { mosaic, etag: response.headers.get('ETag') };
      },
    });
  }

  async getTile(z, x, y) {
    // Re-enter the resolver and archive caches after each retry wait.
    return withRetries(async () => {
      const { mosaic } = await this.manifest.get();
      const tile = await mosaic.getTile(z, x, y);
      if (tile === undefined) return [null, null];
      return [tile, await mosaic.getMimeType()];
    });
  }

  async getConfig() {
    // routes/tiles.js supplies the public URL template after this call.
    const config = await withRetries(async () => {
      const { mosaic } = await this.manifest.get();
      return mosaic.getTileJson([]);
    });
    return {
      ...config,
      attribution: extendAttribution(config.attribution, this.datameetAttribution),
    };
  }
}
