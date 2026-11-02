import { ResolvedValueCache } from 'pmtiles';
import { CacheFill } from './cache_fill.js';

// Keep PMTiles' resolved-value cache and decoding/invalidation behavior. Only
// coordinate its misses; cached values never wait behind an unrelated read.
export class PMTilesCache extends ResolvedValueCache {
  #fill = new CacheFill();

  getHeader(source) {
    const key = source.getKey();
    return this.#fill.run(key, () => super.getHeader(source), {
      ready: () => this.cache.has(key),
    });
  }

  getDirectory(source, offset, length, header, signal) {
    const key = `${source.getKey()}|${header.etag || ''}|${offset}|${length}`;
    return this.#fill.run(key, () => super.getDirectory(source, offset, length, header, signal), {
      ready: () => this.cache.has(key), signal,
    });
  }
}

export const sharedPMTilesCache = new PMTilesCache(100);
