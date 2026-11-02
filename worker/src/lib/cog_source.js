import { SourceError } from '@chunkd/source';
import { HttpError } from './errors.js';
import { strongEtag, etagMismatch } from './http_etag.js';
import { fetchWithRetry } from './request.js';
import { RefreshCache } from './refresh_cache.js';

const MAX_SOURCES = 100;
const MAX_RANGE_BYTES = 16 * 1024 * 1024;

export class COGChangedError extends HttpError {
  constructor() { super('COG changed during range read; retry the request', 503); }
}

async function upstreamError(response, source) {
  const error = new SourceError(`Failed to fetch ${source.url.href}: HTTP ${response.status}`, response.status, source);
  error.retryAfter = response.headers.get('Retry-After');
  await response.body?.cancel();
  return error;
}

// A request owns its source and pins all reads to the validated representation.
// This implements the same Source interface used by SourceHttp/SourceView.
class COGSource {
  type = 'http';

  constructor(url, metadata, signal) {
    this.url = new URL(url);
    this.metadata = { ...metadata };
    this.signal = signal;
  }

  async head() { return this.metadata; }

  async fetch(offset, length, options = {}) {
    const signal = options.signal ?? this.signal;
    signal?.throwIfAborted();
    const { size } = this.metadata;
    length = Math.min(length ?? size - offset, size - offset);
    if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(length) || length <= 0 || length > MAX_RANGE_BYTES) {
      throw new Error('Invalid or oversized COG byte range');
    }
    const end = offset + length - 1;
    const headers = new Headers({ Range: `bytes=${offset}-${end}` });
    if (this.metadata.eTag) headers.set('If-Match', this.metadata.eTag);
    const response = await fetchWithRetry(this.url, { headers, signal, cache: 'no-cache' });
    if (response.status === 412 || response.status === 416) {
      await response.body?.cancel();
      throw new COGChangedError();
    }
    if (!response.ok) throw await upstreamError(response, this);
    const received = strongEtag(response.headers);
    if (etagMismatch(this.metadata.eTag, received, { requireReceived: true })) {
      await response.body?.cancel();
      throw new COGChangedError();
    }
    if (response.status !== 206) {
      await response.body?.cancel();
      throw new Error('COG server did not return a byte range');
    }
    const range = /^bytes (\d+)-(\d+)\/(\d+)$/.exec(response.headers.get('Content-Range') ?? '');
    if (range && Number(range[3]) !== size) {
      await response.body?.cancel();
      throw new COGChangedError();
    }
    if (!range || Number(range[1]) !== offset || Number(range[2]) !== end ||
        (response.headers.has('Content-Length') && Number(response.headers.get('Content-Length')) !== length)) {
      await response.body?.cancel();
      throw new Error('Invalid COG range response');
    }
    // HEAD may omit its validator even when range responses supply one.
    this.metadata.eTag ??= received;
    const bytes = new Uint8Array(length);
    let position = 0;
    for await (const chunk of response.body) {
      if (position + chunk.length > length) throw new Error('COG range response is larger than requested');
      bytes.set(chunk, position);
      position += chunk.length;
    }
    if (position !== length) throw new Error('Truncated COG range response');
    signal?.throwIfAborted();
    return bytes.buffer;
  }
}

export class COGSourceCache {
  #sources = new Map();

  constructor({ rangeCache, logger }) {
    this.rangeCache = rangeCache;
    this.logger = logger;
  }

  #entry(url) {
    let entry = this.#sources.get(url);
    if (entry) this.#sources.delete(url);
    else entry = new RefreshCache({ url, logger: this.logger, load: async (previous, signal) => {
      const headers = new Headers();
      if (previous?.eTag) headers.set('If-None-Match', previous.eTag);
      let response = await fetchWithRetry(url, { method: 'HEAD', headers, signal, cache: 'no-cache' });
      if (response.status === 304 && previous?.eTag) {
        const received = response.headers.get('ETag');
        if (!received || received === previous.eTag) return previous;
        // A weak/different validator cannot renew byte-for-byte cache identity.
        response = await fetchWithRetry(url, { method: 'HEAD', signal, cache: 'no-cache' });
      }
      if (!response.ok) throw await upstreamError(response, { url: new URL(url) });
      const size = Number(response.headers.get('Content-Length'));
      if (!Number.isSafeInteger(size) || size < 8) throw new Error('Invalid COG content length');
      return { size, eTag: strongEtag(response.headers) };
    } });
    this.#sources.set(url, entry);
    while (this.#sources.size > MAX_SOURCES) this.#sources.delete(this.#sources.keys().next().value);
    return entry;
  }

  async read(url, operation, { signal } = {}) {
    for (let attempt = 0; ; attempt++) {
      const entry = this.#entry(url);
      const metadata = await entry.get(signal);
      // Internal identity only; preserve the complete upstream URL and query.
      const key = JSON.stringify([url, metadata.eTag, metadata.size]);
      try {
        return await operation({
          httpSource: new COGSource(url, metadata, signal),
          cacheUrl: key,
          // Weak/missing validators cannot safely identify shared byte ranges.
          shared: !!metadata.eTag,
        });
      } catch (error) {
        if (!(error instanceof COGChangedError)) throw error;
        if (this.#sources.get(url) === entry) this.#sources.delete(url);
        if (metadata.eTag) this.rangeCache.invalidate(key);
        signal?.throwIfAborted();
        if (attempt === 1) throw error;
        // Reopen metadata, TIFF directories, masks and image blocks together.
      }
    }
  }
}
