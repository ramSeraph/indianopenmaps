import { EtagMismatch } from 'pmtiles';
import { UpstreamHttpError } from './errors.js';
import { fetchUpstream } from './request.js';

// Preserve PMTiles' range/ETag contract and expose HTTP failures to the caller.
// Retries wrap operations so they recheck the caches before fetching again.
export class PMTilesSource {
  #mustReload = false;

  constructor(url) {
    this.url = url;
  }

  getKey() { return this.url; }

  async getBytes(offset, length, signal, etag) {
    const headers = new Headers({ Range: `bytes=${offset}-${offset + length - 1}` });
    const options = { headers, signal, cache: this.#mustReload ? 'reload' : undefined };
    let response = await fetchUpstream(this.url, options);

    // Some servers reject the initial 16 KiB read for very small archives.
    if (offset === 0 && response.status === 416) {
      const match = /^bytes \*\/(\d+)$/.exec(response.headers.get('Content-Range') ?? '');
      await response.body?.cancel();
      if (!match || Number(match[1]) <= 0) throw new Error('Missing content-length on 416 response');
      headers.set('Range', `bytes=0-${Number(match[1]) - 1}`);
      response = await fetchUpstream(this.url, { ...options, cache: 'reload' });
    }

    if (response.status >= 300 && response.status !== 416) {
      const error = new UpstreamHttpError(response);
      await response.body?.cancel();
      throw error;
    }

    let newEtag = response.headers.get('ETag');
    if (newEtag?.startsWith('W/')) newEtag = null;
    if (response.status === 416 || (etag && newEtag && newEtag !== etag)) {
      this.#mustReload = true;
      await response.body?.cancel();
      throw new EtagMismatch(`Server returned non-matching ETag ${etag}`);
    }

    const contentLength = response.headers.get('Content-Length');
    if (response.status === 200 && (!contentLength || Number(contentLength) > length)) {
      await response.body?.cancel();
      throw new Error(
        'Server returned no content-length header or content-length exceeding request. ' +
        'Check that your storage backend supports HTTP Byte Serving.'
      );
    }

    return {
      data: await response.arrayBuffer(),
      etag: newEtag || undefined,
      cacheControl: response.headers.get('Cache-Control') || undefined,
      expires: response.headers.get('Expires') || undefined,
    };
  }
}
