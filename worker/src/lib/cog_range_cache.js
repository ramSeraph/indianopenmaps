import { PerOriginSemaphore } from '@developmentseed/geotiff';

// Share only completed bytes across Worker requests. Pending I/O and limiter
// waiters belong to the request that created them, including after cancellation.
export class COGRangeCache {
  name = 'cog-range-cache';
  #entries = new Map();
  #size = 0;
  #maxSize;

  constructor(maxSize = 32 * 1024 * 1024) { this.#maxSize = maxSize; }

  get byteLength() { return this.#size; }

  forRequest() {
    const pending = new Map();
    const limiter = new PerOriginSemaphore({ maxRequests: 4 });
    return {
      name: this.name,
      fetch: (request, next) => {
        const { length, offset, source } = request;
        const key = `${source.url.href}@${offset}+${length}`;
        const hit = this.#get(key);
        if (hit) return Promise.resolve(hit);
        if (pending.has(key)) return pending.get(key);
        const promise = this.#read(key, request, next, limiter).finally(() => pending.delete(key));
        pending.set(key, promise);
        return promise;
      },
    };
  }

  #get(key) {
    const hit = this.#entries.get(key);
    if (hit) {
      this.#entries.delete(key);
      this.#entries.set(key, hit);
    }
    return hit;
  }

  #set(key, bytes) {
    if (bytes.byteLength === 0 || bytes.byteLength > this.#maxSize) return;
    const previous = this.#entries.get(key);
    if (previous) {
      this.#size -= previous.byteLength;
      this.#entries.delete(key);
    }
    while (this.#size + bytes.byteLength > this.#maxSize) {
      const oldest = this.#entries.keys().next().value;
      this.#size -= this.#entries.get(oldest).byteLength;
      this.#entries.delete(oldest);
    }
    this.#entries.set(key, bytes);
    this.#size += bytes.byteLength;
  }

  async #read(key, request, next, limiter) {
    const release = await limiter.acquire(request.source.url, request.signal);
    try {
      // Another request may have completed this range while we were queued.
      const hit = this.#get(key);
      if (hit) return hit;
      const bytes = await next(request);
      if (request.length != null) this.#set(key, bytes);
      return bytes;
    }
    finally { release(); }
  }
}
