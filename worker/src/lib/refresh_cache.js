import { CacheMissError } from './errors.js';
import { parseRetryAfter } from './request.js';
import { CacheFill } from './cache_fill.js';
import { DEFAULT_SOURCE_REFRESH_MS } from './cache_policy.js';

const RETRY_REFRESH_MS = 30_000;

// Share only completed snapshots. Every request owns its refresh I/O; a late
// response cannot overwrite a snapshot installed by a peer in the meantime.
export class RefreshCache {
  #snapshot;
  #fill = new CacheFill();

  constructor({ load, logger, url, revalidate = true, refreshMs = DEFAULT_SOURCE_REFRESH_MS }) {
    this.load = load;
    this.logger = logger;
    this.url = url;
    this.revalidate = revalidate && refreshMs > 0;
    this.refreshMs = refreshMs;
  }

  get(signal) {
    return this.#fill.run('snapshot', () => this.#get(signal), {
      signal,
      ready: () => this.#snapshot && (!this.revalidate || Date.now() < this.#snapshot.expires),
    });
  }

  async #get(signal) {
    signal?.throwIfAborted();
    const previous = this.#snapshot;
    if (previous && (!this.revalidate || Date.now() < previous.expires)) return previous.value;
    try {
      const value = await this.load(previous?.value, signal);
      signal?.throwIfAborted();
      if (this.#snapshot === previous) {
        this.#snapshot = { value, expires: Date.now() + this.refreshMs };
      }
    } catch (error) {
      signal?.throwIfAborted();
      if (error instanceof CacheMissError || !previous) throw error;
      if (this.#snapshot === previous) {
        // Keep the last validated snapshot through temporary upstream failures.
        const delay = Math.max(RETRY_REFRESH_MS, parseRetryAfter(error.retryAfter) ?? 0);
        this.#snapshot = { value: previous.value, expires: Date.now() + delay };
        this.logger.warn?.(`Source refresh failed; using cached data: ${this.url}: ${error.message}`);
      }
    }
    return this.#snapshot.value;
  }
}
