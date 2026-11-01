import { CacheMissError } from './errors.js';
import { parseRetryAfter } from './request.js';

export const SOURCE_REFRESH_MS = 5 * 60 * 1000;
const RETRY_REFRESH_MS = 30_000;

// Share only completed snapshots. Every request owns its refresh I/O; a late
// response cannot overwrite a snapshot installed by a peer in the meantime.
export class RefreshCache {
  #snapshot;

  constructor({ load, logger, url, revalidate = true }) {
    this.load = load;
    this.logger = logger;
    this.url = url;
    this.revalidate = revalidate;
  }

  async get(signal) {
    signal?.throwIfAborted();
    const previous = this.#snapshot;
    if (previous && (!this.revalidate || Date.now() < previous.expires)) return previous.value;
    try {
      const value = await this.load(previous?.value, signal);
      signal?.throwIfAborted();
      if (this.#snapshot === previous) {
        this.#snapshot = { value, expires: Date.now() + SOURCE_REFRESH_MS };
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
