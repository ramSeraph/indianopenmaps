import { isCacheOnly } from './request.js';

export const CACHE_FILL_LEASE_MS = 1000;
const MAX_FILL_MS = 60_000;
const MAX_LEASES = 1024;

// Each follower owns its wake-up promise and fallback timer. A leader signals
// completion without sharing its fetch promise or any I/O objects. Requires
// Workers' default handle_cross_request_promise_resolution behavior: disabling
// it can move the waiter's continuation into the leader's request context.
export class CacheFill {
  #leases = new Map();

  async run(key, operation, { ready = () => false, signal } = {}) {
    for (;;) {
      signal?.throwIfAborted();
      // Retry probes must neither wait for a peer nor delay Retry-After handling.
      if (ready() || isCacheOnly()) return operation();
      const lease = this.#leases.get(key);
      if (!lease || Date.now() >= lease.expires) break;
      await this.#wait(lease, signal);
    }

    const now = Date.now();
    for (const [oldKey, lease] of this.#leases) {
      if (now >= lease.expires) this.#leases.delete(oldKey);
    }
    // Coordination is an optimization, not a reason to retain unbounded state.
    if (this.#leases.size >= MAX_LEASES) return operation();
    const lease = { expires: now + CACHE_FILL_LEASE_MS, waiters: new Set() };
    const deadline = now + MAX_FILL_MS;
    this.#leases.set(key, lease);
    const heartbeat = setInterval(() => {
      lease.expires = Math.min(Date.now() + CACHE_FILL_LEASE_MS, deadline);
      if (Date.now() >= deadline) clearInterval(heartbeat);
    }, CACHE_FILL_LEASE_MS / 4);
    const release = () => {
      clearInterval(heartbeat);
      if (this.#leases.get(key) === lease) this.#leases.delete(key);
      for (const wake of lease.waiters) wake();
      lease.waiters.clear();
    };
    signal?.addEventListener('abort', release, { once: true });
    try {
      return await operation();
    } finally {
      signal?.removeEventListener('abort', release);
      release();
    }
  }

  async #wait(lease, signal) {
    let wake;
    const promise = new Promise(resolve => { wake = resolve; });
    // This request-owned timer keeps the waiter alive and lets it recover if
    // the leader ends without running finally. Normal completion wakes it now;
    // only lease expiry needs a timed check (once a second during slow reads).
    const timer = setTimeout(wake, Math.max(0, lease.expires - Date.now()));
    lease.waiters.add(wake);
    signal?.addEventListener('abort', wake, { once: true });
    try {
      await promise;
      signal?.throwIfAborted();
    } finally {
      clearTimeout(timer);
      lease.waiters.delete(wake);
      signal?.removeEventListener('abort', wake);
    }
  }
}
