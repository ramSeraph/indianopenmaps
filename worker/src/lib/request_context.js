import { AsyncLocalStorage } from 'node:async_hooks';

// Shared by retries, cache-only probes and logging. Never keep request settings
// in mutable module globals: concurrent requests can have different bindings.
export const requestContext = new AsyncLocalStorage();
