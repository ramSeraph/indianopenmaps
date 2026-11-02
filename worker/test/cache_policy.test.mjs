import assert from 'node:assert/strict';
import { test } from 'node:test';
import { cachePolicy } from '../src/lib/cache_policy.js';

test('cache settings default to hourly source refresh and five-minute browser tiles', () => {
  assert.deepEqual(cachePolicy(), { refreshMs: 3_600_000, revalidate: true,
    tileCacheControl: 'public, max-age=300, must-revalidate' });
  assert.deepEqual(cachePolicy({ CACHE_REVALIDATION_SECONDS: '7200', TILE_CACHE_MAX_AGE_SECONDS: '900' }), {
    refreshMs: 7_200_000, revalidate: true, tileCacheControl: 'public, max-age=900, must-revalidate',
  });
});

test('explicit zero disables refresh, while browser zero only disables fresh tile reuse', () => {
  assert.equal(cachePolicy({ CACHE_REVALIDATION_SECONDS: '0' }).revalidate, false);
  assert.deepEqual(cachePolicy({ TILE_CACHE_MAX_AGE_SECONDS: '0' }), {
    refreshMs: 3_600_000, revalidate: true, tileCacheControl: 'public, max-age=0, must-revalidate',
  });
});

test('malformed durations use defaults instead of disabling validation or overflowing', () => {
  for (const value of ['', ' ', '-1', '1.5', 'true', 'false', 'Infinity', '1e3', '9007199254740991']) {
    assert.deepEqual(cachePolicy({ CACHE_REVALIDATION_SECONDS: value, TILE_CACHE_MAX_AGE_SECONDS: value }), cachePolicy(), value);
  }
});
