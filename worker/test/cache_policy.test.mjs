import assert from 'node:assert/strict';
import { test } from 'node:test';
import { cachePolicy } from '../src/lib/cache_policy.js';

test('one period controls source refresh and browser tile freshness', () => {
  assert.deepEqual(cachePolicy(), { refreshMs: 3_600_000, revalidate: true,
    tileCacheControl: 'public, max-age=3600, must-revalidate' });
  assert.deepEqual(cachePolicy({ CACHE_REVALIDATION_SECONDS: '7200' }), {
    refreshMs: 7_200_000, revalidate: true, tileCacheControl: 'public, max-age=7200, must-revalidate',
  });
});

test('explicit zero selects rollback rather than disabling browser caching', () => {
  assert.deepEqual(cachePolicy({ CACHE_REVALIDATION_SECONDS: '0' }), {
    refreshMs: 0, revalidate: false, tileCacheControl: 'public, max-age=0, must-revalidate',
  });
});

test('malformed durations use defaults instead of disabling validation or overflowing', () => {
  for (const value of ['', ' ', '-1', '1.5', 'true', 'false', 'Infinity', '1e3', '9007199254740991']) {
    assert.deepEqual(cachePolicy({ CACHE_REVALIDATION_SECONDS: value }), cachePolicy(), value);
  }
});
