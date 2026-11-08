import assert from 'node:assert/strict';
import { beforeEach, test } from 'node:test';
import { logger } from '../src/lib/logger.js';
import { fetchWithRetry, withRequestContext } from '../src/lib/request.js';
import app from '../src/index.js';

// Keep severity tests outside the independent 1% access sample.
beforeEach(t => { t.mock.method(Math, 'random', () => 0.5); });

function capture(t) {
  const rows = [];
  for (const method of ['error', 'warn', 'log', 'debug']) {
    t.mock.method(console, method, (...args) => rows.push({ method, args }));
  }
  return rows;
}

const request = new Request('https://example.test/');
for (const [logLevel, expected] of [
  [undefined, ['error']], ['invalid', ['error']], ['constructor', ['error']],
  [null, ['error']], [' OFF ', []], ['error', ['error']],
  ['warn', ['error', 'warn']], ['info', ['error', 'warn', 'log']],
  [' DEBUG ', ['error', 'warn', 'log', 'debug']],
]) {
  test(`log level ${String(logLevel)} selects the expected severity threshold`, async t => {
    const rows = capture(t);
    await withRequestContext(request, async () => {
      await Promise.resolve();
      logger.error('failure'); logger.warn('warning');
      logger.info('info'); logger.debug('debug');
    }, { logLevel });
    assert.deepEqual(rows.map(row => row.method), expected);
  });
}

test('overlapping requests retain log levels through retries and cache probes', async t => {
  const rows = capture(t);
  const started = Promise.withResolvers();
  const release = Promise.withResolvers();
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    if (++calls === 1) {
      started.resolve();
      await release.promise;
      return new Response(null, { status: 429, headers: { 'Retry-After': '0' } });
    }
    return new Response('ok');
  });
  const first = withRequestContext(request, () => fetchWithRetry('https://upstream.test/'), { logLevel: 'off' });
  await started.promise;
  await withRequestContext(request, () => fetchWithRetry('https://upstream.test/'), { logLevel: 'info' });
  release.resolve();
  assert.equal(await (await first).text(), 'ok');
  assert.equal(calls, 3);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].method, 'log');
  assert.equal(rows[0].args[0].status, 200);
});

test('unsampled successful, invalid and missing routes are silent by default', async t => {
  const rows = capture(t);
  for (const [path, status] of [['/api/routes', 200], ['/missing', 404], ['/cog-info', 400]]) {
    assert.equal((await app.request(path)).status, status);
  }
  assert.deepEqual(rows, []);
});

test('Worker bindings enable header diagnostics only at debug level', async t => {
  const rows = capture(t);
  for (const LOG_LEVEL of ['error', 'warn', 'info', 'debug', 'off']) {
    assert.equal((await app.request('/api/routes', {}, { LOG_LEVEL })).status, 200);
  }
  assert.equal(rows.length, 1);
  assert.equal(rows[0].method, 'debug');
  assert.equal(rows[0].args[0].message, 'Request headers');
  assert.ok(rows[0].args[0].requestId);
});

test('unhandled route errors obey off and retain failure details by default', async t => {
  const rows = capture(t);
  const ASSETS = { fetch() { throw new Error('Asset binding unavailable'); } };
  for (const LOG_LEVEL of ['off', 'error']) {
    const response = await app.request('/', {}, { LOG_LEVEL, ASSETS });
    assert.equal(response.status, 500);
    assert.equal(response.headers.get('Cache-Control'), 'no-store');
    assert.equal(await response.text(), 'Internal Server Error');
    assert.equal(rows.length, LOG_LEVEL === 'off' ? 0 : 1);
  }
  assert.equal(rows[0].args[0].event, 'request_error');
  assert.equal(rows[0].args[0].message, 'Asset binding unavailable');
});

test('proxy HTTP failures remain visible by default and can be switched off', async t => {
  const rows = capture(t);
  t.mock.method(globalThis, 'fetch', async () => new Response(null, { status: 500 }));
  const path = '/proxy?url=https://github.com/owner/repo/releases/download/data/file';
  assert.equal((await app.request(path, {}, { LOG_LEVEL: 'off' })).status, 500);
  assert.deepEqual(rows, []);
  assert.equal((await app.request(path)).status, 500);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].method, 'error');
  assert.equal(rows[0].args[0].status, 500);
});
