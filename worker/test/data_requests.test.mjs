import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Hono } from 'hono';
import { registerProxyRoutes } from '../src/routes/proxy.js';
import STACHandler from '../src/lib/stac_handler.js';

const url = 'https://github.com/owner/repo/releases/download/data/archive.parquet';
const logger = { info() {}, error() { assert.fail('successful retries must not log an error'); } };

test('proxy retries preserve range and cache options and return the successful body', async t => {
  const app = new Hono();
  registerProxyRoutes(app, logger);
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async (input, options) => {
    assert.equal(input, url);
    assert.equal(options.headers.Range, 'bytes=4-5');
    assert.deepEqual(options.cf, { cacheTtl: -1, cacheEverything: false });
    if (++calls === 1) return new Response('', { status: 429, headers: { 'Retry-After': '0' } });
    return new Response('ok', { status: 206, headers: { 'Content-Range': 'bytes 4-5/100', 'Content-Length': '2' } });
  });
  const response = await app.request(`/proxy?url=${encodeURIComponent(url)}`, { headers: { Range: 'bytes=4-5' } });
  assert.equal(response.status, 206);
  assert.equal(response.headers.get('Content-Range'), 'bytes 4-5/100');
  assert.equal(response.headers.get('Content-Length'), '2');
  assert.equal(await response.text(), 'ok');
  assert.equal(calls, 2);
});

test('proxy passes long Retry-After values and upstream status to its client', async t => {
  const app = new Hono();
  registerProxyRoutes(app, logger);
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response('', {
    status: 429, headers: { 'Retry-After': '120' },
  }));
  const response = await app.request(`/proxy?url=${encodeURIComponent(url)}`);
  assert.equal(response.status, 429);
  assert.equal(response.headers.get('Retry-After'), '120');
  assert.equal(response.headers.get('Access-Control-Expose-Headers'), 'Retry-After');
  assert.equal(response.headers.get('Cache-Control'), 'no-store');
  assert.equal(fetch.mock.callCount(), 1);
});

test('STAC retries its download before passing the successful bytes to the Parquet reader', async t => {
  const handler = new STACHandler({}, logger);
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async input => {
    assert.equal(input, url);
    if (++calls === 1) return new Response('', { status: 429, headers: { 'Retry-After': '0' } });
    return new Response('parquet fixture');
  });
  handler.parquetRead = async ({ file, onComplete }) => {
    assert.equal(new TextDecoder().decode(file), 'parquet fixture');
    onComplete([['fixture-item']]);
  };
  const items = await handler.readGeoparquet(url);
  assert.equal(items[0].id, 'fixture-item');
  assert.equal(calls, 2);
});
