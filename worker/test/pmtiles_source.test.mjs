import assert from 'node:assert/strict';
import { test } from 'node:test';
import { EtagMismatch } from 'pmtiles';
import { PMTilesSource } from '../src/lib/pmtiles_source.js';
import PMTilesHandler from '../src/lib/pmtiles_handler.js';
import MosaicHandler from '../src/lib/mosaic_handler.js';
import { UpstreamHttpError } from '../src/lib/errors.js';
import { metadata, tile, respond } from './helpers/pmtiles_fixture.mjs';

const url = 'https://example.test/archive.pmtiles';

test('range reads preserve cancellation, ETag, and cache metadata', async t => {
  const source = new PMTilesSource(url);
  const controller = new AbortController();
  const fetch = t.mock.method(globalThis, 'fetch', async (input, options) => {
    assert.equal(input, url);
    assert.equal(options.headers.get('Range'), 'bytes=4-5');
    assert.equal(options.signal, controller.signal);
    return new Response(tile, { status: 206, headers: {
      ETag: 'fixture', 'Cache-Control': 'max-age=60', Expires: 'Wed, 28 Oct 2026 10:00:00 GMT',
    } });
  });
  const result = await source.getBytes(4, 2, controller.signal, 'fixture');
  assert.equal(source.getKey(), url);
  assert.deepEqual(new Uint8Array(result.data), tile);
  assert.equal(result.etag, 'fixture');
  assert.equal(result.cacheControl, 'max-age=60');
  assert.equal(result.expires, 'Wed, 28 Oct 2026 10:00:00 GMT');
  assert.equal(fetch.mock.callCount(), 1);
});

test('a small archive retries the initial 416 with its actual length', async t => {
  const ranges = [];
  t.mock.method(globalThis, 'fetch', async (input, options) => {
    ranges.push(options.headers.get('Range'));
    if (ranges.length === 1) return new Response('', { status: 416, headers: { 'Content-Range': 'bytes */2' } });
    assert.equal(options.cache, 'reload');
    return new Response(tile, { status: 206 });
  });
  const result = await new PMTilesSource(url).getBytes(0, 16384);
  assert.deepEqual(new Uint8Array(result.data), tile);
  assert.deepEqual(ranges, ['bytes=0-16383', 'bytes=0-1']);
});

test('ETag mismatches and noninitial 416 responses invalidate cached PMTiles directories', async t => {
  for (const status of [206, 416]) {
    const source = new PMTilesSource(url);
    const fetch = t.mock.method(globalThis, 'fetch', async (input, options) => {
      if (fetch.mock.callCount() === 0) return new Response('', { status, headers: { ETag: 'new' } });
      assert.equal(options.cache, 'reload');
      return new Response(tile, { status: 206, headers: { ETag: 'new' } });
    });
    await assert.rejects(source.getBytes(4, 2, undefined, 'old'), EtagMismatch);
    assert.equal((await source.getBytes(4, 2)).etag, 'new');
    fetch.mock.restore();
  }
});

test('weak ETags do not trigger mismatches', async t => {
  t.mock.method(globalThis, 'fetch', async () => new Response(tile, { status: 206, headers: { ETag: 'W/"weak"' } }));
  const result = await new PMTilesSource(url).getBytes(4, 2, undefined, 'strong');
  assert.equal(result.etag, undefined);
  assert.deepEqual(new Uint8Array(result.data), tile);
});

test('HTTP 200 cannot silently download an entire archive when ranges are ignored', async t => {
  for (const contentLength of [null, '1000000']) {
    let cancelled = false;
    const fetch = t.mock.method(globalThis, 'fetch', async () => new Response(
      new ReadableStream({ cancel() { cancelled = true; } }),
      { headers: contentLength ? { 'Content-Length': contentLength } : {} },
    ));
    await assert.rejects(new PMTilesSource(url).getBytes(4, 2), /HTTP Byte Serving/);
    assert.ok(cancelled);
    fetch.mock.restore();
  }
  t.mock.method(globalThis, 'fetch', async () => new Response(tile, { headers: { 'Content-Length': '2' } }));
  assert.deepEqual(new Uint8Array((await new PMTilesSource(url).getBytes(0, 2)).data), tile);
});

test('rate limits preserve their status and delay, even if an error page has a different ETag', async t => {
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response('', {
    status: 429, headers: { 'Retry-After': '60', ETag: 'error-page' },
  }));
  await assert.rejects(new PMTilesSource(url).getBytes(4, 2, undefined, 'archive'), error => {
    assert.ok(error instanceof UpstreamHttpError);
    assert.equal(error.upstreamStatus, 429);
    assert.equal(error.statusCode, 503);
    assert.equal(error.retryAfter, '60');
    return true;
  });
  assert.equal(fetch.mock.callCount(), 1);
});

for (const Handler of [PMTilesHandler, MosaicHandler]) {
  const suffix = Handler === PMTilesHandler ? 'archive.pmtiles' : 'mosaic.json';

  test(`${Handler.name}: manifest, header, metadata, and tile reads recover from 429`, async t => {
    const handlerUrl = `https://example.test/retry/${Handler.name}/${suffix}`;
    const handler = new Handler(handlerUrl, 'vector', 'pbf', console, false);
    const calls = new Map();
    t.mock.method(globalThis, 'fetch', async (input, options) => {
      const key = `${input} ${options?.headers?.get('Range')}`;
      calls.set(key, (calls.get(key) ?? 0) + 1);
      if (calls.get(key) === 1) return new Response('', { status: 429, headers: { 'Retry-After': '0' } });
      return respond(input, options);
    });
    assert.equal((await handler.getConfig()).name, metadata.name);
    const [result, mimeType] = await handler.getTile(0, 0, 0);
    assert.deepEqual(new Uint8Array(result.data), tile);
    assert.equal(mimeType, 'application/vnd.mapbox-vector-tile');
    assert.ok([...calls.values()].every(count => count === 2), 'each read succeeds on its retry');
    if (Handler === MosaicHandler) {
      assert.ok([...calls.keys()].some(key => key.startsWith(handlerUrl)));
      assert.ok([...calls.keys()].some(key => key.includes('/part.pmtiles bytes=')));
    }
    const before = new Map(calls);
    await handler.getTile(0, 0, 0);
    assert.equal([...calls.values()].reduce((a, b) => a + b), [...before.values()].reduce((a, b) => a + b) + 1,
      'warm reads reuse resolved headers and directories');
  });

  test(`${Handler.name}: an upstream failure does not poison later initialization`, async t => {
    const handler = new Handler(`https://example.test/recover/${Handler.name}/${suffix}`, 'vector', 'pbf', console, false);
    let limited = true;
    t.mock.method(globalThis, 'fetch', async (input, options) => limited
      ? new Response('', { status: 429, headers: { 'Retry-After': '60' } })
      : respond(input, options));
    await assert.rejects(handler.getConfig(), UpstreamHttpError);
    limited = false;
    assert.equal((await handler.getConfig()).name, metadata.name);
  });
}
