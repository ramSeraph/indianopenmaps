import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Hono } from 'hono';
import { PhotonImage } from '@cf-wasm/photon';
import { conditionalResponse, TILE_CACHE_CONTROL } from '../src/lib/http_cache.js';
import { ifNoneMatchMatches } from '../src/lib/http_etag.js';
import { registerTileRoutes } from '../src/routes/tiles.js';
import routes from '../src/routes/listing.json' with { type: 'json' };
import { DEFAULT_SOURCE_REFRESH_MS as SOURCE_REFRESH_MS } from '../src/lib/cache_policy.js';
import { makeArchive, metadata, mosaic, respond } from './helpers/pmtiles_fixture.mjs';
import { fetchTileJSON, tileCacheUrl } from '../static/js/tile_cache.js';
import { registerStaticRoutes } from '../src/routes/static.js';

test('GET validators accept weak tags, lists, and commas inside opaque tags', () => {
  for (const value of ['*', ' "abc" ', 'W/"abc"', '"other", W/"abc"', '"has,a,comma", "abc"']) {
    assert.ok(ifNoneMatchMatches(value, '"abc"'), value);
  }
  assert.ok(ifNoneMatchMatches('"other", W/"a,b"', '"a,b"'));
  for (const value of [null, 'abc', '"ABC"', '"different"', '"abc" trailing', '"nested, W/"abc"']) {
    assert.equal(ifNoneMatchMatches(value, '"abc"'), false, value);
  }
});

test('conditional responses hash only the actual bytes and keep headers on 304/HEAD', async () => {
  const options = { contentType: 'image/png', cacheControl: TILE_CACHE_CONTROL };
  const request = new Request('https://example.test/tile');
  const first = await conditionalResponse(request, new Uint8Array([1, 2]), options);
  const etag = first.headers.get('ETag');
  assert.match(etag, /^"[a-f0-9]{64}"$/);
  const stored = new Uint8Array([9, 1, 2, 9]);
  const unchanged = await conditionalResponse(new Request(request, { headers: { 'If-None-Match': `"other", W/${etag}` } }),
    stored.subarray(1, 3), options);
  assert.equal(unchanged.status, 304);
  assert.equal((await unchanged.arrayBuffer()).byteLength, 0);
  assert.equal(unchanged.headers.get('ETag'), etag);
  assert.equal(unchanged.headers.get('Cache-Control'), TILE_CACHE_CONTROL);
  assert.equal(unchanged.headers.get('Access-Control-Allow-Origin'), '*');
  assert.equal(unchanged.headers.get('Access-Control-Expose-Headers'), 'ETag');
  const changed = await conditionalResponse(new Request(request, { headers: { 'If-None-Match': etag } }),
    new Uint8Array([1, 3]), options);
  assert.equal(changed.status, 200);
  assert.notEqual(changed.headers.get('ETag'), etag);
  const head = await conditionalResponse(new Request(request, { method: 'HEAD' }), new Uint8Array([1, 2]), options);
  assert.equal(head.headers.get('ETag'), etag);
  assert.equal((await head.arrayBuffer()).byteLength, 0);
});

function app(enabled = true, env = {}) {
  const app = new Hono();
  app.use('*', async (c, next) => {
    c.env = { CACHE_REVALIDATION_SECONDS: enabled ? '3600' : '0', ...env };
    await next();
  });
  registerTileRoutes(app, () => 'https://example.test', console);
  registerStaticRoutes(app);
  return app;
}

test('PMTiles routes revalidate TileJSON and tile bytes after an archive replacement', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const server = app();
  const prefix = Object.entries(routes).find(([, r]) => r.handlertype === 'pmtiles' && r.type !== 'raster')[0];
  let version = 1;
  const updated = makeArchive({ jsonMetadata: { ...metadata, name: 'New archive' }, tileBytes: new Uint8Array([0x1a, 5]) });
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    const response = respond(url, options, version === 1 ? undefined : updated);
    response.headers.set('ETag', `"archive-${version}"`);
    return response;
  });
  const original = new Map();
  for (const [path, policy] of [['tiles.json', 'no-cache'], ['0/0/0.pbf', TILE_CACHE_CONTROL]]) {
    const first = await server.request(prefix + path);
    assert.equal(first.status, 200);
    assert.equal(first.headers.get('Cache-Control'), policy);
    original.set(path, first.headers.get('ETag'));
    const repeat = await server.request(prefix + path, { headers: { 'If-None-Match': first.headers.get('ETag') } });
    assert.equal(repeat.status, 304);
    assert.equal(await repeat.text(), '');
    assert.equal(repeat.headers.get('Access-Control-Allow-Origin'), '*');
  }
  version = 2;
  t.mock.timers.tick(300_001);
  assert.equal((await (await server.request(prefix + 'tiles.json')).json()).name, metadata.name,
    'the default metadata lifetime now exceeds five minutes');
  t.mock.timers.tick(SOURCE_REFRESH_MS - 300_001);
  for (const [path, etag] of original) {
    const response = await server.request(prefix + path, { headers: { 'If-None-Match': etag } });
    assert.equal(response.status, 200);
    assert.notEqual(response.headers.get('ETag'), etag);
    if (path === 'tiles.json') assert.equal((await response.json()).name, 'New archive');
    else assert.deepEqual(new Uint8Array(await response.arrayBuffer()), new Uint8Array([0x1a, 5]));
  }
  const missing = await server.request(prefix + '1/0/0.pbf', { headers: { 'If-None-Match': '*' } });
  assert.equal(missing.status, 404, 'a validator must not turn a missing tile into 304');
  assert.equal(missing.headers.get('ETag'), null);
  assert.equal(missing.headers.get('Cache-Control'), 'no-store');
});

for (const kind of ['pmtiles', 'mosaic']) test(`${kind}: custom source period and browser max-age are independent`, async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const server = app(true, { CACHE_REVALIDATION_SECONDS: '10', TILE_CACHE_MAX_AGE_SECONDS: '900' });
  const prefix = Object.entries(routes).find(([, r]) => r.handlertype === kind && r.type !== 'raster')[0];
  let version = 1;
  const fetch = t.mock.method(globalThis, 'fetch', async (url, options) => {
    const name = `Version ${version}`;
    const response = url.endsWith('.json') ? Response.json({ ...mosaic, metadata: { ...metadata, name } })
      : respond(url, options, makeArchive({ jsonMetadata: { ...metadata, name } }));
    response.headers.set('ETag', `"custom-${version}"`);
    return response;
  });
  assert.equal((await (await server.request(prefix + 'tiles.json')).json()).name, 'Version 1');
  const reads = fetch.mock.callCount();
  version = 2;
  t.mock.timers.tick(9999);
  assert.equal((await (await server.request(prefix + 'tiles.json')).json()).name, 'Version 1');
  assert.equal(fetch.mock.callCount(), reads);
  t.mock.timers.tick(1);
  assert.equal((await (await server.request(prefix + 'tiles.json')).json()).name, 'Version 2');
  const response = await server.request(prefix + '0/0/0.pbf');
  assert.equal(response.status, 200);
  assert.equal(response.headers.get('Cache-Control'), 'public, max-age=900, must-revalidate');
  const cached = await server.request(prefix + '0/0/0.pbf', { headers: { 'If-None-Match': response.headers.get('ETag') } });
  assert.equal(cached.status, 304);
  assert.equal(cached.headers.get('Cache-Control'), 'public, max-age=900, must-revalidate');
});

test('WebP and converted PNG responses have distinct validators', async t => {
  const server = app();
  const prefix = Object.entries(routes).find(([, r]) => r.type === 'raster' && r.handlertype !== 'mosaic' && (!r.tilesuffix || r.tilesuffix === 'webp'))[0];
  const image = new PhotonImage(new Uint8Array([255, 0, 0, 255]), 1, 1);
  const webp = image.get_bytes_webp();
  image.free();
  const bytes = makeArchive({ tileBytes: webp });
  new DataView(bytes.buffer).setUint8(99, 4); // WebP tile type
  t.mock.method(globalThis, 'fetch', async (url, options) => respond(url, options, bytes));
  const original = await server.request(prefix + '0/0/0.webp');
  const converted = await server.request(prefix + '0/0/0.png', { headers: { 'If-None-Match': original.headers.get('ETag') } });
  assert.equal(converted.status, 200);
  assert.equal(converted.headers.get('Content-Type'), 'image/png');
  assert.notEqual(converted.headers.get('ETag'), original.headers.get('ETag'));
  const repeat = await server.request(prefix + '0/0/0.png', { headers: { 'If-None-Match': converted.headers.get('ETag') } });
  assert.equal(repeat.status, 304);
});

test('viewer migration bypasses old metadata and tile URLs while preserving templates and COG parameters', async t => {
  const configBefore = globalThis.IOMAPS_CONFIG;
  globalThis.IOMAPS_CONFIG = { tileCacheRevalidation: true };
  t.after(() => { globalThis.IOMAPS_CONFIG = configBefore; });
  const template = 'https://example.test/data/{z}/{x}/{y}.pbf';
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(url, '/data/tiles.json?iomaps-cache=2');
    assert.equal(options.cache, 'no-cache');
    return Response.json({ tiles: [template], bounds: [0, 0, 1, 1] });
  });
  const config = await fetchTileJSON('/data/tiles.json');
  assert.equal(config.tiles[0], template + '?iomaps-cache=2');
  assert.deepEqual(config.bounds, [0, 0, 1, 1]);
  assert.equal(tileCacheUrl(config.tiles[0]), config.tiles[0], 'migration stays stable');
  const cog = '/cog-tiles/{z}/{x}/{y}?url=https%3A%2F%2Fgithub.com%2Fa.tif%3Fx%3D1%26y%3D2#fragment';
  assert.equal(tileCacheUrl(cog), cog.replace('#fragment', '&iomaps-cache=2#fragment'));
});

test('the deployment switch disables ETags, short lifetimes, and viewer migration', async t => {
  const server = app(false);
  const prefix = Object.entries(routes).find(([, r]) => r.handlertype === 'pmtiles' && r.type !== 'raster')[0];
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.notEqual(options.cache, 'no-cache', 'periodic upstream revalidation is disabled');
    return respond(url, options);
  });
  for (const path of ['tiles.json', '0/0/0.pbf']) {
    const response = await server.request(prefix + path, { headers: { 'If-None-Match': '*' } });
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('ETag'), null);
    assert.equal(response.headers.get('Cache-Control'), 'max-age=86400000');
  }
  const config = await server.request('/api/viewer-config.js');
  assert.match(await config.text(), /"tileCacheRevalidation":false/);
  assert.equal(config.headers.get('Cache-Control'), 'no-store');
  const configBefore = globalThis.IOMAPS_CONFIG;
  globalThis.IOMAPS_CONFIG = { tileCacheRevalidation: false };
  t.after(() => { globalThis.IOMAPS_CONFIG = configBefore; });
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(url, '/data/tiles.json');
    assert.equal(options, undefined);
    return Response.json({ tiles: ['/data/{z}/{x}/{y}.pbf'] });
  });
  assert.deepEqual((await fetchTileJSON('/data/tiles.json')).tiles, ['/data/{z}/{x}/{y}.pbf']);
});
