import assert from 'node:assert/strict';
import { after, before, test } from 'node:test';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { PhotonImage } from '@cf-wasm/photon';
import { COGRangeCache } from '../src/lib/cog_range_cache.js';
import { makeCOG, TILE } from './helpers/cog_fixture.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const prefix = 'https://github.com/ramSeraph/fixtures/';
const files = new Map([
  ['masked.tif', makeCOG()], ['jpeg.tif', makeCOG({ jpeg: true })],
  ['plain.tif', makeCOG({ mask: false })],
  ['partial.tif', makeCOG({ mask: false, width: 384, height: 320 })],
  ['wrong-crs.tif', makeCOG({ epsg: 4326 })],
  ['zoom.tif', makeCOG({ jpeg: true, blockAlignment: 32 * 1024 })],
]);
let worker;
let requests = [];
let failNext = false;
let rateLimitNext = false;
let replaceOnRange;
let clockOffset = 0;

before(async () => {
  const bundle = await build({
    stdin: {
      contents: `import { Hono } from 'hono';
        import { registerCogRoutes } from './src/routes/cog.js';
        const now = Date.now;
        const app = new Hono();
        app.use('*', async (c, next) => {
          Date.now = () => now() + Number(c.req.header('test-clock-offset') || 0);
          if (c.req.header('test-disable-revalidation')) c.env = { TILE_CACHE_REVALIDATION: 'false' };
          await next();
        });
        registerCogRoutes(app, {error() {}, warn() {}});
        export default app;`,
      resolveDir: root,
    },
    bundle: true, format: 'esm', platform: 'browser', conditions: ['workerd'], target: 'es2022', write: false,
    external: ['node:async_hooks'],
    plugins: [{ name: 'wasm', setup(b) {
      b.onResolve({ filter: /^@cf-wasm\/photon$/ }, () => ({ path: root + 'node_modules/@cf-wasm/photon/dist/workerd.js' }));
      b.onResolve({ filter: /\.wasm$/ }, () => ({ path: './photon.wasm', external: true }));
      b.onResolve({ filter: /^module$/ }, () => ({ path: 'node:module', external: true }));
    } }],
  });
  worker = new Miniflare({
    compatibilityDate: '2024-11-01', compatibilityFlags: [
      'nodejs_compat', 'no_handle_cross_request_promise_resolution', 'cache_option_enabled', 'cache_no_cache_enabled',
    ],
    bindings: { TILE_CACHE_REVALIDATION: 'true' },
    modulesRoot: root,
    modules: [
      { type: 'ESModule', path: root + 'cog-test.mjs', contents: bundle.outputFiles[0].text },
      { type: 'CompiledWasm', path: root + 'photon.wasm', contents: await readFile(root + 'node_modules/@cf-wasm/photon/dist/lib/photon_rs_bg.wasm') },
    ],
    outboundService: async request => {
      const name = new URL(request.url).pathname.split('/').at(-1);
      if (replaceOnRange?.name === name && request.headers.has('range')) {
        files.set(name, replaceOnRange.bytes);
        replaceOnRange = undefined;
      }
      const bytes = files.get(name);
      requests.push({ name, method: request.method, range: request.headers.get('range'),
        ifMatch: request.headers.get('If-Match'), ifNoneMatch: request.headers.get('If-None-Match') });
      if (failNext) { failNext = false; return new Response('', { status: 502 }); }
      if (rateLimitNext && request.headers.has('range')) {
        rateLimitNext = false;
        return new Response('', { status: 429, headers: { 'Retry-After': '1' } });
      }
      if (!bytes) return new Response('', { status: 404 });
      const etag = '"' + createHash('sha256').update(bytes).digest('hex') + '"';
      if (request.method === 'HEAD') {
        if (request.headers.get('If-None-Match') === etag) return new Response(null, { status: 304, headers: { ETag: etag } });
        return new Response(null, { headers: { 'Content-Length': String(bytes.length), ETag: etag } });
      }
      if (request.headers.has('If-Match') && request.headers.get('If-Match') !== etag) return new Response(null, { status: 412 });
      const match = /^bytes=(\d+)-(\d+)$/.exec(request.headers.get('range'));
      assert.ok(match, 'COGs must be read with Range requests');
      const start = Number(match[1]), end = Math.min(Number(match[2]), bytes.length - 1);
      if (name === 'zoom.tif') await delay(40);
      return new Response(bytes.subarray(start, end + 1), { status: 206, headers: {
        'Content-Range': `bytes ${start}-${end}/${bytes.length}`, 'Content-Length': String(end - start + 1), ETag: etag,
      } });
    },
  });
});
after(async () => { await worker?.dispose(); });

function get(path, name, headers = {}) {
  return worker.dispatchFetch(`http://localhost${path}${path.includes('?') ? '&' : '?'}url=${encodeURIComponent(prefix + name)}`,
    { headers: { 'test-clock-offset': String(clockOffset), ...headers } });
}
function tilePath(z = TILE.z, x = TILE.x, y = TILE.y, format = 'png') { return `/cog-tiles/${z}/${x}/${y}?format=${format}`; }
async function pixels(response) {
  if (response.status !== 200) assert.fail(`HTTP ${response.status}: ${(await response.text()).slice(0, 200)}`);
  const image = PhotonImage.new_from_byteslice(new Uint8Array(await response.arrayBuffer()));
  try {
    assert.equal(image.get_width(), 256); assert.equal(image.get_height(), 256);
    return image.get_raw_pixels();
  } finally { image.free(); }
}
const at = (data, x, y = 128) => [...data.slice((y * 256 + x) * 4, (y * 256 + x) * 4 + 4)];

for (const name of ['masked.tif', 'jpeg.tif']) {
  test(`${name}: select overview, decode mask, and preserve PNG/WebP pixels in workerd`, async () => {
    const info = await get('/cog-info', name);
    assert.equal(info.status, 200);
    const metadata = await info.json();
    assert.deepEqual(metadata.size, { width: 512, height: 512 });
    assert.equal(metadata.imageCount, 4);
    const overviewResponse = await get(tilePath(), name);
    assert.equal(overviewResponse.headers.get('access-control-allow-origin'), '*');
    assert.equal(overviewResponse.headers.get('cache-control'), 'public, max-age=300, must-revalidate');
    const overview = await pixels(overviewResponse);
    assert.equal(at(overview, 20)[3], 0);
    const blue = at(overview, 128);
    assert.ok(blue[2] > 190 && blue[0] < 30 && blue[3] === 255);
    const detail = await pixels(await get(tilePath(TILE.z + 1, TILE.x * 2, TILE.y * 2), name));
    assert.equal(at(detail, 20)[3], 0);
    const red = at(detail, 200);
    assert.ok(red[0] > 190 && red[2] < 30 && red[3] === 255);
    requests = [];
    const webp = await pixels(await get(tilePath(TILE.z + 1, TILE.x * 2, TILE.y * 2, 'webp'), name));
    assert.deepEqual(webp, detail);
    assert.equal(requests.length, 0, 'warm tiles reuse cached ranges');
  });
}

test('unmasked imagery stays opaque and edge blocks are cropped to the image extent', async () => {
  const plain = await pixels(await get(tilePath(), 'plain.tif'));
  assert.deepEqual(at(plain, 20), [20, 60, 200, 255]);
  const partial = await pixels(await get(tilePath(), 'partial.tif'));
  assert.deepEqual(at(partial, 100, 100), [20, 60, 200, 255]);
  assert.equal(at(partial, 220, 100)[3], 0);
  assert.equal(at(partial, 100, 200)[3], 0);
});

test('COG metadata and tiles support conditional browser requests in workerd', async () => {
  for (const path of ['/cog-info', tilePath()]) {
    const url = `http://localhost${path}${path.includes('?') ? '&' : '?'}url=${encodeURIComponent(prefix + 'plain.tif')}`;
    const response = await worker.dispatchFetch(url);
    assert.equal(response.status, 200);
    const etag = response.headers.get('ETag');
    assert.match(etag, /^"[a-f0-9]{64}"$/);
    await response.arrayBuffer();
    const repeat = await worker.dispatchFetch(url, { headers: { 'If-None-Match': `W/${etag}` } });
    assert.equal(repeat.status, 304);
    assert.equal(await repeat.text(), '');
    assert.equal(repeat.headers.get('Access-Control-Allow-Origin'), '*');
    assert.equal(repeat.headers.get('Cache-Control'), path === '/cog-info' ? 'no-cache' : 'public, max-age=300, must-revalidate');
  }
});

test('out-of-coverage, whitelist, missing data, and CRS errors keep HTTP semantics', async () => {
  assert.equal((await get(tilePath(TILE.z, 0, 0), 'plain.tif')).status, 404);
  assert.equal((await worker.dispatchFetch('http://localhost/cog-info?url=https://example.com/x.tif')).status, 403);
  assert.equal((await worker.dispatchFetch('http://localhost/cog-info')).status, 400);
  assert.equal((await get('/cog-info', 'missing.tif')).status, 424);
  assert.equal((await get('/cog-info', 'wrong-crs.tif')).status, 400);
});

test('failed header reads can retry and completed headers are reused', async () => {
  files.set('retry.tif', files.get('plain.tif'));
  failNext = true;
  assert.equal((await get('/cog-info', 'retry.tif')).status, 424);
  requests = [];
  const responses = await Promise.all([get('/cog-info', 'retry.tif'), get('/cog-info', 'retry.tif')]);
  assert.ok(responses.every(response => response.status === 200));
  await Promise.all(responses.map(response => response.arrayBuffer()));
  requests = [];
  assert.equal((await get('/cog-info', 'retry.tif')).status, 200);
  assert.equal(requests.length, 0);
});

test('COG range reads recover from a rate limit through the shared request helper', async () => {
  files.set('rate-limit.tif', files.get('plain.tif'));
  requests = [];
  rateLimitNext = true;
  const response = await get('/cog-info', 'rate-limit.tif');
  assert.equal(response.status, 200);
  assert.equal((await response.json()).size.width, 512);
  const ranges = requests.filter(request => request.range);
  assert.deepEqual(ranges[0], ranges[1], 'retry the same byte range');
});

test('overlapping zoom requests survive cancellation and reuse completed ranges', { timeout: 15000 }, async () => {
  // Metadata fits in the first range; image and mask blocks require new I/O.
  const base = await worker.ready;
  const query = '&url=' + encodeURIComponent(prefix + 'zoom.tif');
  const info = await get('/cog-info', 'zoom.tif');
  assert.equal(info.status, 200);
  await info.arrayBuffer();
  requests = [];
  const pending = [];
  for (const zoom of [1, 0, 1]) {
    for (let i = 0; i < 8; i++) {
      const path = tilePath(TILE.z + zoom, TILE.x * 2 ** zoom + (zoom ? i % 2 : 0),
        TILE.y * 2 ** zoom + (zoom ? Math.floor(i / 2) % 2 : 0));
      const controller = new AbortController();
      const cancelled = zoom === 1 && i % 3 === 0;
      const timer = cancelled ? setTimeout(() => controller.abort(), 10) : undefined;
      pending.push((async () => {
        try {
          const response = await fetch(new URL(path + query, base), { signal: controller.signal });
          const data = await pixels(response);
          assert.ok(at(data, 200)[3] > 0);
        } catch (error) {
          if (!controller.signal.aborted) throw error;
        } finally { clearTimeout(timer); }
      })().then(() => null, error => error));
    }
    await delay(5);
  }
  const failures = (await Promise.all(pending)).filter(Boolean);
  assert.equal(failures.length, 0, failures.map(error => error.message).join('\n'));
  assert.ok(requests.some(request => request.range !== 'bytes=0-32767'), 'imagery needs fresh range reads');
  requests = [];
  await pixels(await get(tilePath(TILE.z + 1, TILE.x * 2, TILE.y * 2), 'zoom.tif'));
  assert.equal(requests.length, 0, 'completed ranges are shared with later requests');
});

test('COG replacements refresh pixels and metadata at the same URL after five minutes', async () => {
  const name = 'replace.tif';
  files.set(name, makeCOG());
  const original = await get(tilePath(), name);
  const oldEtag = original.headers.get('ETag');
  assert.deepEqual(at(await pixels(original), 128), [20, 60, 200, 255]);
  requests = [];
  clockOffset += 300_001;
  await pixels(await get(tilePath(), name));
  assert.equal(requests.length, 1, '304 HEAD retains all completed ranges');
  assert.equal(requests[0].method, 'HEAD');
  assert.ok(requests[0].ifNoneMatch);

  files.set(name, makeCOG({ mask: false, width: 384, height: 320, colors: [[60, 200, 20], [20, 200, 60]] }));
  requests = [];
  clockOffset += 300_001;
  const changed = await get(tilePath(), name, { 'If-None-Match': oldEtag });
  assert.notEqual(changed.headers.get('ETag'), oldEtag);
  const data = await pixels(changed);
  assert.deepEqual(at(data, 20, 100), [20, 200, 60, 255], 'new mask and imagery are used');
  assert.equal(at(data, 220, 100)[3], 0, 'new dimensions are used');
  const info = await (await get('/cog-info', name)).json();
  assert.deepEqual(info.size, { width: 384, height: 320 });
  assert.equal(info.imageCount, 2);
  assert.ok(requests.filter(request => request.range).every(request => request.ifMatch));
});

test('a replacement during rendering reopens TIFF directories, masks and imagery together', async () => {
  const name = 'mid-render.tif';
  files.set(name, makeCOG({ blockAlignment: 32 * 1024 }));
  const info = await (await get('/cog-info', name)).json();
  assert.equal(info.imageCount, 4);
  replaceOnRange = { name, bytes: makeCOG({ mask: false, blockAlignment: 32 * 1024,
    colors: [[60, 200, 20], [20, 200, 60]],
  }) };
  requests = [];
  const data = await pixels(await get(tilePath(), name));
  assert.deepEqual(at(data, 20), [20, 200, 60, 255]);
  assert.equal(requests.filter(request => request.method === 'HEAD').length, 1, 'mismatch triggers a fresh HEAD');
  assert.equal((await (await get('/cog-info', name)).json()).imageCount, 2);
  requests = [];
  await pixels(await get(tilePath(), name));
  assert.equal(requests.length, 0, 'retried ranges are reusable');
});

test('disabling revalidation restores legacy COG caching without HEAD checks', async () => {
  const name = 'rollback.tif';
  const headers = { 'test-disable-revalidation': 'true' };
  files.set(name, makeCOG());
  requests = [];
  const initial = await get(tilePath(), name, headers);
  assert.equal(initial.headers.get('Cache-Control'), 'max-age=86400000');
  assert.equal(initial.headers.get('ETag'), null);
  const data = await pixels(initial);
  assert.ok(requests.length > 0);
  assert.ok(requests.every(request => request.range && !request.ifMatch));
  clockOffset += 300_001;
  files.set(name, makeCOG({ mask: false }));
  requests = [];
  assert.deepEqual(await pixels(await get(tilePath(), name, headers)), data);
  assert.equal(requests.length, 0, 'legacy cache does not refresh');
});

test('range cache respects its byte cap and retries failures', async () => {
  const cache = new COGRangeCache(16);
  const local = cache.forRequest();
  const request = offset => ({ source: { url: new URL(prefix + 'cache.tif') }, offset, length: 8 });
  let calls = 0;
  const read = async () => { calls++; return new ArrayBuffer(8); };
  await Promise.all([local.fetch(request(0), read), local.fetch(request(0), read)]);
  assert.equal(calls, 1);
  await local.fetch(request(8), read);
  await local.fetch(request(0), read);
  await local.fetch(request(16), read);
  assert.equal(cache.byteLength, 16);
  await cache.forRequest().fetch(request(8), read);
  assert.equal(calls, 4);
  await assert.rejects(local.fetch(request(24), async () => { throw new Error('transient'); }));
  await local.fetch(request(24), read);
  assert.equal(calls, 5);
  assert.ok(cache.byteLength <= 16);
});

test('a failed request cannot hold up another request or evict its completed bytes', { timeout: 1000 }, async () => {
  const cache = new COGRangeCache(16);
  const request = { source: { url: new URL(prefix + 'cache.tif') }, offset: 0, length: 8 };
  let rejectFirst;
  const first = cache.forRequest().fetch(request, () => new Promise((resolve, reject) => { rejectFirst = reject; }));
  const failure = assert.rejects(first, /cancelled/);
  await delay(0);
  const bytes = Uint8Array.of(1, 2, 3, 4).buffer;
  assert.equal(await cache.forRequest().fetch(request, async () => bytes), bytes);
  rejectFirst(new Error('cancelled'));
  await failure;
  const cached = await cache.forRequest().fetch(request, () => assert.fail('completed bytes should be reused'));
  assert.equal(cached, bytes);
  assert.equal(cache.byteLength, 4, 'account for actual bytes, including short responses at EOF');
});
