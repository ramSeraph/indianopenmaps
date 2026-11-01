import assert from 'node:assert/strict';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import MosaicHandler from '../src/lib/mosaic_handler.js';
import PMTilesHandler from '../src/lib/pmtiles_handler.js';
import { SOURCE_REFRESH_MS } from '../src/lib/refresh_cache.js';
import { makeArchive, metadata, mosaic, respond, tile } from './helpers/pmtiles_fixture.mjs';

function setup(t, revalidate = true) {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const warnings = [];
  const handler = new MosaicHandler(`https://example.test/${encodeURIComponent(t.name)}/mosaic.json`,
    'vector', 'pbf', { warn: message => warnings.push(message) }, false, { revalidate });
  return { handler, warnings, expire: () => t.mock.timers.tick(SOURCE_REFRESH_MS) };
}

test('mosaic 304 keeps its resolver and renews freshness, including weak validators', async t => {
  const { handler, expire } = setup(t);
  const fetch = t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(options.cache, 'no-cache');
    if (!options.headers.has('If-None-Match')) return Response.json(mosaic, { headers: { ETag: 'W/"v1"' } });
    assert.equal(options.headers.get('If-None-Match'), 'W/"v1"');
    return new Response(null, { status: 304 });
  });
  const first = await handler.manifest.get();
  assert.equal((await handler.getConfig()).name, metadata.name);
  assert.equal(fetch.mock.callCount(), 1);
  expire();
  assert.equal((await handler.manifest.get()).mosaic, first.mosaic);
  await handler.getConfig();
  assert.equal(fetch.mock.callCount(), 2);
});

test('disabled revalidation keeps the original mosaic without periodic upstream requests', async t => {
  const { handler, expire } = setup(t, false);
  const fetch = t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(options.cache, undefined);
    return Response.json(mosaic, { headers: { ETag: '"original"' } });
  });
  const first = await handler.manifest.get();
  expire();
  expire();
  assert.equal(await handler.manifest.get(), first);
  assert.equal(fetch.mock.callCount(), 1);
});

test('updated manifest swaps source selection, bounds, and metadata together', async t => {
  const { handler, expire } = setup(t);
  let version = 1;
  const updated = structuredClone(mosaic);
  updated.metadata.name = 'New mosaic';
  updated.header.min_lon_e7 = 0;
  updated.slices = { 'new.pmtiles': { header: updated.header } };
  const changedTile = new Uint8Array([0x1a, 4]);
  const bytes = makeArchive({ tileBytes: changedTile });
  const reads = [];
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    reads.push(url);
    if (url.endsWith('mosaic.json')) return Response.json(version === 1 ? mosaic : updated,
      { headers: { ETag: `"v${version}"` } });
    return respond(url, options, url.endsWith('new.pmtiles') ? bytes : undefined);
  });
  assert.deepEqual(new Uint8Array((await handler.getTile(0, 0, 0))[0].data), tile);
  version = 2;
  expire();
  const config = await handler.getConfig();
  assert.equal(config.name, 'New mosaic');
  assert.deepEqual(config.bounds, [0, -90, 180, 90]);
  const before = reads.length;
  assert.deepEqual(new Uint8Array((await handler.getTile(0, 0, 0))[0].data), changedTile);
  assert.ok(reads.slice(before).every(url => url.endsWith('new.pmtiles')));
});

for (const failure of ['http', 'json', 'schema']) {
  test(`failed ${failure} refresh keeps a valid mosaic and retries after a bounded backoff`, async t => {
    const { handler, warnings, expire } = setup(t);
    let failing = false;
    const fetch = t.mock.method(globalThis, 'fetch', async (url, options) => {
      assert.equal(options.headers.get('If-None-Match'), failing ? '"old"' : null);
      if (!failing) return Response.json(mosaic, { headers: { ETag: '"old"' } });
      if (failure === 'http') return new Response('', { status: 429, headers: { 'Retry-After': '60' } });
      if (failure === 'json') return new Response('broken JSON', { headers: { ETag: '"broken"' } });
      return Response.json({ version: 999 }, { headers: { ETag: '"broken"' } });
    });
    const original = await handler.manifest.get();
    failing = true;
    expire();
    assert.equal((await handler.manifest.get()).mosaic, original.mosaic);
    await handler.getConfig();
    assert.equal(fetch.mock.callCount(), 2);
    assert.equal(warnings.length, 1);
    t.mock.timers.tick(failure === 'http' ? 60_000 : 30_000);
    await handler.getConfig();
    assert.equal(fetch.mock.callCount(), 3, 'retry with the last valid ETag');
  });
}

test('mosaics without an ETag are downloaded again after expiry', async t => {
  const { handler, expire } = setup(t);
  let document = mosaic;
  const fetch = t.mock.method(globalThis, 'fetch', async (url, options) => {
    assert.equal(options.headers.has('If-None-Match'), false);
    return Response.json(document);
  });
  await handler.getConfig();
  document = { ...mosaic, metadata: { ...metadata, name: 'Changed without ETag' } };
  expire();
  assert.equal((await handler.getConfig()).name, document.metadata.name);
  assert.equal(fetch.mock.callCount(), 2);
});

for (const lateResponse of ['304', '200']) {
  test(`a late ${lateResponse} cannot replace a newer completed mosaic refresh`, async t => {
    const { handler, expire } = setup(t);
    const stalled = Promise.withResolvers();
    let reads = 0;
    t.mock.method(globalThis, 'fetch', async () => {
      if (++reads === 1) return Response.json(mosaic, { headers: { ETag: '"old"' } });
      if (reads === 2) return stalled.promise;
      return Response.json({ ...mosaic, metadata: { ...metadata, name: 'New' } }, { headers: { ETag: '"new"' } });
    });
    await handler.getConfig();
    expire();
    const first = handler.getConfig();
    assert.equal((await handler.getConfig()).name, 'New');
    stalled.resolve(lateResponse === '304' ? new Response(null, { status: 304 }) : Response.json(mosaic));
    assert.equal((await first).name, 'New');
    assert.equal((await handler.getConfig()).name, 'New');
  });
}

test('a cancelled refresh cannot install a manifest or delay the next refresh', async t => {
  const { handler, expire } = setup(t);
  const controller = new AbortController();
  let cancel = false;
  t.mock.method(globalThis, 'fetch', async () => {
    if (cancel) controller.abort();
    return Response.json(mosaic, { headers: { ETag: cancel ? '"new"' : '"old"' } });
  });
  await handler.getConfig();
  expire();
  cancel = true;
  await assert.rejects(handler.manifest.get(controller.signal), { name: 'AbortError' });
  cancel = false;
  assert.equal((await handler.manifest.get()).etag, '"old"');
});

test('single PMTiles TileJSON refresh reads metadata and the replacement header consistently', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const handler = new PMTilesHandler('https://example.test/metadata-refresh.pmtiles', 'vector', 'pbf', console, false, { revalidate: true });
  let version = 1;
  const newer = makeArchive({ jsonMetadata: { ...metadata, name: 'New and longer metadata', vector_layers: [{ id: 'new-layer' }] } });
  new DataView(newer.buffer).setInt32(102, 0, true);
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    const response = respond(url, options, version === 1 ? undefined : newer);
    response.headers.set('ETag', `"v${version}"`);
    return response;
  });
  assert.equal((await handler.getConfig()).name, metadata.name);
  version = 2;
  assert.equal((await handler.getConfig()).name, metadata.name, 'metadata is fresh for five minutes');
  t.mock.timers.tick(SOURCE_REFRESH_MS);
  const config = await handler.getConfig();
  assert.equal(config.name, 'New and longer metadata');
  assert.deepEqual(config.vector_layers, [{ id: 'new-layer' }]);
  assert.deepEqual(config.bounds, [0, -90, 180, 90]);
});

test('workerd refreshes manifests and archive ETags without unsupported cache modes', { timeout: 15000 }, async () => {
  const root = fileURLToPath(new URL('../', import.meta.url));
  const bundle = await build({
    stdin: {
      contents: `import MosaicHandler from './src/lib/mosaic_handler.js';
        import { conditionalResponse, TILE_CACHE_CONTROL } from './src/lib/http_cache.js';
        const handler = new MosaicHandler('https://example.test/workerd-refresh/mosaic.json', 'vector', 'pbf', console, false, {revalidate: true});
        let now = 0; Date.now = () => now;
        export default { async fetch(request) {
          now = Number(request.headers.get('test-time') || 0);
          const [tile, mimeType] = await handler.getTile(0, 0, 0);
          return conditionalResponse(request, new Uint8Array(tile.data), {contentType: mimeType, cacheControl: TILE_CACHE_CONTROL});
        } };`,
      resolveDir: root,
    },
    bundle: true, format: 'esm', platform: 'browser', conditions: ['workerd'], target: 'es2022', write: false,
    external: ['node:async_hooks'],
  });
  let version = 1;
  const validators = [];
  const updatedTile = new Uint8Array([0x1a, 8]);
  const worker = new Miniflare({
    compatibilityDate: '2024-11-01',
    compatibilityFlags: ['nodejs_compat', 'no_handle_cross_request_promise_resolution', 'cache_option_enabled', 'cache_no_cache_enabled'],
    modules: true, script: bundle.outputFiles[0].text,
    outboundService: request => {
      assert.equal(request.headers.get('Cache-Control'), 'no-cache');
      if (request.url.endsWith('mosaic.json')) {
        validators.push(request.headers.get('If-None-Match'));
        return validators.length === 1 ? Response.json(mosaic, { headers: { ETag: 'W/"manifest"' } })
          : new Response(null, { status: 304 });
      }
      const response = respond(request.url, { headers: request.headers },
        makeArchive({ tileBytes: version === 1 ? tile : updatedTile }));
      response.headers.set('ETag', `"archive-${version}"`);
      return response;
    },
  });
  try {
    const url = 'http://localhost/tile';
    const first = await worker.dispatchFetch(url);
    assert.equal(first.status, 200);
    const etag = first.headers.get('ETag');
    assert.deepEqual(new Uint8Array(await first.arrayBuffer()), tile);
    const unchanged = await worker.dispatchFetch(url, { headers: { 'test-time': String(SOURCE_REFRESH_MS), 'If-None-Match': etag } });
    assert.equal(unchanged.status, 304);
    assert.deepEqual(validators, [null, 'W/"manifest"']);
    version = 2;
    const updated = await worker.dispatchFetch(url, { headers: { 'test-time': String(SOURCE_REFRESH_MS + 1), 'If-None-Match': etag } });
    assert.equal(updated.status, 200);
    assert.notEqual(updated.headers.get('ETag'), etag);
    assert.deepEqual(new Uint8Array(await updated.arrayBuffer()), updatedTile);
  } finally {
    await worker.dispose();
  }
});
