import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setImmediate as flush } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { bytesToHeader } from 'pmtiles';
import PMTilesHandler from '../src/lib/pmtiles_handler.js';
import MosaicHandler from '../src/lib/mosaic_handler.js';
import { archive, makeArchive, metadata, tile, respond } from './helpers/pmtiles_fixture.mjs';

const headerRange = 'bytes=0-16383';
const header = bytesToHeader(archive.buffer);
const metadataRange = `bytes=${header.jsonMetadataOffset}-${header.jsonMetadataOffset + header.jsonMetadataLength - 1}`;
const leafArchive = makeArchive({ leafDirectory: true });
const leafHeader = bytesToHeader(leafArchive.buffer);
const leafRange = `bytes=${leafHeader.leafDirectoryOffset}-${leafHeader.leafDirectoryOffset + leafHeader.leafDirectoryLength - 1}`;

function throttleFirst(t, rangeToThrottle, bytes = archive) {
  const requests = [];
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    const range = options.headers?.get('Range');
    requests.push({ url, range });
    if (range === rangeToThrottle && requests.filter(request => request.range === range).length === 1) {
      return new Response('', { status: 429, headers: { 'Retry-After': '1' } });
    }
    return respond(url, options, bytes);
  });
  return requests;
}

for (const [part, Handler, range, suffix] of [
  ['header', PMTilesHandler, headerRange, 'archive.pmtiles'],
  ['metadata', PMTilesHandler, metadataRange, 'archive.pmtiles'],
  ['manifest', MosaicHandler, undefined, 'mosaic.json'],
]) {
  test(`a late ${part} 429 uses the completed cache immediately, even with Retry-After 60`, { timeout: 2000 }, async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const handler = new Handler(`https://example.test/immediate-${part}/${suffix}`, 'vector', 'pbf', console, false);
    const inFlight = Promise.withResolvers();
    const failure = Promise.withResolvers();
    let stalled = false;
    const fetch = t.mock.method(globalThis, 'fetch', async (url, options) => {
      if (!stalled && options.headers?.get('Range') === range) {
        stalled = true;
        inFlight.resolve();
        return failure.promise;
      }
      return respond(url, options);
    });
    const first = handler.getConfig();
    await inFlight.promise;
    assert.equal((await handler.getConfig()).name, metadata.name);
    const completed = fetch.mock.callCount();
    failure.resolve(new Response('', { status: 429, headers: { 'Retry-After': '60' } }));
    assert.equal((await first).name, metadata.name, 'a cache hit completes without advancing the clock');
    assert.equal(fetch.mock.callCount(), completed, 'no upstream request is needed');
  });
}

for (const [part, range] of [['header', headerRange], ['metadata', metadataRange]]) {
  test(`initial ${part} retry reuses initialization completed by another tile request`, { timeout: 2000 }, async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const handler = new PMTilesHandler(`https://example.test/concurrent-${part}.pmtiles`, 'vector', 'pbf', console, false);
    const requests = throttleFirst(t, range);

    const first = handler.getTile(0, 0, 0);
    await flush();
    assert.equal(requests.filter(request => request.range === range).length, 1, 'cache misses must not fetch early');
    const [second] = await handler.getTile(0, 0, 0);
    assert.deepEqual(new Uint8Array(second.data), tile);
    const completed = requests.length;

    t.mock.timers.tick(1000);
    const [retried] = await first;
    assert.deepEqual(new Uint8Array(retried.data), tile);
    assert.equal(requests.length, completed + 1, 'only the uncached tile body needs fetching after the wait');
    assert.equal(requests.filter(request => request.range === range).length, 2, 'one 429 and one successful read');
  });
}

test('a header retry rechecks the resolved cache shared by different handlers', { timeout: 2000 }, async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const url = 'https://example.test/concurrent-handlers.pmtiles';
  const firstHandler = new PMTilesHandler(url, 'vector', 'pbf', console, false);
  const secondHandler = new PMTilesHandler(url, 'vector', 'pbf', console, false);
  const requests = throttleFirst(t, headerRange);
  const first = firstHandler.getConfig();
  await flush();
  assert.equal((await secondHandler.getConfig()).name, metadata.name);
  t.mock.timers.tick(1000);
  assert.equal((await first).name, metadata.name);
  assert.equal(requests.filter(request => request.range === headerRange).length, 2);
});

test('a mosaic manifest retry reuses the resolver loaded by another request', { timeout: 2000 }, async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const handler = new MosaicHandler('https://example.test/concurrent-manifest/mosaic.json', 'vector', 'pbf', console, false);
  const requests = throttleFirst(t, undefined);
  const first = handler.getConfig();
  await flush();
  assert.equal(requests.length, 1, 'the immediate cache probe must not fetch the manifest');
  assert.equal((await handler.getConfig()).name, metadata.name);
  const completed = requests.length;
  t.mock.timers.tick(1000);
  assert.equal((await first).name, metadata.name);
  assert.equal(requests.length, completed, 'reuse the completed manifest without fetching it again');
  assert.equal(requests.length, 2);
});

for (const Handler of [PMTilesHandler, MosaicHandler]) {
  test(`${Handler.name}: a leaf-directory retry reuses another tile request's cached directory`, { timeout: 2000 }, async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const suffix = Handler === PMTilesHandler ? 'archive.pmtiles' : 'mosaic.json';
    const handler = new Handler(`https://example.test/concurrent-leaf/${Handler.name}/${suffix}`, 'vector', 'pbf', console, false);
    const requests = throttleFirst(t, leafRange, leafArchive);
    await handler.getConfig();
    const first = handler.getTile(0, 0, 0);
    await flush();
    assert.equal(requests.filter(request => request.range === leafRange).length, 1, 'cache probes must not fetch leaves');
    const [second] = await handler.getTile(0, 0, 0);
    assert.deepEqual(new Uint8Array(second.data), tile);
    const completed = requests.length;
    t.mock.timers.tick(1000);
    const [retried] = await first;
    assert.deepEqual(new Uint8Array(retried.data), tile);
    assert.equal(requests.length, completed + 1, 'only the tile body is requested after the wait');
    assert.equal(requests.filter(request => request.range === leafRange).length, 2);
    assert.equal(requests.filter(request => request.range === headerRange).length, 1);
  });
}

test('retries use the new archive version after another request refreshes its ETag', { timeout: 2000 }, async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const handler = new PMTilesHandler('https://example.test/concurrent-etag.pmtiles', 'vector', 'pbf', console, false);
  let etag = 'fixture';
  let bytes = leafArchive;
  let leafReads = 0;
  let headerReads = 0;
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    const range = options.headers.get('Range');
    if (range === headerRange) headerReads++;
    if (range === leafRange && ++leafReads === 1) return new Response('', {
      status: 429, headers: { 'Retry-After': '1' },
    });
    const response = respond(url, options, bytes);
    response.headers.set('ETag', etag);
    return response;
  });
  await handler.getConfig();
  const first = handler.getTile(0, 0, 0);
  await flush();

  etag = 'updated';
  bytes = leafArchive.slice();
  const updatedTile = new Uint8Array([0x1a, 7]);
  bytes.set(updatedTile, leafHeader.tileDataOffset);
  const [second] = await handler.getTile(0, 0, 0);
  assert.deepEqual(new Uint8Array(second.data), updatedTile);
  const completedLeafReads = leafReads;
  const completedHeaderReads = headerReads;

  t.mock.timers.tick(1000);
  const [retried] = await first;
  assert.deepEqual(new Uint8Array(retried.data), updatedTile);
  assert.equal(headerReads, completedHeaderReads);
  assert.equal(leafReads, completedLeafReads);
});

test('workerd requests reuse completed initialization while a peer waits for Retry-After', { timeout: 15000 }, async () => {
  const root = fileURLToPath(new URL('../', import.meta.url));
  const bundle = await build({
    stdin: {
      contents: `import PMTilesHandler from './src/lib/pmtiles_handler.js';
        const handler = new PMTilesHandler('https://example.test/workerd.pmtiles', 'vector', 'pbf', console, false);
        export default { async fetch() { return Response.json(await handler.getConfig()); } };`,
      resolveDir: root,
    },
    bundle: true, format: 'esm', platform: 'browser', conditions: ['workerd'], target: 'es2022', write: false,
    external: ['node:async_hooks'],
  });
  const firstAttempt = Promise.withResolvers();
  let headerReads = 0;
  let metadataReads = 0;
  const worker = new Miniflare({
    compatibilityDate: '2024-11-01', compatibilityFlags: ['nodejs_compat', 'no_handle_cross_request_promise_resolution'],
    modules: true, script: bundle.outputFiles[0].text,
    outboundService: request => {
      const range = request.headers.get('Range');
      if (range === headerRange && ++headerReads === 1) {
        firstAttempt.resolve();
        return new Response('', { status: 429, headers: { 'Retry-After': '1' } });
      }
      if (range === metadataRange) metadataReads++;
      return respond(request.url, { headers: request.headers });
    },
  });
  try {
    const first = worker.dispatchFetch('http://localhost/tiles.json');
    await firstAttempt.promise;
    const second = await worker.dispatchFetch('http://localhost/tiles.json');
    assert.equal(second.status, 200);
    assert.equal((await second.json()).name, metadata.name);
    const retried = await first;
    assert.equal(retried.status, 200);
    assert.equal((await retried.json()).name, metadata.name);
    assert.equal(headerReads, 2, 'one rate-limited request and one successful header read');
    assert.equal(metadataReads, 1);
  } finally {
    await worker.dispose();
  }
});
