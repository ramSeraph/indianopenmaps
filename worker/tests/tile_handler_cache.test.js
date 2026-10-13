import assert from 'node:assert/strict';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';

// Match Wrangler's module resolution: pmtiles v2 exposes its ESM entry through
// the bundler's "module" field rather than Node's native package resolution.
const { outputFiles } = await build({
  stdin: {
    contents: `
      export { default as PMTilesHandler } from '../src/lib/pmtiles_handler.js';
      export { default as MosaicHandler } from '../src/lib/mosaic_handler.js';
    `,
    resolveDir: fileURLToPath(new URL('.', import.meta.url)),
  },
  bundle: true, format: 'esm', platform: 'browser', write: false,
});
const { PMTilesHandler, MosaicHandler } = await import(
  `data:text/javascript;base64,${Buffer.from(
    outputFiles[0].text + '\n//# sourceURL=tile-handlers.test-bundle.mjs'
  ).toString('base64')}`
);

const metadata = { name: 'Test archive', vector_layers: [{ id: 'test' }] };
const tile = new Uint8Array([0x1a, 0]);

// A small PMTiles v3 archive with one uncompressed z0 tile.
function makeArchive() {
  const directory = new Uint8Array([1, 0, 1, tile.length, 1]);
  const json = new TextEncoder().encode(JSON.stringify(metadata));
  const metadataOffset = 127 + directory.length;
  const tileOffset = metadataOffset + json.length;
  const bytes = new Uint8Array(tileOffset + tile.length);
  bytes.set(new TextEncoder().encode('PMTiles'));
  const view = new DataView(bytes.buffer);
  view.setUint8(7, 3);
  for (const [offset, value] of [
    [8, 127], [16, directory.length], [24, metadataOffset], [32, json.length],
    [56, tileOffset], [64, tile.length], [72, 1], [80, 1], [88, 1],
  ]) view.setBigUint64(offset, BigInt(value), true);
  for (const offset of [96, 97, 98, 99]) view.setUint8(offset, 1);
  for (const [offset, value] of [[102, -180], [106, -90], [110, 180], [114, 90]]) {
    view.setInt32(offset, value * 1e7, true);
  }
  bytes.set(directory, 127);
  bytes.set(json, metadataOffset);
  bytes.set(tile, tileOffset);
  return bytes;
}

const archive = makeArchive();
const mosaicHeader = {
  min_zoom: 0, max_zoom: 0, tile_type: 1,
  min_lon_e7: -180e7, min_lat_e7: -90e7, max_lon_e7: 180e7, max_lat_e7: 90e7,
  center_lon_e7: 0, center_lat_e7: 0, center_zoom: 0,
};
const mosaic = {
  version: 1, header: mosaicHeader, metadata,
  // Enough shards to exercise the spatial index as well as the per-zoom lists.
  slices: Object.fromEntries(Array.from({ length: 12 }, (_, i) => [
    `part-${i}.pmtiles`, { header: mosaicHeader },
  ])),
};

function respond(url, options) {
  if (url.endsWith('.json')) return Response.json(mosaic);
  const [, start, end] = options.headers.get('Range').match(/bytes=(\d+)-(\d+)/);
  return new Response(archive.slice(Number(start), Number(end) + 1), {
    status: 206, headers: { ETag: 'fixture' },
  });
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

async function within(promise) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('Waiting on another request')), 1000);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

let nextUrl = 0;
for (const Handler of [PMTilesHandler, MosaicHandler]) {
  const create = () => new Handler(
    `https://example.test/${++nextUrl}/archive.${Handler === MosaicHandler ? 'json' : 'pmtiles'}`,
    'vector', 'pbf', console, false,
  );

  test(`${Handler.name}: a stalled initialization does not block another request`, async t => {
    const handler = create();
    const stalled = deferred();
    let calls = 0;
    t.mock.method(globalThis, 'fetch', async (url, options) => {
      if (++calls === 1) await stalled.promise;
      return respond(url, options);
    });
    const first = handler.getConfig();
    try {
      assert.equal((await within(handler.getConfig())).name, metadata.name);
    } finally {
      stalled.resolve();
      await first;
    }
    const completedCalls = calls;
    assert.equal((await handler.getConfig()).name, metadata.name);
    assert.equal(calls, completedCalls, 'warm metadata must not refetch');
    if (Handler === MosaicHandler) {
      assert.equal(handler.keys_map[0].length, 12, 'concurrent initialization must not duplicate shards');
      assert.equal(handler.index_map[0].numItems, 12);
    }
  });

  test(`${Handler.name}: initialization retries after an upstream failure`, async t => {
    const handler = create();
    let calls = 0;
    t.mock.method(globalThis, 'fetch', async (url, options) => {
      if (++calls === 1) return new Response('Unavailable', { status: 503 });
      return respond(url, options);
    });
    await assert.rejects(handler.getConfig(), /503/);
    assert.equal((await handler.getConfig()).name, metadata.name);
  });

  test(`${Handler.name}: tile reads reuse resolved headers and directories`, async t => {
    const handler = create();
    const requests = [];
    t.mock.method(globalThis, 'fetch', async (url, options) => {
      requests.push({ url, range: options?.headers.get('Range') });
      return respond(url, options);
    });
    await handler.getConfig();
    const [first, mimeType] = await handler.getTile(0, 0, 0);
    assert.deepEqual(new Uint8Array(first.data), tile);
    assert.equal(mimeType, 'application/vnd.mapbox-vector-tile');
    const completedCalls = requests.length;
    const [second] = await handler.getTile(0, 0, 0);
    assert.deepEqual(new Uint8Array(second.data), tile);
    assert.equal(requests.length, completedCalls + 1, 'only tile contents should be refetched');
    assert.equal(requests.filter(request => request.range === 'bytes=0-16383').length, 1);
  });
}

test('PMTiles handlers sharing a URL do not share pending header reads', async t => {
  const url = 'https://example.test/shared/archive.pmtiles';
  const first = new PMTilesHandler(url, 'vector', 'pbf', console, false);
  const second = new PMTilesHandler(url, 'vector', 'pbf', console, false);
  const stalled = deferred();
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async (url, options) => {
    if (++calls === 1) await stalled.promise;
    return respond(url, options);
  });
  const pending = first.getConfig();
  try {
    assert.equal((await within(second.getConfig())).name, metadata.name);
  } finally {
    stalled.resolve();
    await pending;
  }
});
