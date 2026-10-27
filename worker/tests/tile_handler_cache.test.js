import assert from 'node:assert/strict';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { metadata, mosaic as fixtureMosaic, tile, respond as respondArchive } from '../test/helpers/pmtiles_fixture.mjs';

// Exercise the browser bundle, keeping runtime-provided compatibility modules external.
const { outputFiles } = await build({
  stdin: {
    contents: `
      export { default as PMTilesHandler } from '../src/lib/pmtiles_handler.js';
      export { default as MosaicHandler } from '../src/lib/mosaic_handler.js';
    `,
    resolveDir: fileURLToPath(new URL('.', import.meta.url)),
  },
  bundle: true, format: 'esm', platform: 'browser', write: false,
  external: ['node:async_hooks'],
});
const { PMTilesHandler, MosaicHandler } = await import(
  `data:text/javascript;base64,${Buffer.from(
    outputFiles[0].text + '\n//# sourceURL=tile-handlers.test-bundle.mjs'
  ).toString('base64')}`
);

const mosaic = {
  ...fixtureMosaic,
  // Adjacent longitude partitions exercise the index with twelve shards.
  slices: Object.fromEntries(Array.from({ length: 12 }, (_, i) => [
    `part-${i}.pmtiles`, { header: {
      ...fixtureMosaic.header,
      min_lon_e7: (-180 + i * 30) * 1e7,
      max_lon_e7: (-150 + i * 30) * 1e7,
    } },
  ])),
};

function respond(url, options) {
  if (url.endsWith('.json')) return Response.json(mosaic);
  return respondArchive(url, options);
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
      const resolver = await handler.mosaic.getResolver();
      assert.equal(resolver.resolve(0, 0, 0).length, 12, 'concurrent initialization must not duplicate shards');
    }
  });

  test(`${Handler.name}: initialization retries after an upstream failure`, async t => {
    const handler = create();
    let calls = 0;
    t.mock.method(globalThis, 'fetch', async (url, options) => {
      if (++calls === 1) return new Response('Unavailable', { status: 503, headers: { 'Retry-After': '60' } });
      return respond(url, options);
    });
    await assert.rejects(handler.getConfig(), /503/);
    assert.equal((await handler.getConfig()).name, metadata.name);
  });

  test(`${Handler.name}: tile reads reuse resolved headers and directories`, async t => {
    const handler = create();
    const requests = [];
    t.mock.method(globalThis, 'fetch', async (url, options) => {
      requests.push({ url, range: options?.headers?.get('Range') });
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
