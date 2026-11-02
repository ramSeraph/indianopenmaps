import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import { bytesToHeader } from 'pmtiles';
import { CacheFill, CACHE_FILL_LEASE_MS } from '../src/lib/cache_fill.js';
import { RefreshCache } from '../src/lib/refresh_cache.js';
import { makeArchive, respond, tile } from './helpers/pmtiles_fixture.mjs';

test('cache completion notifies waiting requests without advancing their fallback timers', { timeout: 1000 }, async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const gate = Promise.withResolvers();
  let loads = 0;
  const cache = new RefreshCache({ load: () => { loads++; return gate.promise; } });
  const pending = Array.from({ length: 8 }, () => cache.get());
  gate.resolve('completed');
  assert.deepEqual(await Promise.all(pending), Array(8).fill('completed'));
  assert.equal(loads, 1);
});

test('cancelled followers do not cancel the cache fill; cancelled leaders release their lease', async () => {
  const fill = new CacheFill();
  const gate = Promise.withResolvers();
  const leader = new AbortController();
  const follower = new AbortController();
  const first = fill.run('key', () => gate.promise, { signal: leader.signal });
  const second = fill.run('key', () => assert.fail('cancelled follower fetched'), { signal: follower.signal });
  follower.abort();
  await assert.rejects(second, { name: 'AbortError' });
  leader.abort();
  assert.equal(await fill.run('key', () => 'replacement'), 'replacement');
  gate.resolve('old');
  await first;
});

test('an abandoned lease expires and a late completion cannot replace a newer snapshot', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const gate = Promise.withResolvers();
  let loads = 0;
  const cache = new RefreshCache({ load: () => ++loads === 1 ? gate.promise : 'new' });
  const first = cache.get();
  t.mock.timers.tick(CACHE_FILL_LEASE_MS);
  assert.equal(await cache.get(), 'new');
  gate.resolve('old');
  assert.equal(await first, 'new');
  assert.equal(await cache.get(), 'new');
});

async function makeWorker(outboundService) {
  const bundle = await build({
    stdin: {
      contents: `import PMTilesHandler from './src/lib/pmtiles_handler.js';
        import MosaicHandler from './src/lib/mosaic_handler.js';
        import { withRequestContext } from './src/lib/request.js';
        const single = new PMTilesHandler('https://example.test/single.pmtiles', 'vector', 'pbf', console, false);
        const mosaic = new MosaicHandler('https://example.test/mosaic.json', 'vector', 'pbf', console, false);
        export default { fetch(request) {
          return withRequestContext(request, async () => {
            const path = new URL(request.url).pathname;
            const handler = path.includes('mosaic') ? mosaic : single;
            const pending = handler.getTile(0, 0, 0);
            if (path.includes('abandon')) {
              pending.catch(() => {});
              await new Promise(resolve => setTimeout(resolve, 50));
              return new Response('abandoned');
            }
            const [tile] = await pending;
            if (request.method === 'POST' && await request.text() !== 'follower-owned body') throw new Error('wrong body');
            return new Response(tile.data);
          });
        } };`,
      resolveDir: fileURLToPath(new URL('../', import.meta.url)),
    },
    bundle: true, format: 'esm', platform: 'browser', conditions: ['workerd'], target: 'es2022', write: false,
    external: ['node:async_hooks'],
  });
  return new Miniflare({
    compatibilityDate: '2024-11-01',
    compatibilityFlags: ['nodejs_compat', 'cache_option_enabled', 'cache_no_cache_enabled'],
    modules: true, script: bundle.outputFiles[0].text, outboundService,
  });
}

for (const kind of ['single', 'mosaic']) {
  test(`workerd: eight ${kind} tile requests share completed manifest/header/directory reads`, { timeout: 15000 }, async () => {
    const bytes = makeArchive({ leafDirectory: true });
    const header = bytesToHeader(bytes.buffer);
    const tileRange = `bytes=${header.tileDataOffset}-${header.tileDataOffset + tile.length - 1}`;
    const reads = new Map();
    const worker = await makeWorker(async request => {
      const key = `${request.url}|${request.headers.get('Range')}`;
      reads.set(key, (reads.get(key) ?? 0) + 1);
      // Outlive the lease to verify the leader renews it while its request lives.
      await sleep(request.headers.get('Range') === 'bytes=0-16383' ? CACHE_FILL_LEASE_MS * 1.5 : 75);
      return respond(request.url, { headers: request.headers }, bytes);
    });
    try {
      const responses = await Promise.all(Array.from({ length: 8 }, () => worker.dispatchFetch(`http://localhost/${kind}`, {
        method: 'POST', body: 'follower-owned body',
      })));
      for (const response of responses) {
        assert.equal(response.status, 200, response.status === 200 ? undefined : await response.text());
        assert.deepEqual(new Uint8Array(await response.arrayBuffer()), tile);
      }
      for (const [key, count] of reads) assert.equal(count, key.endsWith(`|${tileRange}`) ? 8 : 1, key);
      assert.equal(reads.size, 4, 'header, metadata/manifest, directory and tile body');
    } finally { await worker.dispose(); }
  });

  for (const ending of ['response', 'disconnect']) test(`workerd: ${kind} followers survive the initiator's ${ending} before its read finishes`, { timeout: 15000 }, async () => {
    let abandoned = false;
    const started = Promise.withResolvers();
    const stalled = Promise.withResolvers();
    let reads = 0;
    const worker = await makeWorker(async request => {
      reads++;
      if (!abandoned) {
        abandoned = true;
        started.resolve();
        return stalled.promise;
      }
      return respond(request.url, { headers: request.headers });
    });
    try {
      const controller = new AbortController();
      const first = ending === 'response' ? worker.dispatchFetch(`http://localhost/${kind}/abandon`)
        : fetch(new URL(`/${kind}`, await worker.ready), { signal: controller.signal });
      const disconnected = ending === 'disconnect' ? assert.rejects(first, { name: 'AbortError' }) : undefined;
      await started.promise;
      const followers = Array.from({ length: 4 }, () => worker.dispatchFetch(`http://localhost/${kind}`));
      if (ending === 'response') assert.equal(await (await first).text(), 'abandoned');
      else {
        controller.abort();
        await disconnected;
      }
      for (const response of await Promise.all(followers)) {
        assert.equal(response.status, 200, response.status === 200 ? undefined : await response.text());
        assert.deepEqual(new Uint8Array(await response.arrayBuffer()), tile);
      }
      assert.equal(reads, 7, 'one abandoned read, two initialization reads, four tile bodies');
    } finally {
      stalled.resolve(new Response('cancelled', { status: 503 }));
      await worker.dispose();
    }
  });
}
