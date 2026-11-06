import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { test } from 'node:test';
import { STACParquet } from '../src/stac/stac_parquet.js';
import { RangeCache } from '../src/lib/range_cache.js';

const bytes = await readFile(new URL('./fixtures/stac-ranges.parquet', import.meta.url));
const url = 'https://github.com/example/data/releases/download/maps/index.parquet';
const logger = { error() {} };

function mockCaches(t, value) {
  const previous = globalThis.caches;
  globalThis.caches = value;
  t.after(() => { globalThis.caches = previous; });
}

function upstream(t, override) {
  const ranges = [];
  t.mock.method(globalThis, 'fetch', async (_, options) => {
    if (options.method === 'HEAD') return new Response(null, { headers: { 'Content-Length': String(bytes.length) } });
    const [, start, end] = options.headers.Range.match(/^bytes=(\d+)-(\d+)$/).map(Number);
    ranges.push([start, end + 1]);
    const headers = { 'Content-Range': `bytes ${start}-${end}/${bytes.length}` };
    return override?.(start, end, headers) || new Response(bytes.subarray(start, end + 1), { status: 206, headers });
  });
  return ranges;
}

function edgeCache(t) {
  const entries = new Map();
  mockCaches(t, { open: async () => ({
    match: async key => entries.get(key)?.clone(),
    put: async (key, response) => entries.set(key, response.clone()),
    delete: async key => entries.delete(key),
  }) });
  return entries;
}

test('search columns and sparse item pages skip unrelated payloads and reuse completed ranges', async t => {
  const ranges = upstream(t);
  const reader = new STACParquet(logger, { rangeCache: new RangeCache() });
  const lookup = await reader.read(url, { searchOnly: true });
  assert.equal(lookup.length, 192);
  assert.equal(lookup[0].payload, undefined);
  assert.equal(lookup[0].assets, undefined);
  const payload = reader.metadataCache.get(url).metadata.row_groups[2].columns.find(c => c.meta_data.path_in_schema[0] === 'payload').meta_data;
  const payloadStart = Number(payload.dictionary_page_offset || payload.data_page_offset);
  assert.ok(!ranges.some(([start, end]) => start <= payloadStart && end > payloadStart));
  const selected = await reader.read(url, { positions: [0, 191] });
  assert.deepEqual(selected.map(row => row.id), ['sheet-000', 'sheet-191']);
  assert.equal(selected[0].payload.length, 1024);
  assert.ok(selected[1].assets.cog.href);
  assert.ok(!ranges.some(([start, end]) => start <= payloadStart && end > payloadStart));
  const count = ranges.length;
  assert.deepEqual(await reader.read(url, { positions: [0, 191] }), selected);
  assert.equal(ranges.length, count);
});

test('a fresh reader reuses validated edge ranges without sharing pending I/O', async t => {
  const entries = edgeCache(t);
  const ranges = upstream(t);
  const first = await new STACParquet(logger, { rangeCache: new RangeCache() }).read(url, { rowStart: 32, rowEnd: 35 });
  assert.deepEqual(first.map(row => row.id), ['sheet-032', 'sheet-033', 'sheet-034']);
  assert.ok(entries.size > 0);
  assert.ok([...entries.values()].every(response => response.status === 200));
  const count = ranges.length;
  assert.deepEqual(await new STACParquet(logger, { rangeCache: new RangeCache() }).read(url, { rowStart: 32, rowEnd: 35 }), first);
  assert.equal(ranges.length, count);
});

for (const failure of ['ignored', 'wrong-range', 'truncated', 'oversized', 'corrupt-footer']) {
  test(`${failure} responses cannot poison a later valid read`, async t => {
    const entries = edgeCache(t);
    let failing = true;
    upstream(t, (start, end, headers) => {
      if (!failing) return;
      if (failure === 'ignored') return new Response(bytes);
      if (failure === 'wrong-range') return new Response(bytes.subarray(start, end + 1), { status: 206, headers: { 'Content-Range': 'bytes 0-1/2' } });
      const body = failure === 'corrupt-footer' ? new Uint8Array(end - start + 1)
        : failure === 'oversized' ? new Uint8Array(end - start + 2) : bytes.subarray(start, end);
      return new Response(body, { status: 206, headers });
    });
    const cache = new RangeCache();
    const reader = new STACParquet(logger, { rangeCache: cache });
    await assert.rejects(reader.read(url, { rowStart: 0, rowEnd: 1 }));
    assert.equal(cache.byteLength, 0);
    assert.equal(entries.size, 0);
    assert.equal(reader.metadataCache.size, 0);
    failing = false;
    assert.equal((await reader.read(url, { rowStart: 0, rowEnd: 1 }))[0].id, 'sheet-000');
  });
}

test('unavailable edge cache leaves direct range reads usable', async t => {
  mockCaches(t, { open: async () => { throw new Error('cache unavailable'); } });
  upstream(t);
  assert.equal((await new STACParquet(logger, { rangeCache: new RangeCache() }).read(url, { rowStart: 191, rowEnd: 192 }))[0].id, 'sheet-191');
});

test('in-place revisions keep the upstream URL stable and isolate every cache', async t => {
  edgeCache(t);
  const ranges = upstream(t);
  const reader = new STACParquet(logger, { rangeCache: new RangeCache() });
  const options = { rowStart: 0, rowEnd: 1, revision: 'old-hash' };
  const first = await reader.read(url, options);
  const count = ranges.length;
  assert.deepEqual(await reader.read(url, options), first);
  assert.equal(ranges.length, count);
  assert.deepEqual(await reader.read(url, { ...options, revision: 'new-hash' }), first);
  assert.ok(ranges.length > count);
  assert.ok(globalThis.fetch.mock.calls.every(call => call.arguments[0] === url));
});

test('an asset changing between HEAD and a range cannot mix revisions', async t => {
  const entries = edgeCache(t);
  let changed = true;
  t.mock.method(globalThis, 'fetch', async (_, options) => {
    if (options.method === 'HEAD') return new Response(null, { headers: { 'Content-Length': String(bytes.length), ETag: 'current' } });
    const [, start, end] = options.headers.Range.match(/^bytes=(\d+)-(\d+)$/).map(Number);
    return new Response(bytes.subarray(start, end + 1), { status: 206, headers: {
      'Content-Range': `bytes ${start}-${end}/${bytes.length}`, ETag: changed ? 'different' : 'current',
    } });
  });
  const cache = new RangeCache();
  const reader = new STACParquet(logger, { rangeCache: cache });
  await assert.rejects(reader.read(url, { rowStart: 0, rowEnd: 1 }), /Index changed/);
  assert.equal(cache.byteLength, 0);
  assert.equal(entries.size, 0);
  assert.equal(reader.metadataCache.size, 0);
  changed = false;
  assert.equal((await reader.read(url, { rowStart: 0, rowEnd: 1 }))[0].id, 'sheet-000');
});

test('invalidating a source discards both completed ranges and late pending results', async () => {
  const cache = new RangeCache(8);
  const request = { source: { url: new URL(url) }, offset: 0, length: 4 };
  let complete;
  const pending = cache.forRequest().fetch(request, () => new Promise(resolve => { complete = resolve; }));
  await new Promise(resolve => setImmediate(resolve));
  cache.invalidate(url);
  complete(new Uint8Array(4));
  await pending;
  assert.equal(cache.byteLength, 0);
  await cache.forRequest().fetch(request, async () => new Uint8Array(4));
  assert.equal(cache.byteLength, 4);
  cache.invalidate(url);
  assert.equal(cache.byteLength, 0);
});
