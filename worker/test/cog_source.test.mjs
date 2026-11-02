import assert from 'node:assert/strict';
import { test } from 'node:test';
import { COGSourceCache, COGChangedError } from '../src/lib/cog_source.js';
import { RangeCache } from '../src/lib/range_cache.js';
import { SOURCE_REFRESH_MS } from '../src/lib/refresh_cache.js';

const url = 'https://github.com/ramSeraph/fixtures/replace.tif';

function setup(t) {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const rangeCache = new RangeCache();
  const warnings = [];
  const sources = new COGSourceCache({ rangeCache, logger: { warn: message => warnings.push(message) } });
  const state = { version: 1, size: 64, override: undefined, validator: version => `"v${version}"` };
  const requests = [];
  t.mock.method(globalThis, 'fetch', async (input, options) => {
    assert.equal(String(input), url, 'cache identity must not change the upstream URL');
    assert.equal(options.cache, 'no-cache');
    const request = { method: options.method ?? 'GET', headers: new Headers(options.headers), signal: options.signal };
    requests.push(request);
    const overridden = await state.override?.(request);
    if (overridden) return overridden;
    const etag = state.validator(state.version);
    const headers = etag ? { ETag: etag } : {};
    if (request.method === 'HEAD') {
      if (etag && request.headers.get('If-None-Match') === etag) return new Response(null, { status: 304, headers });
      return new Response(null, { headers: { ...headers, 'Content-Length': String(state.size) } });
    }
    if (request.headers.has('If-Match') && request.headers.get('If-Match') !== etag) return new Response(null, { status: 412 });
    const [, start, end] = request.headers.get('Range').match(/^bytes=(\d+)-(\d+)$/).map(Number);
    return new Response(new Uint8Array(end - start + 1).fill(state.version), { status: 206,
      headers: { ...headers, 'Content-Range': `bytes ${start}-${end}/${state.size}`, 'Content-Length': String(end - start + 1) },
    });
  });
  function operation(offsets = [0]) {
    return async options => {
      const local = rangeCache.forRequest(options);
      const source = options.httpSource;
      const output = [];
      for (const offset of offsets) {
        const request = { source, offset, length: 8 };
        output.push(new Uint8Array(await local.fetch(request, () => source.fetch(offset, 8))));
      }
      return output.map(bytes => [...bytes]);
    };
  }
  return { state, requests, rangeCache, sources, warnings, operation,
    read: (offsets, options) => sources.read(url, operation(offsets), options),
    expire: () => t.mock.timers.tick(SOURCE_REFRESH_MS),
  };
}

test('warm COG ranges avoid I/O and a conditional HEAD renews unchanged bytes', async t => {
  const { read, requests, expire } = setup(t);
  assert.deepEqual(await read(), [Array(8).fill(1)]);
  assert.equal(requests.length, 2);
  assert.equal(requests[1].headers.get('If-Match'), '"v1"');
  await read();
  assert.equal(requests.length, 2);
  expire();
  assert.deepEqual(await read(), [Array(8).fill(1)]);
  assert.equal(requests.length, 3);
  assert.equal(requests[2].headers.get('If-None-Match'), '"v1"');
  await read();
  assert.equal(requests.length, 3);
});

test('same-length COG replacement changes cached ranges at the same upstream URL', async t => {
  const { read, state, requests, expire } = setup(t);
  await read();
  state.version = 2;
  assert.deepEqual(await read(), [Array(8).fill(1)], 'snapshot remains fresh for five minutes');
  expire();
  assert.deepEqual(await read(), [Array(8).fill(2)]);
  assert.equal(requests.at(-1).headers.get('If-Match'), '"v2"');
});

test('a changed size cannot reuse old ranges even if the upstream reuses an ETag', async t => {
  const { read, state, expire } = setup(t);
  await read();
  state.version = 2;
  state.size = 96;
  state.validator = () => '"v1"';
  state.override = request => request.method === 'HEAD'
    ? new Response(null, { headers: { ETag: '"v1"', 'Content-Length': '96' } }) : undefined;
  expire();
  assert.deepEqual(await read(), [Array(8).fill(2)]);
});

for (const validator of [undefined, 'W/"weak"']) {
  test(`COGs with ${validator ?? 'no'} ETag do not share completed byte ranges`, async t => {
    const { read, state, requests, rangeCache } = setup(t);
    state.validator = () => validator;
    await read();
    state.version = 2;
    assert.deepEqual(await read(), [Array(8).fill(2)]);
    assert.equal(requests.filter(request => request.method === 'HEAD').length, 1);
    assert.ok(requests.every(request => !request.headers.has('If-Match')));
    assert.equal(rangeCache.byteLength, 0);
  });
}

test('a weak validator on 304 requires a fresh HEAD and disables shared range reuse', async t => {
  const { read, state, requests, rangeCache, expire } = setup(t);
  await read();
  const priorSize = rangeCache.byteLength;
  state.version = 2;
  state.validator = () => 'W/"v1"';
  state.override = request => request.method === 'HEAD' && request.headers.has('If-None-Match')
    ? new Response(null, { status: 304, headers: { ETag: 'W/"v1"' } }) : undefined;
  expire();
  assert.deepEqual(await read(), [Array(8).fill(2)]);
  assert.equal(requests.filter(request => request.method === 'HEAD').length, 3);
  assert.equal(rangeCache.byteLength, priorSize);
  state.version = 3;
  assert.deepEqual(await read(), [Array(8).fill(3)]);
});

for (const mismatch of ['412', '416', 'etag', 'missing-etag', 'size']) {
  test(`${mismatch} during a COG range read retries the entire operation once`, async t => {
    const { read, state, requests } = setup(t);
    await read();
    let replace = true;
    state.override = request => {
      if (request.method === 'HEAD' || !replace) return;
      replace = false;
      state.version = 2;
      if (mismatch === '412' || mismatch === '416') return new Response(null, { status: Number(mismatch) });
      const headers = { 'Content-Range': `bytes 8-15/${mismatch === 'size' ? 96 : 64}` };
      if (mismatch !== 'missing-etag') headers.ETag = mismatch === 'etag' ? '"v2"' : '"v1"';
      return new Response(new Uint8Array(8).fill(2), { status: 206, headers });
    };
    assert.deepEqual(await read([0, 8]), [Array(8).fill(2), Array(8).fill(2)], 'cached headers and fresh blocks have one version');
    assert.equal(requests.filter(request => request.method === 'HEAD').length, 2);
  });
}

test('a continuously changing COG stops after one retry with an uncachable 503', async t => {
  const { read, state, requests, rangeCache } = setup(t);
  state.override = request => request.method !== 'HEAD' ? new Response(null, { status: 412 }) : undefined;
  await assert.rejects(read(), error => error instanceof COGChangedError && error.statusCode === 503);
  assert.equal(requests.length, 4);
  assert.equal(rangeCache.byteLength, 0);
});

for (const failure of ['ignored', 'wrong-range', 'wrong-length', 'truncated', 'oversized']) {
  test(`${failure} COG responses are rejected before entering the byte cache`, async t => {
    const { read, state, rangeCache } = setup(t);
    let cancelled = false;
    state.override = request => {
      if (request.method === 'HEAD') return;
      if (failure === 'ignored') {
        return new Response(new ReadableStream({ cancel() { cancelled = true; } }), { headers: { ETag: '"v1"' } });
      }
      const headers = { ETag: '"v1"', 'Content-Range': failure === 'wrong-range' ? 'bytes 1-8/64' : 'bytes 0-7/64' };
      if (failure === 'wrong-length') headers['Content-Length'] = '9';
      return new Response(new Uint8Array(failure === 'truncated' ? 7 : failure === 'oversized' ? 9 : 8), { status: 206, headers });
    };
    await assert.rejects(read(), /range response|byte range/i);
    if (failure === 'ignored') assert.equal(cancelled, true, 'never download an unbounded full COG');
    assert.equal(rangeCache.byteLength, 0);
    state.override = undefined;
    assert.deepEqual(await read(), [Array(8).fill(1)]);
  });
}

test('failed COG refresh retains validated bytes and honors Retry-After before checking again', async t => {
  const { read, state, requests, warnings, expire } = setup(t);
  await read();
  state.override = request => request.method === 'HEAD'
    ? new Response(null, { status: 429, headers: { 'Retry-After': '60' } }) : undefined;
  expire();
  assert.deepEqual(await read(), [Array(8).fill(1)]);
  assert.equal(warnings.length, 1);
  assert.equal(requests.length, 3);
  t.mock.timers.tick(59_999);
  await read();
  assert.equal(requests.length, 3);
  t.mock.timers.tick(1);
  await read();
  assert.equal(requests.length, 4);
});

test('failed HEAD refresh cannot combine cached bytes with a replaced source', async t => {
  const { read, state, expire } = setup(t);
  await read();
  state.version = 2;
  state.override = request => request.method === 'HEAD' ? new Response(null, { status: 502 }) : undefined;
  expire();
  assert.deepEqual(await read(), [Array(8).fill(1)]);
  await assert.rejects(read([0, 8]), /HTTP 502/);
  state.override = undefined;
  assert.deepEqual(await read([0, 8]), [Array(8).fill(2), Array(8).fill(2)]);
});

test('late old COG bytes cannot overwrite ranges from a newer ETag', async t => {
  const { read, state, expire } = setup(t);
  let release;
  let started;
  const pending = new Promise(resolve => { started = resolve; });
  state.override = request => {
    if (request.method === 'HEAD' || state.version !== 1) return;
    started();
    return new Promise(resolve => { release = () => resolve(new Response(new Uint8Array(8).fill(1), { status: 206,
      headers: { ETag: '"v1"', 'Content-Range': 'bytes 0-7/64' },
    })); });
  };
  const oldRead = read();
  await pending;
  state.version = 2;
  expire();
  assert.deepEqual(await read(), [Array(8).fill(2)]);
  release();
  assert.deepEqual(await oldRead, [Array(8).fill(1)]);
  assert.deepEqual(await read(), [Array(8).fill(2)]);
});

test('a cancelled COG HEAD cannot delay a peer or install its old metadata', async t => {
  const { read, state, requests } = setup(t);
  let release;
  let started;
  const pending = new Promise(resolve => { started = resolve; });
  state.override = request => {
    if (request.method !== 'HEAD' || requests.length !== 1) return;
    started();
    return new Promise(resolve => { release = () => resolve(new Response(null, {
      headers: { ETag: '"old"', 'Content-Length': '64' },
    })); });
  };
  const controller = new AbortController();
  const oldRead = read(undefined, { signal: controller.signal });
  const rejected = assert.rejects(oldRead, /cancelled/);
  await pending;
  controller.abort(new Error('cancelled'));
  assert.deepEqual(await read(), [Array(8).fill(1)], 'cancellation releases coordination before the old read settles');
  release();
  await rejected;
  const count = requests.length;
  assert.deepEqual(await read(), [Array(8).fill(1)]);
  assert.equal(requests.length, count);
});
