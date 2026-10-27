import assert from 'node:assert/strict';
import { test } from 'node:test';
import { setImmediate as flush } from 'node:timers/promises';
import { fetchUpstream, fetchWithRetry, parseRetryAfter, withRetries } from '../src/lib/request.js';
import { UpstreamHttpError } from '../src/lib/errors.js';

const url = 'https://github.com/owner/repo/releases/download/data/archive.pmtiles';

test('Retry-After accepts seconds and HTTP dates, without interpreting malformed seconds as dates', () => {
  const now = Date.parse('Wed, 28 Oct 2026 10:00:00 GMT');
  assert.equal(parseRetryAfter(' 12 ', now), 12000);
  assert.equal(parseRetryAfter('0', now), 0);
  assert.equal(parseRetryAfter('Wed, 28 Oct 2026 10:00:05 GMT', now), 5000);
  assert.equal(parseRetryAfter('Wednesday, 28-Oct-26 10:00:05 GMT', now), 5000);
  assert.equal(parseRetryAfter('Wed Oct 28 10:00:05 2026', now), 5000);
  assert.equal(parseRetryAfter('Tue, 27 Oct 2026 10:00:00 GMT', now), 0);
  for (const value of [null, '', ' ', '-1', '+1', '1.5', '1e3', 'NaN', 'Infinity', 'tomorrow', 'Wed, nonsense']) {
    assert.equal(parseRetryAfter(value, now), null, String(value));
  }
});

for (const retryAfter of ['2', 'Wed, 28 Oct 2026 10:00:02 GMT']) {
  test(`429 waits for Retry-After ${retryAfter} before repeating the same range request`, async t => {
    t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: Date.parse('Wed, 28 Oct 2026 10:00:00 GMT') });
    let cancelled = false;
    const limited = new Response(new ReadableStream({ cancel() { cancelled = true; } }), {
      status: 429, headers: { 'Retry-After': retryAfter },
    });
    const success = new Response('tile', { status: 206 });
    const options = { headers: new Headers({ Range: 'bytes=127-255' }), cf: { cacheTtl: -1 } };
    const fetch = t.mock.method(globalThis, 'fetch', async (input, init) => {
      assert.equal(input, url);
      assert.equal(init, options);
      return fetch.mock.callCount() === 0 ? limited : success;
    });
    const pending = fetchWithRetry(url, options);
    await flush();
    assert.ok(cancelled, 'discard the error body before sleeping');
    t.mock.timers.tick(1999);
    await flush();
    assert.equal(fetch.mock.callCount(), 1);
    t.mock.timers.tick(1);
    assert.equal(await pending, success);
    assert.equal(await success.text(), 'tile');
    assert.equal(fetch.mock.callCount(), 2);
  });
}

for (const status of [429, 503]) {
  test(`${status} without a usable Retry-After uses bounded exponential backoff`, async t => {
    t.mock.timers.enable({ apis: ['setTimeout'] });
    const responses = [];
    const fetch = t.mock.method(globalThis, 'fetch', async () => {
      const response = new Response('unavailable', {
        status, headers: responses.length === 1 ? { 'Retry-After': '-1' } : {},
      });
      responses.push(response);
      return response;
    });
    const pending = fetchWithRetry(url);
    await flush();
    t.mock.timers.tick(999);
    await flush();
    assert.equal(fetch.mock.callCount(), 1);
    t.mock.timers.tick(1);
    await flush();
    assert.equal(fetch.mock.callCount(), 2);
    t.mock.timers.tick(1999);
    await flush();
    assert.equal(fetch.mock.callCount(), 2);
    t.mock.timers.tick(1);
    const response = await pending;
    assert.equal(fetch.mock.callCount(), 3);
    assert.equal(response, responses[2]);
    assert.equal(await response.text(), 'unavailable', 'the caller still owns the final body');
  });
}

test('zero Retry-After cannot cause an unbounded retry loop', async t => {
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response('', {
    status: 429, headers: { 'Retry-After': '0' },
  }));
  assert.equal((await fetchWithRetry(url)).status, 429);
  assert.equal(fetch.mock.callCount(), 3);
});

test('long Retry-After values are returned intact without retrying early', async t => {
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response('limited', {
    status: 429, headers: { 'Retry-After': '60' },
  }));
  const response = await fetchWithRetry(url);
  assert.equal(fetch.mock.callCount(), 1);
  assert.equal(response.headers.get('Retry-After'), '60');
  assert.equal(await response.text(), 'limited');
});

test('the waiting budget covers all retries combined', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response('limited', {
    status: 429, headers: { 'Retry-After': '6' },
  }));
  const pending = fetchWithRetry(url);
  await flush();
  t.mock.timers.tick(6000);
  const response = await pending;
  assert.equal(fetch.mock.callCount(), 2);
  assert.equal(response.status, 429);
  assert.equal(response.headers.get('Retry-After'), '6');
});

test('only GET and HEAD retry, and unrelated HTTP errors pass through', async t => {
  for (const method of ['GET', 'HEAD', 'POST', 'PUT', 'DELETE']) {
    for (const status of [206, 401, 403, 404, 416, 429, 500, 503]) {
      const fetch = t.mock.method(globalThis, 'fetch', async () => new Response(null, {
        status, headers: { 'Retry-After': '0' },
      }));
      const response = await fetchWithRetry(url, { method });
      assert.equal(response.status, status);
      const retry = ['GET', 'HEAD'].includes(method) && [429, 503].includes(status);
      assert.equal(fetch.mock.callCount(), retry ? 3 : 1, `${method} ${status}`);
      fetch.mock.restore();
    }
  }
});

test('Request input supplies its method and signal', async t => {
  const controller = new AbortController();
  const request = new Request(url, { method: 'POST', body: 'data', signal: controller.signal });
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response(null, {
    status: 429, headers: { 'Retry-After': '0' },
  }));
  assert.equal((await fetchWithRetry(request)).status, 429);
  assert.equal(fetch.mock.callCount(), 1);
  controller.abort();
  await assert.rejects(fetchWithRetry(request), { name: 'AbortError' });
  assert.equal(fetch.mock.callCount(), 1);
  assert.equal((await fetchWithRetry(request, { signal: null })).status, 429);
  assert.equal(fetch.mock.callCount(), 2, 'an explicit null signal overrides the Request signal');
});

test('cancellation during backoff stops retries and removes the timer', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const controller = new AbortController();
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response('', {
    status: 429, headers: { 'Retry-After': '2' },
  }));
  const pending = fetchWithRetry(url, { signal: controller.signal });
  const rejected = assert.rejects(pending, { name: 'AbortError' });
  await flush();
  controller.abort();
  await rejected;
  t.mock.timers.runAll();
  await flush();
  assert.equal(fetch.mock.callCount(), 1);
});

test('simultaneous requests do not share pending I/O or retry timers', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const fetch = t.mock.method(globalThis, 'fetch', async () => {
    if (fetch.mock.callCount() === 0) return new Response('', { status: 429, headers: { 'Retry-After': '2' } });
    return new Response('ok');
  });
  const first = fetchWithRetry(url);
  await flush();
  assert.equal(await (await fetchWithRetry(url)).text(), 'ok');
  assert.equal(fetch.mock.callCount(), 2);
  t.mock.timers.tick(2000);
  assert.equal(await (await first).text(), 'ok');
  assert.equal(fetch.mock.callCount(), 3);
});

test('network errors and aborts from fetch propagate without retries', async t => {
  const error = new TypeError('Connection lost');
  const fetch = t.mock.method(globalThis, 'fetch', async () => { throw error; });
  await assert.rejects(fetchWithRetry(url), e => e === error);
  assert.equal(fetch.mock.callCount(), 1);
});

test('an asynchronous cache probe cannot block another request or send an early retry', { timeout: 2000 }, async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const probeStarted = Promise.withResolvers();
  const releaseProbe = Promise.withResolvers();
  let reads = 0;
  const fetch = t.mock.method(globalThis, 'fetch', async () => fetch.mock.callCount() === 0
    ? new Response('', { status: 429, headers: { 'Retry-After': '1' } })
    : new Response('ok'));
  const first = withRetries(async () => {
    if (++reads === 2) {
      probeStarted.resolve();
      await releaseProbe.promise;
    }
    const response = await fetchUpstream(url);
    if (!response.ok) throw new UpstreamHttpError(response);
    return response;
  });
  await probeStarted.promise;
  assert.equal(await (await fetchWithRetry(url)).text(), 'ok', 'other requests can still fetch during the probe');
  releaseProbe.resolve();
  await flush();
  assert.equal(fetch.mock.callCount(), 2, 'the cache probe must not reach fetch, even after an await');
  t.mock.timers.tick(1000);
  assert.equal(await (await first).text(), 'ok');
  assert.equal(fetch.mock.callCount(), 3);
});
