import assert from 'node:assert/strict';
import { beforeEach, test } from 'node:test';
import { setImmediate as flush } from 'node:timers/promises';
import { fetchUpstream, fetchWithRetry, parseRetryAfter, withRetries, withRequestContext } from '../src/lib/request.js';
import { UpstreamHttpError } from '../src/lib/errors.js';

const url = 'https://github.com/owner/repo/releases/download/data/archive.pmtiles';
beforeEach(t => {
  t.mock.method(console, 'error', () => {});
  t.mock.method(console, 'log', () => {});
});

test('rate limits are logged even when retries succeed, without signed URLs or credentials', async t => {
  const errors = t.mock.method(console, 'error', () => {});
  const input = new URL(url + '?token=secret-query#secret-fragment');
  input.username = 'secret-user'; input.password = 'secret-password';
  const limited = new Response('limited', { status: 429, headers: {
    'Retry-After': '0', 'X-GitHub-Request-Id': 'request-id', 'CF-Ray': 'ray-id',
  } });
  Object.defineProperty(limited, 'url', { value: 'https://release-assets.githubusercontent.com/file?sig=secret-signature' });
  const fetch = t.mock.method(globalThis, 'fetch', async () => fetch.mock.callCount() === 0 ? limited : new Response('ok'));
  assert.equal(await (await fetchWithRetry(input, { headers: { Authorization: 'secret-authorization' } })).text(), 'ok');
  assert.equal(errors.mock.callCount(), 1, 'cache probes and successful retries must not duplicate the error');
  const [row] = errors.mock.calls[0].arguments;
  assert.equal(row.message, 'Upstream rate limited');
  assert.equal(row.status, 429);
  assert.equal(row.host, 'github.com');
  assert.equal(row.path, '/owner/repo/releases/download/data/archive.pmtiles');
  assert.equal(row.responseHost, 'release-assets.githubusercontent.com');
  assert.equal(row.method, 'GET');
  assert.equal(row.attempt, 1);
  assert.equal(row.retryAfter, '0');
  assert.equal(row.githubRequestId, 'request-id');
  assert.equal(row.ray, 'ray-id');
  assert.ok(row.elapsedMs >= 0);
  assert.doesNotMatch(JSON.stringify(row), /secret/);
});

test('every real rate-limited attempt is logged, including the exhausted final attempt', async t => {
  const errors = t.mock.method(console, 'error', () => {});
  t.mock.method(globalThis, 'fetch', async () => new Response(null, { status: 429, headers: { 'Retry-After': '0' } }));
  assert.equal((await fetchWithRetry(url)).status, 429);
  assert.deepEqual(errors.mock.calls.map(call => call.arguments[0].attempt), [1, 2, 3]);
});

test('rate limits are logged for operation retries and non-retried requests too', async t => {
  const errors = t.mock.method(console, 'error', () => {});
  t.mock.method(globalThis, 'fetch', async () => new Response(null, { status: 429, headers: { 'Retry-After': '0' } }));
  // PMTiles uses fetchUpstream inside an operation-level retry.
  await assert.rejects(withRetries(async () => { throw new UpstreamHttpError(await fetchUpstream(url)); }));
  assert.deepEqual(errors.mock.calls.map(call => call.arguments[0].attempt), [1, 2, 3]);
  await fetchWithRetry(new Request(url, { method: 'POST' }));
  assert.equal(errors.mock.calls.at(-1).arguments[0].method, 'POST');
  assert.equal(errors.mock.calls.at(-1).arguments[0].attempt, 1);
});

test('an unretried long rate limit still produces one error', async t => {
  const errors = t.mock.method(console, 'error', () => {});
  t.mock.method(globalThis, 'fetch', async () => new Response(null, { status: 429, headers: { 'Retry-After': '60' } }));
  await fetchWithRetry(url);
  assert.equal(errors.mock.callCount(), 1);
  assert.equal(errors.mock.calls[0].arguments[0].retryAfter, '60');
});

test('info logs successful fetches, warns about slow ones and errors on HTTP failures', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const warnings = t.mock.method(console, 'warn', () => {});
  const logs = t.mock.method(console, 'log', () => {});
  const errors = t.mock.method(console, 'error', () => {});
  const fetch = t.mock.method(globalThis, 'fetch', async () => {
    if (fetch.mock.callCount() === 1) t.mock.timers.tick(20_000);
    return new Response(null, { status: fetch.mock.callCount() >= 2 ? 522 : 206 });
  });
  await withRequestContext(new Request('https://example.test/'), async () => {
    await fetchUpstream(url);
    assert.equal(warnings.mock.callCount(), 0);
    assert.equal(logs.mock.callCount(), 1);
    assert.equal(logs.mock.calls[0].arguments[0].status, 206);
    await fetchUpstream(url);
    const row = warnings.mock.calls[0].arguments[0];
    assert.equal(row.message, 'Slow upstream fetch');
    assert.equal(row.elapsedMs, 20_000);
    await fetchUpstream(url);
    assert.equal(errors.mock.calls[0].arguments[0].status, 522);
  }, { logLevel: 'info' });
});

test('default logging suppresses successful and slow fetches but retains failures', async t => {
  t.mock.timers.enable({ apis: ['Date'], now: 0 });
  const logs = t.mock.method(console, 'log', () => {});
  const warnings = t.mock.method(console, 'warn', () => {});
  const errors = t.mock.method(console, 'error', () => {});
  let status = 200;
  t.mock.method(globalThis, 'fetch', async () => {
    t.mock.timers.tick(6000);
    return new Response(null, { status });
  });
  await fetchUpstream(url);
  assert.equal(logs.mock.callCount(), 0);
  assert.equal(warnings.mock.callCount(), 0);
  status = 503;
  await fetchUpstream(url);
  assert.equal(errors.mock.callCount(), 1);
});

test('concurrent tile requests retain separate log identities through retries and cache probes', async t => {
  const rows = [];
  t.mock.method(console, 'log', row => rows.push(row));
  t.mock.method(console, 'error', row => rows.push(row));
  const firstStarted = Promise.withResolvers();
  const releaseFirst = Promise.withResolvers();
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => {
    if (++calls === 1) {
      firstStarted.resolve();
      await releaseFirst.promise;
      return new Response(null, { status: 429, headers: { 'Retry-After': '0' } });
    }
    return new Response('ok');
  });
  const run = (path, range) => withRequestContext(new Request(`https://indianopenmaps.com${path}?private=query`),
    () => fetchWithRetry(url, { headers: { Range: range } }), { logLevel: 'info' });
  const first = run('/tiles/1/0/0.pbf', 'bytes=0-16383');
  await firstStarted.promise;
  await run('/tiles/1/0/1.pbf', 'bytes=0-16383');
  releaseFirst.resolve();
  await first;
  assert.equal(rows.length, 3, 'each real fetch is logged once; cache probes produce no fetch logs');
  const firstRows = rows.filter(row => row.requestPath === '/tiles/1/0/0.pbf');
  assert.deepEqual(firstRows.map(row => row.attempt), [1, 2]);
  assert.equal(firstRows[0].requestId, firstRows[1].requestId);
  assert.notEqual(rows[0].requestId, firstRows[0].requestId);
  assert.ok(rows.every(row => row.range === 'bytes=0-16383' && row.event === 'upstream_fetch'));
  assert.doesNotMatch(JSON.stringify(rows), /private=query/);
});

test('network failures are logged without leaking signed URLs in the error message', async t => {
  const errors = t.mock.method(console, 'error', () => {});
  const error = new TypeError('Failed fetch https://example.com/?secret=token');
  t.mock.method(globalThis, 'fetch', async () => { throw error; });
  await assert.rejects(fetchUpstream(url), actual => actual === error);
  const row = errors.mock.calls[0].arguments[0];
  assert.equal(row.error, 'TypeError');
  assert.equal(row.status, null);
  assert.doesNotMatch(JSON.stringify(row), /secret/);
});

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

for (const status of [429, 503, 522]) {
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
    for (const status of [206, 401, 403, 404, 416, 429, 500, 503, 522]) {
      const fetch = t.mock.method(globalThis, 'fetch', async () => new Response(null, {
        status, headers: { 'Retry-After': '0' },
      }));
      const response = await fetchWithRetry(url, { method });
      assert.equal(response.status, status);
      const retry = ['GET', 'HEAD'].includes(method) && [429, 503, 522].includes(status);
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
