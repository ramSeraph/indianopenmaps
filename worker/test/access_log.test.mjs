import assert from 'node:assert/strict';
import { test } from 'node:test';
import app from '../src/index.js';

function capture(t, random = 0.5) {
  const rows = [];
  for (const method of ['log', 'error', 'warn', 'debug']) {
    t.mock.method(console, method, record => rows.push({ method, record }));
  }
  t.mock.method(Math, 'random', () => random);
  return rows;
}

test('default access sampling selects 1% of uniformly spaced draws', async t => {
  const rows = capture(t);
  let draw = 0;
  t.mock.method(Math, 'random', () => draw++ / 100);
  for (let i = 0; i < 100; i++) assert.equal((await app.request('/api/routes')).status, 200);
  assert.equal(draw, 100, 'one sampling decision per request');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].record.event, 'access');
  assert.equal(rows[0].record.sampleRate, 0.01);
});

test('sampled records retain route, response and safe referrer fields', async t => {
  const rows = capture(t);
  const response = await app.request('/api/routes?secret=request', {
    headers: { Referer: 'https://user:secret-password@maps.example/view?secret=token#secret-fragment' },
  }, { ACCESS_LOG_SAMPLE_RATE: '1' });
  assert.equal(response.status, 200);
  assert.equal(rows.length, 1);
  const { method, record } = rows[0];
  assert.equal(method, 'log');
  assert.deepEqual(record, {
    event: 'access', requestId: record.requestId, method: 'GET', path: '/api/routes',
    status: 200, referer: 'https://maps.example/view', refererHost: 'maps.example',
    elapsedMs: record.elapsedMs, sampleRate: 1,
  });
  assert.ok(record.requestId);
  assert.ok(record.elapsedMs >= 0);
  assert.doesNotMatch(JSON.stringify(record), /secret|user:/);
});

test('missing, malformed and non-HTTP referrers are null', async t => {
  const rows = capture(t);
  for (const referer of [null, 'not a URL?secret', 'data:text/plain,secret', 'file:///secret']) {
    await app.request('/api/routes', { headers: referer === null ? {} : { Referer: referer } },
      { ACCESS_LOG_SAMPLE_RATE: '1' });
  }
  assert.equal(rows.length, 4);
  assert.ok(rows.every(({ record }) => record.referer === null && record.refererHost === null));
  assert.doesNotMatch(JSON.stringify(rows), /secret/);
});

test('access rate zero and global off suppress access even when a draw would select it', async t => {
  const rows = capture(t, 0);
  await app.request('/api/routes', {}, { ACCESS_LOG_SAMPLE_RATE: '0' });
  await app.request('/api/routes', {}, { ACCESS_LOG_SAMPLE_RATE: '1', LOG_LEVEL: 'off' });
  assert.deepEqual(rows, []);
});

test('invalid rates fall back to 1% rather than turning on all access logs', async t => {
  const rows = capture(t, 0.02);
  for (const ACCESS_LOG_SAMPLE_RATE of ['', ' ', 'invalid', '-1', '1.01', 'NaN', 'Infinity', null, false]) {
    await app.request('/api/routes', {}, { ACCESS_LOG_SAMPLE_RATE });
  }
  assert.deepEqual(rows, []);
  t.mock.method(Math, 'random', () => 0);
  await app.request('/api/routes', {}, { ACCESS_LOG_SAMPLE_RATE: 'invalid' });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].record.sampleRate, 0.01);
});

test('custom rates apply at their boundary and are included in records', async t => {
  const rows = capture(t, 0.2499);
  await app.request('/api/routes', {}, { ACCESS_LOG_SAMPLE_RATE: ' 0.25 ' });
  t.mock.method(Math, 'random', () => 0.25);
  await app.request('/api/routes', {}, { ACCESS_LOG_SAMPLE_RATE: '0.25' });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].record.sampleRate, 0.25);
});

test('access samples include final 200, 302, 400, 404 and 500 responses once each', async t => {
  const rows = capture(t);
  const ASSETS = { fetch() { throw new Error('Assets unavailable'); } };
  for (const [path, status] of [
    ['/api/routes', 200], ['/wikidata-locater', 302], ['/cog-info', 400], ['/missing', 404], ['/', 500],
  ]) {
    assert.equal((await app.request(path, {}, { ACCESS_LOG_SAMPLE_RATE: '1', ASSETS })).status, status);
  }
  const access = rows.filter(({ record }) => record.event === 'access');
  assert.deepEqual(access.map(({ record }) => record.status), [200, 302, 400, 404, 500]);
  assert.equal(rows.filter(({ method }) => method === 'error').length, 1);
});

test('unsampled requests still log every failure', async t => {
  const rows = capture(t, 0.99);
  const ASSETS = { fetch() { throw new Error('Assets unavailable'); } };
  for (const ACCESS_LOG_SAMPLE_RATE of ['0', '0.01']) {
    assert.equal((await app.request('/', {}, { ACCESS_LOG_SAMPLE_RATE, ASSETS })).status, 500);
  }
  assert.equal(rows.length, 2);
  assert.ok(rows.every(({ method, record }) => method === 'error' && record.event === 'request_error'));
});

test('overlapping requests retain separate access rates and request identities', async t => {
  const rows = capture(t, 0);
  const started = Promise.withResolvers();
  const release = Promise.withResolvers();
  const first = app.request('/', {}, { ACCESS_LOG_SAMPLE_RATE: '0', ASSETS: {
    async fetch() { started.resolve(); await release.promise; return new Response('ok'); },
  } });
  await started.promise;
  await app.request('/api/routes', {}, { ACCESS_LOG_SAMPLE_RATE: '1' });
  release.resolve();
  assert.equal((await first).status, 200);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].record.path, '/api/routes');
  assert.equal(rows[0].record.sampleRate, 1);
  assert.ok(rows[0].record.requestId);
});
