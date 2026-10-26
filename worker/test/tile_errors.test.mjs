import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Hono } from 'hono';
import { registerTileRoutes } from '../src/routes/tiles.js';
import routes from '../src/routes/listing.json' with { type: 'json' };

const prefix = '/not-so-open/environmental/approvals/maharashtra/parivesh/';
const sourceUrl = 'https://github.com/ramSeraph/india-environmental-approvals/releases/latest/download/Projects_27.pmtiles';

function createApp(getServerUrl = () => 'https://example.test') {
  const app = new Hono();
  const logs = [];
  registerTileRoutes(app, getServerUrl, {
    error: (...args) => logs.push(args),
  });
  return { app, logs };
}

const statusCases = [
  [401, 502, 'Bad Gateway'],
  [403, 502, 'Bad Gateway'],
  [404, 502, 'Bad Gateway'],
  [408, 504, 'Gateway Timeout'],
  [429, 503, 'Service Unavailable'],
  [500, 502, 'Bad Gateway'],
  [502, 502, 'Bad Gateway'],
  [503, 503, 'Service Unavailable'],
  [504, 504, 'Gateway Timeout'],
];

for (const resource of ['tiles.json', '0/0/0.pbf']) {
  for (const [status, clientStatus, clientMessage] of statusCases) {
    test(`${resource}: preserve upstream HTTP ${status} and source context in one log`, async t => {
      const { app, logs } = createApp();
      const fetch = t.mock.method(globalThis, 'fetch', async url => {
        assert.equal(url, sourceUrl);
        return new Response('Unavailable', { status });
      });

      const response = await app.request(prefix + resource);
      assert.equal(response.status, clientStatus);
      assert.equal(await response.text(), clientMessage);
      assert.equal(fetch.mock.callCount(), 1, 'upstream failures must not trigger retries');
      assert.equal(response.headers.get('access-control-allow-origin'), '*');
      assert.equal(response.headers.get('cache-control'), 'no-store');
      assert.equal(logs.length, 1);
      assert.equal(logs[0].length, 1);
      assert.equal(typeof logs[0][0], 'string', 'never pass a native Error to the console');
      assert.ok(!logs[0][0].includes('\n'), 'stack newlines must be escaped inside the log record');
      const record = JSON.parse(logs[0][0]);
      assert.equal(record.message, `Tile source request failed: Bad response code: ${status}`);
      assert.equal(record.name, 'Error');
      assert.ok(record.stack.includes('FetchSource'));
      assert.equal(record.method, 'GET');
      assert.equal(record.path, prefix + resource);
      assert.equal(record.sourceUrl, sourceUrl);
      assert.equal(record.upstreamStatus, status);
      assert.equal(record.responseStatus, clientStatus);
    });
  }
}

test('mosaic manifest HTTP errors receive the same translation and logging', async t => {
  const [mosaicPrefix, mosaic] = Object.entries(routes).find(([, route]) => route.handlertype === 'mosaic');
  let status;
  const fetch = t.mock.method(globalThis, 'fetch', async url => {
    assert.equal(url, mosaic.url);
    return new Response('Unavailable', { status });
  });

  for (const [upstreamStatus, clientStatus, clientMessage] of statusCases) {
    status = upstreamStatus;
    const { app, logs } = createApp();
    const callsBefore = fetch.mock.callCount();
    const response = await app.request(mosaicPrefix + 'tiles.json');
    assert.equal(response.status, clientStatus);
    assert.equal(await response.text(), clientMessage);
    assert.equal(fetch.mock.callCount(), callsBefore + 1, 'mosaic failures must not trigger retries');
    assert.equal(logs.length, 1);
    const record = JSON.parse(logs[0][0]);
    assert.equal(record.upstreamStatus, upstreamStatus);
    assert.equal(record.responseStatus, clientStatus);
    assert.equal(record.sourceUrl, mosaic.url);
    assert.equal(record.path, mosaicPrefix + 'tiles.json');
  }
});

test('preserve a network error message even when its stack only contains frames', async t => {
  const { app, logs } = createApp();
  const error = new Error('Connection lost');
  error.stack = '    at FetchSource.getBytes (index.js:1:1)';
  t.mock.method(globalThis, 'fetch', async () => { throw error; });

  const response = await app.request(prefix + 'tiles.json');
  assert.equal(response.status, 500);
  const record = JSON.parse(logs[0][0]);
  assert.equal(record.message, 'Tile source request failed: Connection lost');
  assert.equal(record.stack, error.stack);
  assert.equal(record.responseStatus, 500);
  assert.equal(record.upstreamStatus, undefined, 'do not invent a status for a network exception');
});

test('internal errors stay 500 without being mistaken for upstream HTTP failures', async t => {
  const { app, logs } = createApp(() => { throw new TypeError('Invalid server URL: 503'); });
  const fetch = t.mock.method(globalThis, 'fetch', async () => assert.fail('must not reach fetch'));
  const response = await app.request(prefix + 'tiles.json');
  assert.equal(response.status, 500);
  assert.equal(fetch.mock.callCount(), 0);
  const record = JSON.parse(logs[0][0]);
  assert.equal(record.upstreamStatus, undefined);
  assert.equal(record.responseStatus, 500);
});

test('invalid tile coordinates still return 400 without an upstream fetch or error log', async t => {
  const { app, logs } = createApp();
  const fetch = t.mock.method(globalThis, 'fetch', async () => {
    assert.fail('invalid requests must not fetch an archive');
  });
  const response = await app.request(prefix + '27/0/0.pbf');
  assert.equal(response.status, 400);
  assert.equal(fetch.mock.callCount(), 0);
  assert.deepEqual(logs, []);
});
