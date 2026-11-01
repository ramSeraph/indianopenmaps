import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFile } from 'node:fs/promises';
import { Hono } from 'hono';
import { registerStacRoutes } from '../src/routes/stac.js';
import { bboxGeometry } from '../src/lib/stac_query.js';
import { fetchStacPages } from '../static/js/stac_pagination.js';

const base = 'https://example.org/stac';
const item = (id, bbox = [77, 28, 78, 29], datetime = '2025-01-01T00:00:00Z') => ({
  type: 'Feature', stac_version: '1.1.0', id, bbox, geometry: bboxGeometry(bbox),
  properties: { datetime }, assets: { cog: { href: 'https://example.org/map.tif' } }, links: [],
});
const catalog = { id: 'test', title: 'Test', description: 'Test', links: ['one', 'two'].map(id => ({
  rel: 'child', href: `./${id}`, title: id, geoparquet: `https://example.org/${id}.parquet`,
  collection: { id, description: id, license: 'other', extent: { spatial: { bbox: [[77,28,78,29]] }, temporal: { interval: [[null,null]] } } }
})) };

async function fixture() {
  const app = new Hono();
  const handler = await registerStacRoutes(app, { error() {} }, catalog);
  handler.readGeoparquet = async url => url.includes('/one.') ? Array.from({ length: 1003 }, (_, i) => item(`a-${i}`)) : [item('b-0')];
  return { app, handler };
}

test('GET and standard POST pagination cross collection boundaries without duplicates', async () => {
  const { app } = await fixture();
  for (const method of ['GET', 'POST']) {
    let url = method === 'GET' ? `${base}/search?limit=1000` : `${base}/search`;
    let body = { limit: 1000 }; const ids = [];
    while (url) {
      const response = await app.request(url, { method, ...(method === 'POST' ? { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) } : {}) });
      assert.equal(response.status, 200);
      assert.match(response.headers.get('content-type'), /application\/geo\+json/);
      assert.equal(response.headers.get('access-control-allow-origin'), '*');
      const page = await response.json();
      assert.equal(page.numberMatched, 1004);
      ids.push(...page.features.map(f => `${f.collection}/${f.id}`));
      for (const feature of page.features) {
        assert.ok(feature.links.find(l => l.rel === 'collection').href.startsWith(base));
        assert.ok(feature.links.find(l => l.rel === 'self').href.startsWith(base));
      }
      const next = page.links.find(l => l.rel === 'next');
      url = next?.href; body = next?.body;
    }
    assert.equal(ids.length, 1004); assert.equal(new Set(ids).size, 1004);
    assert.equal(ids.at(-1), 'two/b-0');
  }
});

test('late item lookup and filter pagination use complete records', async () => {
  const { app } = await fixture();
  const response = await app.request(`${base}/collections/one/items/a-1002`);
  assert.equal(response.status, 200); assert.equal((await response.json()).id, 'a-1002');
  const page = await (await app.request(`${base}/search?collections=one&bbox=77.1,28.1,77.2,28.2&limit=2&offset=1000`)).json();
  assert.deepEqual(page.features.map(i => i.id), ['a-1000', 'a-1001']);
  const next = new URL(page.links.find(l => l.rel === 'next').href);
  assert.equal(next.searchParams.get('collections'), 'one'); assert.equal(next.searchParams.get('offset'), '1002');
});

test('spatial search uses real polygons, holes, touching edges and dateline bboxes', async () => {
  const { app, handler } = await fixture();
  const triangle = item('triangle', [0, 0, 10, 10]);
  triangle.geometry = { type: 'Polygon', coordinates: [[[0,0],[10,0],[0,10],[0,0]], [[1,1],[1,2],[2,2],[2,1],[1,1]]] };
  handler.readGeoparquet = async () => [triangle, item('dateline', [178,0,179,1])];
  for (const [bbox, expected] of [['8,8,9,9', []], ['1.1,1.1,1.9,1.9', []], ['10,0,11,1', ['triangle']], ['177,-1,-177,2', ['dateline']]]) {
    const page = await (await app.request(`${base}/search?collections=one&bbox=${bbox}`)).json();
    assert.deepEqual(page.features.map(i => i.id), expected);
  }
  const page = await (await app.request(`${base}/search`, { method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ collections: ['one'], intersects: { type: 'Point', coordinates: [5,1] } }) })).json();
  assert.equal(page.features[0].id, 'triangle');
});

test('datetime filtering handles intervals, open ranges, and invalid inputs return 400', async () => {
  const { app } = await fixture();
  for (const [datetime, count] of [['2025-01-01T00:00:00Z',1003], ['../2024-12-31T23:59:59Z',0], ['2024-12-31T00:00:00Z/..',1003]]) {
    const page = await (await app.request(`${base}/search?collections=one&datetime=${encodeURIComponent(datetime)}`)).json();
    assert.equal(page.numberMatched,count);
  }
  for (const query of ['bbox=1,2,3', 'bbox=1,2,3,NaN', 'limit=-1', 'limit=0', 'offset=1.5', 'datetime=bad', 'datetime=../..', 'fields=id']) {
    const response = await app.request(`${base}/search?${query}`);
    assert.equal(response.status,400,query); assert.equal(response.headers.get('access-control-allow-origin'),'*');
  }
  assert.equal((await app.request(`${base}/search`,{method:'POST',body:'{' })).status,400);
  assert.equal((await app.request(`${base}/search`,{method:'POST',body:JSON.stringify({collections:[]}) })).status,400);
  assert.equal((await app.request(`${base}/collections/missing`)).status,404);
});

test('CORS preflight allows standard JSON search, collection listing follows next links', async () => {
  const { app } = await fixture();
  const response = await app.request(`${base}/search`, { method: 'OPTIONS', headers: {
    Origin: 'https://browser.example', 'Access-Control-Request-Method': 'POST', 'Access-Control-Request-Headers': 'content-type',
  } });
  assert.equal(response.status,204); assert.match(response.headers.get('access-control-allow-methods'),/POST/);
  const page = await (await app.request(`${base}/collections?limit=1`)).json();
  assert.equal(page.collections.length,1);
  const next = await (await app.request(page.links.find(l=>l.rel==='next').href)).json();
  assert.equal(next.collections[0].id,'two');
});

test('index failures propagate and cannot poison later requests with empty cached data', async t => {
  const { handler } = await fixture();
  delete handler.readGeoparquet;
  t.mock.method(globalThis,'fetch',async()=>new Response('',{status:404}));
  await assert.rejects(handler.loadItemsFromGeoparquet('https://example.org/bad'),/unavailable/);
  assert.equal(handler.itemsCache.size,0);
  handler.readGeoparquet = async()=>[item('retry')];
  assert.equal((await handler.loadItemsFromGeoparquet('https://example.org/bad'))[0].id,'retry');
});

test('real legacy and modern Parquet decode WKB, projection, source assets and viewer links', async t => {
  const { app, handler } = await fixture();
  delete handler.readGeoparquet;
  const legacy = await readFile(new URL('./fixtures/stac-legacy.parquet', import.meta.url));
  const modern = await readFile(new URL('./fixtures/stac-modern.parquet', import.meta.url));
  t.mock.method(globalThis, 'fetch', async url => new Response(String(url).includes('/one.') ? legacy : modern));
  const response = await app.request(`${base}/search?collections=one`);
  assert.equal(response.status, 200);
  const old = (await response.json()).features[0];
  assert.equal(old.id, '40D_15');
  assert.equal(old.geometry.coordinates[0].length, 5);
  assert.equal(old.properties['proj:epsg'], 3857);
  assert.ok(old.stac_extensions.some(url => url.includes('/projection/')));
  const xyz = old.links.find(link => link.rel === 'xyz');
  assert.ok(xyz.href.includes('/cog-tiles/{z}/{x}/{y}'));
  assert.equal(new URL(xyz.href).searchParams.get('url'), old.assets.cog.href);
  assert.ok(old.assets.original.href.endsWith('.jpg'));
  const current = (await (await app.request(`${base}/search?collections=two`)).json()).features[0];
  assert.equal(current.geometry.coordinates[0].length, 4, 'preserve the original triangle');
  assert.equal(current.properties['proj:code'], 'EPSG:3857');
  assert.equal(current.properties['proj:transform'].length, 9);
});

function edgeCache(t) {
  const previous = Object.getOwnPropertyDescriptor(globalThis, 'caches');
  const entries = new Map();
  const cache = {
    async match(url) { return entries.get(url)?.clone(); },
    async put(url, response) {
      assert.equal(response.status, 200);
      assert.equal(response.headers.get('Cache-Control'), 'public, max-age=86400');
      assert.equal(response.headers.get('Location'), null, 'do not cache signed redirects');
      entries.set(url, response);
    },
  };
  Object.defineProperty(globalThis, 'caches', { configurable: true, value: { async open() { return cache; } } });
  t.after(() => {
    if (previous) Object.defineProperty(globalThis, 'caches', previous);
    else delete globalThis.caches;
  });
  return { entries, cache };
}

test('validated index bytes survive decoded-cache eviction and a new handler without upstream reads', async t => {
  const { entries } = edgeCache(t);
  const bytes = await readFile(new URL('./fixtures/stac-modern.parquet', import.meta.url));
  const fetch = t.mock.method(globalThis, 'fetch', async () => new Response(bytes));
  const { handler } = await fixture();
  delete handler.readGeoparquet;
  const urls = Array.from({ length: 5 }, (_, i) => `https://example.org/${i}.parquet`);
  for (const url of urls) await handler.loadItemsFromGeoparquet(url);
  assert.equal(handler.itemsCache.has(urls[0]), false, 'first decoded collection was evicted');
  assert.equal(entries.size, 5);
  fetch.mock.mockImplementation(async () => { throw new Error('upstream offline'); });
  const { handler: next } = await fixture();
  delete next.readGeoparquet;
  const first = await handler.loadItemsFromGeoparquet(urls[0]);
  const second = await next.loadItemsFromGeoparquet(urls[0]);
  assert.deepEqual(first, second);
  assert.equal(fetch.mock.callCount(), 5);
});

test('failed and malformed downloads are never cached and can be retried later', async t => {
  const { entries } = edgeCache(t);
  const bytes = await readFile(new URL('./fixtures/stac-modern.parquet', import.meta.url));
  const responses = [new Response('missing', { status: 404 }), new Response('broken parquet'), new Response(bytes)];
  const fetch = t.mock.method(globalThis, 'fetch', async () => responses.shift());
  const { handler } = await fixture();
  delete handler.readGeoparquet;
  const url = 'https://example.org/retry.parquet';
  for (let i = 0; i < 2; i++) {
    await assert.rejects(handler.loadItemsFromGeoparquet(url), /unavailable/);
    assert.equal(entries.size, 0);
  }
  assert.ok((await handler.loadItemsFromGeoparquet(url)).length);
  assert.equal(entries.size, 1);
  assert.equal(fetch.mock.callCount(), 3);
});

test('edge-cache failures do not make readable release indexes unavailable', async t => {
  const { cache } = edgeCache(t);
  t.mock.method(cache, 'match', async () => { throw new Error('cache read failed'); });
  t.mock.method(cache, 'put', async () => { throw new Error('cache write failed'); });
  const bytes = await readFile(new URL('./fixtures/stac-modern.parquet', import.meta.url));
  t.mock.method(globalThis, 'fetch', async () => new Response(bytes));
  const { handler } = await fixture();
  delete handler.readGeoparquet;
  assert.ok((await handler.readGeoparquet('https://example.org/available.parquet')).length);
});

test('viewer follows next links beyond the former 10000-item cap and detects repeated pages', async t => {
  t.mock.method(globalThis,'fetch',async url => {
    const page = Number(new URL(url).searchParams.get('page') || 0);
    return Response.json({ features: Array.from({length:1000},(_,i)=>({id:page*1000+i})),
      links: page<10 ? [{rel:'next',href:`?page=${page+1}`}] : [] });
  });
  const features = await fetchStacPages('https://example.org/items','features');
  assert.equal(features.length,11000); assert.equal(features.at(-1).id,10999);
  t.mock.method(globalThis,'fetch',async()=>Response.json({features:[],links:[{rel:'next',href:'https://example.org/items'}]}));
  await assert.rejects(fetchStacPages('https://example.org/items','features'),/repeated/);
});
