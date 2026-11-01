import assert from 'node:assert/strict';
import { test } from 'node:test';
import { readFile } from 'node:fs/promises';
import { Hono } from 'hono';
import { registerStacRoutes } from '../src/routes/stac.js';
import { bboxGeometry } from '../src/lib/stac_query.js';
import { fetchStacPages } from '../static/js/stac_pagination.js';
import { RangeCache } from '../src/lib/range_cache.js';

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
  handler.parquet.rangeCache = new RangeCache();
  handler.readGeoparquet = async (url, options) => selectRows(url.includes('/one.') ? Array.from({ length: 1003 }, (_, i) => item(`a-${i}`)) : [item('b-0')], options);
  return { app, handler };
}

function selectRows(rows, { positions, rowStart = 0, rowEnd = rows.length } = {}) {
  return positions ? positions.map(i => rows[i]) : rows.slice(rowStart, rowEnd);
}

function rangeResponse(bytes, init = {}) {
  if (init.method === 'HEAD') return new Response(null, { headers: { 'Content-Length': String(bytes.length) } });
  const [, start, end] = /^bytes=(\d+)-(\d+)$/.exec(new Headers(init.headers).get('Range'));
  return new Response(bytes.subarray(Number(start), Number(end) + 1), { status: 206, headers: {
    'Content-Range': `bytes ${start}-${end}/${bytes.length}`, 'Content-Length': String(Number(end) + 1 - Number(start)),
  } });
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

test('unfiltered pages use published counts without reading unrelated collection indexes', async t => {
  for (const [offset, expected, reads] of [[0, ['a-0', 'a-1'], 1], [1002, ['a-1002', 'b-0'], 2], [1004, [], 0]]) {
    const { app, handler } = await fixture();
    for (const [id, count] of [['one', 1003], ['two', 1]]) {
      handler.collections.get(id).metadata = { ...handler.collections.get(id).metadata, item_count: count };
    }
    const read = t.mock.method(handler, 'readGeoparquet');
    const page = await (await app.request(`${base}/search?limit=2&offset=${offset}`)).json();
    assert.equal(page.numberMatched, 1004);
    assert.deepEqual(page.features.map(f => f.id), expected);
    assert.equal(read.mock.callCount(), reads);
  }
});

test('metadata count shortcut does not apply to filtered queries', async t => {
  const { app, handler } = await fixture();
  for (const [id, count] of [['one', 1003], ['two', 1]]) {
    handler.collections.get(id).metadata = { ...handler.collections.get(id).metadata, item_count: count };
  }
  const read = t.mock.method(handler, 'readGeoparquet');
  const page = await (await app.request(`${base}/search?ids=b-0&limit=1`)).json();
  assert.equal(page.numberMatched, 1);
  assert.deepEqual(page.features.map(f => f.id), ['b-0']);
  assert.equal(read.mock.callCount(), 3, 'read both search indexes, then only the matching item');
});

test('spatial search uses real polygons, holes, touching edges and dateline bboxes', async () => {
  const { app, handler } = await fixture();
  const triangle = item('triangle', [0, 0, 10, 10]);
  triangle.geometry = { type: 'Polygon', coordinates: [[[0,0],[10,0],[0,10],[0,0]], [[1,1],[1,2],[2,2],[2,1],[1,1]]] };
  handler.readGeoparquet = async (url, options) => selectRows([triangle, item('dateline', [178,0,179,1])], options);
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

test('the search cache retains only geometry, IDs and dates, not full asset metadata', async t => {
  const { handler } = await fixture();
  const description = 'a'.repeat(8192) + '地図';
  const rows = Array.from({ length: 2048 }, (_, i) => ({ ...item(`large-${i}`), properties: { description } }));
  const read = t.mock.method(handler, 'readGeoparquet', async () => rows);
  const url = 'https://example.org/large.parquet';
  await handler.loadItemsFromGeoparquet(url);
  const cached = handler.itemsCache.get(url);
  assert.ok(cached.items);
  assert.ok(cached.bytes < 1024 * 1024);
  const warm = await handler.loadItemsFromGeoparquet(url);
  assert.equal(warm.length, 2048);
  assert.equal(warm.at(-1).id, 'large-2047');
  assert.equal(warm.at(-1).properties.description, undefined);
  assert.equal(warm.at(-1).assets, undefined);
  assert.equal(read.mock.callCount(), 1);
});

test('real legacy and modern Parquet decode WKB, projection, source assets and viewer links', async t => {
  const { app, handler } = await fixture();
  delete handler.readGeoparquet;
  const legacy = await readFile(new URL('./fixtures/stac-legacy.parquet', import.meta.url));
  const modern = await readFile(new URL('./fixtures/stac-modern.parquet', import.meta.url));
  t.mock.method(globalThis, 'fetch', async (url, init) => rangeResponse(String(url).includes('/one.') ? legacy : modern, init));
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
