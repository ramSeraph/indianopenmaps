import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BaseLayerPicker, getDefaultBaseLayers } from '../static/js/base_layer_picker.js';
import { loadProtomapsStyle, resolveBaseLayerName } from '../static/js/protomaps_basemaps.js';

function mockMap() {
  const sources = new Map([['terrain', {}], ['user-data', {}]]);
  const layers = [
    { id: 'hills', source: 'terrain' },
    { id: 'user-overlay', source: 'user-data' },
  ];
  return {
    getLayer: id => layers.find(layer => layer.id === id),
    getSource: id => sources.get(id),
    getStyle: () => ({ layers }),
    getGlyphs: () => '',
    setGlyphs() {},
    setSprite() {},
    addSource(id, source) { assert.ok(!sources.has(id)); sources.set(id, source); },
    removeSource(id) { assert.ok(!layers.some(layer => layer.source === id)); sources.delete(id); },
    addLayer(layer, beforeId) {
      assert.ok(!layers.some(existing => existing.id === layer.id));
      const index = layers.findIndex(existing => existing.id === beforeId);
      layers.splice(index < 0 ? layers.length : index, 0, layer);
    },
    removeLayer(id) { layers.splice(layers.findIndex(layer => layer.id === id), 1); },
  };
}

function picker() {
  const instance = new BaseLayerPicker({
    map: mockMap(),
    terrainHandler: { HILLSHADE_LAYER_ID: 'hills' },
    searchParams: { updateBaseLayer(name) { this.name = name; } },
  });
  instance.baseLayers = getDefaultBaseLayers();
  instance.statusElement = { textContent: '' };
  return instance;
}

function deferred() {
  let resolve;
  const promise = new Promise(done => { resolve = done; });
  return { promise, resolve };
}

test('Carto links migrate; corrected OSM and other raster basemaps remain', () => {
  assert.equal(resolveBaseLayerName('Carto OSM Dark'), 'Protomaps Black');
  assert.equal(resolveBaseLayerName('CartoDB Light'), 'Protomaps White');
  assert.equal(resolveBaseLayerName('Protomaps Dark'), 'Protomaps Black');
  assert.equal(resolveBaseLayerName('Protomaps Light'), 'Protomaps White');
  const layers = getDefaultBaseLayers();
  assert.deepEqual(layers.filter(layer => layer.styleUrl).map(layer => layer.name), [
    'Protomaps Black', 'Protomaps White', 'Protomaps Grayscale',
  ]);
  const osm = layers.find(layer => layer.name === 'OSM Carto');
  assert.ok(osm.sources['osm-carto'].tiles.every(url => url.startsWith('ibc://')));
  for (const name of ['ESRI World Imagery', 'Google Streets', 'Google Hybrid']) {
    assert.ok(layers.find(layer => layer.name === name));
  }
});

test('SOI replaces both boundary layers with the theme appearance and double line width', async t => {
  const paint = { 'line-color': '#adadad', 'line-width': 0.4, 'line-dasharray': [2, 1] };
  const layout = { 'line-join': 'round' };
  const style = {
    version: 8, sources: { protomaps: { type: 'vector', tiles: ['https://example.test/{z}/{x}/{y}.mvt'] } },
    layers: [
      { id: 'background', type: 'background' },
      { id: 'boundaries_country', type: 'line', source: 'protomaps', 'source-layer': 'boundaries' },
      { id: 'boundaries', type: 'line', source: 'protomaps', 'source-layer': 'boundaries', filter: ['>', 'kind_detail', 2], paint, layout },
      { id: 'labels', type: 'symbol', source: 'protomaps', 'source-layer': 'places' },
    ], glyphs: 'https://example.test/fonts/{fontstack}/{range}.pbf', sprite: 'https://example.test/light',
  };
  const boundary = {
    vector_layers: [{ id: 'SOI_States' }], tiles: ['/states/soi/{z}/{x}/{y}.pbf'],
    minzoom: 0, maxzoom: 11, bounds: [68, 6, 98, 38], attribution: 'Survey of India / Datameet',
  };
  t.mock.method(globalThis, 'fetch', async url => ({ ok: true, json: async () => url === '/states/soi/tiles.json' ? boundary : style }));
  const result = await loadProtomapsStyle({ styleUrl: 'https://example.test/style.json' });
  const soi = result.layers.find(layer => layer.id === 'basemap-soi-boundaries');
  assert.deepEqual(soi.paint, { ...paint, 'line-width': 0.8 });
  assert.deepEqual(soi.layout, layout);
  assert.equal(soi.filter, undefined);
  assert.equal(soi['source-layer'], 'SOI_States');
  assert.equal(result.sources[soi.source].maxzoom, 11);
  assert.equal(result.sources[soi.source].attribution, boundary.attribution);
  assert.ok(!result.layers.some(layer => layer['source-layer'] === 'boundaries'));
  assert.deepEqual(result.layers.map(layer => layer.id), ['basemap-background', 'basemap-soi-boundaries', 'basemap-labels']);
  assert.equal(result.layers[0].source, undefined);
  assert.equal(result.sprite, style.sprite);
});

test('a failed style fetch leaves the current basemap, overlay and URL intact', async t => {
  const instance = picker();
  await instance.switchLayer('ESRI World Imagery');
  const oldLayers = [...instance.map.getStyle().layers];
  t.mock.method(instance, 'loadLayerInfo', async () => { throw new Error('offline'); });
  t.mock.method(console, 'error', () => {});
  await instance.switchLayer('Protomaps White');
  assert.equal(instance.currentLayerName, 'ESRI World Imagery');
  assert.deepEqual(instance.map.getStyle().layers, oldLayers);
  assert.equal(instance.searchParams.name, 'ESRI World Imagery');
  assert.match(instance.statusElement.textContent, /Could not load/);
});

test('latest selection wins when metadata requests finish out of order', async t => {
  const instance = picker();
  const slow = deferred();
  t.mock.method(instance, 'loadLayerInfo', async info => {
    if (info.name === 'Google Streets') await slow.promise;
  });
  const pending = instance.switchLayer('Google Streets');
  await instance.switchLayer('Google Hybrid');
  slow.resolve();
  await pending;
  assert.equal(instance.currentLayerName, 'Google Hybrid');
  assert.deepEqual(instance.map.getStyle().layers.map(layer => layer.id), ['google-hybrid-layer', 'hills', 'user-overlay']);
  assert.ok(instance.map.getSource('user-data'));
});

test('reselecting the active basemap cancels a pending theme switch', async t => {
  const instance = picker();
  await instance.switchLayer('ESRI World Imagery');
  const slow = deferred();
  t.mock.method(instance, 'loadLayerInfo', () => slow.promise);
  const pending = instance.switchLayer('Google Streets');
  await instance.switchLayer('ESRI World Imagery');
  slow.resolve();
  await pending;
  assert.equal(instance.currentLayerName, 'ESRI World Imagery');
  assert.equal(instance.statusElement.textContent, '');
});
