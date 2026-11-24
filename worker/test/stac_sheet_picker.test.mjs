import assert from 'node:assert/strict';
import test from 'node:test';
import { coversPoint, sheetName } from '../static/stac/sheet-picker.js';

const box = (x, y, size) => [
  [x, y],
  [x + size, y],
  [x + size, y + size],
  [x, y + size],
  [x, y]
];
const layer = geometry => ({ feature: { geometry }, getBounds: () => ({ contains: () => true }) });
const hits = (item, lng, lat) => coversPoint(item, { lng, lat }, new Set());

test('a shared edge picks both sheets and respects holes and detached insets', () => {
  const west = layer({ type: 'MultiPolygon', coordinates: [[box(0, 0, 4), box(1, 1, 2)], [box(8, 0, 1)]] });
  const east = layer({ type: 'Polygon', coordinates: [box(4, 0, 4)] });
  assert.ok(hits(west, 4, 2) && hits(east, 4, 2));
  assert.ok(hits(west, 1, 2));
  assert.equal(hits(west, 2, 2), false);
  assert.ok(hits(west, 8.5, 0.5));
  assert.equal(hits(west, 6, 0.5), false);
  assert.ok(hits(west, 0, 0));
});

test('same-year full sheet and separate inset have distinct picker names', () => {
  const item = {
    collection: 'indian-atlas-253k',
    id: 'indian-atlas_138_full_1830_epsg3857.cog',
    properties: { 'iomaps:source_properties': { role: 'combined-cog' } }
  };
  assert.match(sheetName(item), /138.*with inset.*1830/);
  assert.match(
    sheetName({ ...item, id: 'indian-atlas_138_full_1830_inset-01_epsg3857.cog', properties: {} }),
    /138.*1830.*Inset/
  );
  assert.equal(sheetName({ collection: 'other', id: '44/B' }), '44/B');
});
