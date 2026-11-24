// Use the published boundaries. The simplified paths Leaflet draws at low zoom
// can span small holes and move joins between neighbouring sheets.
function ringContains(ring, x, y) {
  let inside = false;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [ax, ay] = ring[j],
      [bx, by] = ring[i];
    if (
      x >= Math.min(ax, bx) &&
      x <= Math.max(ax, bx) &&
      y >= Math.min(ay, by) &&
      y <= Math.max(ay, by) &&
      (x - ax) * (by - ay) === (y - ay) * (bx - ax)
    )
      return 'edge';
    if (ay > y !== by > y && x < ax + ((y - ay) * (bx - ax)) / (by - ay)) inside = !inside;
  }
  return inside;
}

function polygonContains(rings, x, y) {
  const exterior = ringContains(rings[0], x, y);
  if (exterior === 'edge') return true;
  if (!exterior) return false;
  for (const hole of rings.slice(1)) {
    const result = ringContains(hole, x, y);
    if (result === 'edge') return true;
    if (result) return false;
  }
  return true;
}

export function coversPoint(layer, latlng, hitElements) {
  const geometry = layer.feature?.geometry;
  if (geometry?.type === 'Polygon' || geometry?.type === 'MultiPolygon') {
    if (!layer.getBounds().contains(latlng)) return false;
    const polygons = geometry.type === 'Polygon' ? [geometry.coordinates] : geometry.coordinates;
    return polygons.some(polygon => polygonContains(polygon, latlng.lng, latlng.lat));
  }
  // Preserve the clickable size of point and line symbols.
  if (layer.getLayers) return layer.getLayers().some(part => coversPoint(part, latlng, hitElements));
  return hitElements.has(layer.getElement?.());
}

export function sheetName(item) {
  const title = item.properties?.title;
  if (title) return title;
  const atlas =
    item.collection === 'indian-atlas-253k' &&
    /^indian-atlas_([^_]+)_(full|partial|ne|nw|se|sw)_([^_]+)(.*?)_epsg3857\.cog$/i.exec(item.id);
  if (!atlas) return item.id;
  const [, sheet, part, date, suffix] = atlas;
  const partName = { full: 'Full sheet', partial: 'Partial sheet' }[part] || part.toUpperCase();
  const variants = {
    color: 'Colour',
    'color-rgs': 'Colour, RGS',
    rgs: 'RGS',
    'main-only': 'Main only',
    'inset-01': 'Inset',
    'frame-trial': 'Frame trial',
    'reassembly-trial': 'Reassembly trial'
  };
  const role = item.properties?.['iomaps:source_properties']?.role;
  const partLabel =
    { inset: 'Inset', 'main-component': 'Main sheet only', 'combined-cog': 'Full sheet with inset' }[role] || partName;
  const labels = [
    'Sheet ' + sheet.toUpperCase(),
    partLabel,
    date === 'nd' ? 'Undated' : item.properties?.['iomaps:source_properties']?.date_label || date
  ];
  for (const token of suffix.split('_').filter(Boolean)) {
    if ((token === 'inset-01' && role === 'inset') || (token === 'main-only' && role === 'main-component')) continue;
    labels.push(variants[token] || token.replaceAll('-', ' '));
  }
  return labels.join(' · ');
}
