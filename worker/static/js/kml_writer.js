// KML XML generation: geometry serialization, Placemark construction, and document envelope.

// XML-escape text content
function esc(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function coord(c) {
  // KML coordinates: lon,lat[,alt]
  return c.length >= 3 ? `${c[0]},${c[1]},${c[2]}` : `${c[0]},${c[1]}`;
}

function coordRing(ring) {
  return ring.map(coord).join(' ');
}

function linearRing(ring) {
  return `<LinearRing><coordinates>${coordRing(ring)}</coordinates></LinearRing>`;
}

/**
 * Convert a GeoJSON-style geometry { type, coordinates } to a KML geometry string.
 */
export function geometryToKml(geom) {
  switch (geom.type) {
    case 'Point':
      return `<Point><coordinates>${coord(geom.coordinates)}</coordinates></Point>`;

    case 'LineString':
      return `<LineString><coordinates>${coordRing(geom.coordinates)}</coordinates></LineString>`;

    case 'Polygon': {
      const outer = `<outerBoundaryIs>${linearRing(geom.coordinates[0])}</outerBoundaryIs>`;
      const inner = geom.coordinates.slice(1).map(ring =>
        `<innerBoundaryIs>${linearRing(ring)}</innerBoundaryIs>`
      ).join('');
      return `<Polygon>${outer}${inner}</Polygon>`;
    }

    case 'MultiPoint':
      return `<MultiGeometry>${geom.coordinates.map(c =>
        `<Point><coordinates>${coord(c)}</coordinates></Point>`
      ).join('')}</MultiGeometry>`;

    case 'MultiLineString':
      return `<MultiGeometry>${geom.coordinates.map(line =>
        `<LineString><coordinates>${coordRing(line)}</coordinates></LineString>`
      ).join('')}</MultiGeometry>`;

    case 'MultiPolygon':
      return `<MultiGeometry>${geom.coordinates.map(poly => {
        const outer = `<outerBoundaryIs>${linearRing(poly[0])}</outerBoundaryIs>`;
        const inner = poly.slice(1).map(ring =>
          `<innerBoundaryIs>${linearRing(ring)}</innerBoundaryIs>`
        ).join('');
        return `<Polygon>${outer}${inner}</Polygon>`;
      }).join('')}</MultiGeometry>`;

    default:
      return '';
  }
}

function styleUrlForGeom(geomType) {
  if (geomType === 'Point' || geomType === 'MultiPoint') return '#default-point';
  if (geomType === 'LineString' || geomType === 'MultiLineString') return '#default-line';
  return '#default-poly';
}

/**
 * Convert a geometry + properties to a KML Placemark string.
 * @param {{ type: string, coordinates: any }} geom - GeoJSON-style geometry
 * @param {Object} props - Feature properties keyed by column name
 * @param {Array<{ originalName: string }>} attrColumns - Column definitions for ExtendedData
 * @returns {string} KML Placemark XML, or empty string if geometry is unsupported
 */
export function featureToPlacemark(geom, props, attrColumns) {
  const kmlGeom = geometryToKml(geom);
  if (!kmlGeom) return '';

  const styleUrl = `<styleUrl>${styleUrlForGeom(geom.type)}</styleUrl>`;

  const nameProp = props['name'] || props['NAME'] || props['Name'];
  const nameTag = nameProp != null ? `<name>${esc(nameProp)}</name>` : '';

  const dataEntries = attrColumns.map(col => {
    const val = props[col.originalName];
    return `<Data name="${esc(col.originalName)}"><value>${esc(val)}</value></Data>`;
  }).join('');
  const extData = dataEntries ? `<ExtendedData>${dataEntries}</ExtendedData>` : '';

  return `<Placemark>${nameTag}${styleUrl}${extData}${kmlGeom}</Placemark>\n`;
}

export const KML_HEADER = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
<name>Export</name>
<Style id="default-poly">
<LineStyle><color>ff0000ff</color><width>2</width></LineStyle>
<PolyStyle><color>440000ff</color></PolyStyle>
</Style>
<Style id="default-line">
<LineStyle><color>ff0000ff</color><width>2</width></LineStyle>
</Style>
<Style id="default-point">
<IconStyle><color>ff0000ff</color><scale>1</scale></IconStyle>
</Style>
`;

export const KML_FOOTER = `</Document>
</kml>
`;
