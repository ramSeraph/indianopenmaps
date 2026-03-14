// DXF generation: geometry serialization, entity construction, header/tables/footer.
// Outputs AutoCAD R14 (AC1014) DXF with UTM coordinates (meters).
// Properties are stored as XDATA on entities.
//
// Adapted from https://github.com/publicmap/amche-atlas/blob/main/js/dxf-converter.js
// and https://github.com/publicmap/amche-atlas/blob/main/js/dxf-coordinate-transformer.js

const APPID = 'IOM_GEO';

// --- UTM projection (WGS 84 ellipsoid, Snyder series expansion) ---

const WGS84_A = 6378137.0;
const WGS84_F = 1 / 298.257223563;
const WGS84_E2 = 2 * WGS84_F - WGS84_F * WGS84_F;
const WGS84_EP2 = WGS84_E2 / (1 - WGS84_E2);
const UTM_K0 = 0.9996;

const DEG2RAD = Math.PI / 180;

import { getUtmZone, bboxUtmZone } from './utils.js';

/**
 * Convert a single WGS 84 coordinate [lon, lat, alt?] to UTM [easting, northing, alt].
 * Standard Snyder series-expansion formulas; accurate to < 1 m within a UTM zone.
 */
function lonLatToUtm(lon, lat, centralMeridian) {
  const φ = lat * DEG2RAD;
  const λ = lon * DEG2RAD;
  const λ0 = centralMeridian * DEG2RAD;

  const sinφ = Math.sin(φ);
  const cosφ = Math.cos(φ);
  const tanφ = Math.tan(φ);

  const N = WGS84_A / Math.sqrt(1 - WGS84_E2 * sinφ * sinφ);
  const T = tanφ * tanφ;
  const C = WGS84_EP2 * cosφ * cosφ;
  const A = (λ - λ0) * cosφ;
  const A2 = A * A;
  const A4 = A2 * A2;

  const e2 = WGS84_E2;
  const M = WGS84_A * (
    (1 - e2 / 4 - 3 * e2 * e2 / 64 - 5 * e2 * e2 * e2 / 256) * φ
    - (3 * e2 / 8 + 3 * e2 * e2 / 32 + 45 * e2 * e2 * e2 / 1024) * Math.sin(2 * φ)
    + (15 * e2 * e2 / 256 + 45 * e2 * e2 * e2 / 1024) * Math.sin(4 * φ)
    - (35 * e2 * e2 * e2 / 3072) * Math.sin(6 * φ)
  );

  const easting = UTM_K0 * N * (
    A + (1 - T + C) * A2 * A / 6
    + (5 - 18 * T + T * T + 72 * C - 58 * WGS84_EP2) * A4 * A / 120
  ) + 500000;

  const northing = UTM_K0 * (
    M + N * tanφ * (
      A2 / 2
      + (5 - T + 9 * C + 4 * C * C) * A4 / 24
      + (61 - 58 * T + T * T + 600 * C - 330 * WGS84_EP2) * A4 * A2 / 720
    )
  );

  return [easting, northing];
}

/**
 * Create a coordinate transform function for a given UTM zone.
 * @returns {(coord: number[]) => number[]} Transform [lon,lat,alt?] → [easting,northing,alt]
 */
export function createUtmTransform(zone, hemisphere) {
  const centralMeridian = (zone - 1) * 6 - 180 + 3;
  const falseNorthing = hemisphere === 'S' ? 10000000 : 0;
  return (coord) => {
    const [lon, lat, alt = 0] = coord;
    const [e, n] = lonLatToUtm(lon, lat, centralMeridian);
    return [e, n + falseNorthing, alt];
  };
}

// --- Geometry coordinate transform ---

function transformGeometry(geom, transform) {
  if (!transform || !geom) return geom;
  switch (geom.type) {
    case 'Point':
      return { type: 'Point', coordinates: transform(geom.coordinates) };
    case 'LineString':
      return { type: 'LineString', coordinates: geom.coordinates.map(transform) };
    case 'Polygon':
      return { type: 'Polygon', coordinates: geom.coordinates.map(r => r.map(transform)) };
    case 'MultiPoint':
      return { type: 'MultiPoint', coordinates: geom.coordinates.map(transform) };
    case 'MultiLineString':
      return { type: 'MultiLineString', coordinates: geom.coordinates.map(l => l.map(transform)) };
    case 'MultiPolygon':
      return { type: 'MultiPolygon', coordinates: geom.coordinates.map(p => p.map(r => r.map(transform))) };
    case 'GeometryCollection':
      return { type: 'GeometryCollection', geometries: geom.geometries.map(g => transformGeometry(g, transform)) };
    default:
      return geom;
  }
}

// --- DXF helpers ---

function sanitizeLayerName(name) {
  return String(name).replace(/[<>:"/\\|?*\x00-\x1F]/g, '_').substring(0, 255);
}

function getFeatureLayer(props) {
  if (!props) return 'DEFAULT';
  if (props.layer) return String(props.layer);
  if (props.layerName) return String(props.layerName);
  return 'DEFAULT';
}

function propertiesToXData(props) {
  if (!props || typeof props !== 'object') return '';
  const entries = Object.entries(props).filter(([, v]) => v != null);
  if (entries.length === 0) return '';

  let xdata = `1001\n${APPID}\n`;
  for (const [key, value] of entries) {
    xdata += `1000\n${String(key).substring(0, 255)}\n`;
    xdata += `1000\n${String(value).substring(0, 255)}\n`;
  }
  return xdata;
}

// --- Geometry to DXF entity strings ---
// XDATA is passed into each generator and placed directly after the entity's
// own group codes, before any child entities (VERTEXes) or SEQEND.
// This avoids the fragile post-hoc string search that broke on group-code
// values of "0" (e.g. polyline flag 70=0, z-coordinate 30=0).

function pointToDxf(coordinates, layer, xdata) {
  const [x, y, z = 0] = coordinates;
  return `0\nPOINT\n8\n${layer}\n10\n${x}\n20\n${y}\n30\n${z}\n${xdata}`;
}

function lineStringToDxf(coordinates, layer, xdata) {
  let dxf = `0\nPOLYLINE\n8\n${layer}\n66\n1\n70\n0\n${xdata}`;
  for (const coord of coordinates) {
    const [x, y, z = 0] = coord;
    dxf += `0\nVERTEX\n8\n${layer}\n10\n${x}\n20\n${y}\n30\n${z}\n`;
  }
  dxf += '0\nSEQEND\n';
  return dxf;
}

function polygonToDxf(rings, layer, xdata) {
  let dxf = '';
  for (let i = 0; i < rings.length; i++) {
    // Attach XDATA only to the outer ring's POLYLINE
    dxf += `0\nPOLYLINE\n8\n${layer}\n66\n1\n70\n1\n${i === 0 ? xdata : ''}`;
    for (const coord of rings[i]) {
      const [x, y, z = 0] = coord;
      dxf += `0\nVERTEX\n8\n${layer}\n10\n${x}\n20\n${y}\n30\n${z}\n`;
    }
    dxf += '0\nSEQEND\n';
  }
  return dxf;
}

function geometryToDxf(geom, layer, xdata) {
  if (!geom) return '';
  switch (geom.type) {
    case 'Point':
      return pointToDxf(geom.coordinates, layer, xdata);
    case 'LineString':
      return lineStringToDxf(geom.coordinates, layer, xdata);
    case 'Polygon':
      return polygonToDxf(geom.coordinates, layer, xdata);
    case 'MultiPoint':
      return geom.coordinates.map((c, i) => pointToDxf(c, layer, i === 0 ? xdata : '')).join('');
    case 'MultiLineString':
      return geom.coordinates.map((line, i) => lineStringToDxf(line, layer, i === 0 ? xdata : '')).join('');
    case 'MultiPolygon':
      return geom.coordinates.map((rings, i) => polygonToDxf(rings, layer, i === 0 ? xdata : '')).join('');
    case 'GeometryCollection':
      return (geom.geometries || []).map((g, i) => geometryToDxf(g, layer, i === 0 ? xdata : '')).join('');
    default:
      return '';
  }
}

/**
 * Convert a geometry + properties to DXF entity string(s).
 * Coordinates are projected via transform before serialization.
 * Returns { dxf, layerName } so the caller can collect layer names.
 * @param {object} geom - GeoJSON-style geometry (lon/lat)
 * @param {object} props - Feature properties
 * @param {Function} transform - Coordinate transform ([lon,lat,alt?] → [x,y,z])
 */
export function featureToDxfEntities(geom, props, transform) {
  const rawLayer = getFeatureLayer(props);
  const layerName = sanitizeLayerName(rawLayer);
  const xdata = propertiesToXData(props);
  const projected = transformGeometry(geom, transform);
  const dxf = geometryToDxf(projected, layerName, xdata);
  return { dxf, layerName };
}

/** Generate the DXF header section. Units = meters (INSUNITS 6). */
function generateHeader() {
  return '0\nSECTION\n2\nHEADER\n'
    + '9\n$ACADVER\n1\nAC1014\n'
    + '9\n$INSUNITS\n70\n6\n'
    + '0\nENDSEC\n';
}

/**
 * Generate the TABLES section with layer definitions and required linetypes/appid.
 * @param {Map<string, number>} layers - Map of layer name → ACI colour index
 */
function generateTables(layers) {
  let t = '0\nSECTION\n2\nTABLES\n';

  // LTYPE table
  t += '0\nTABLE\n2\nLTYPE\n70\n1\n';
  t += '0\nLTYPE\n2\nCONTINUOUS\n70\n0\n3\nSolid line\n72\n65\n73\n0\n40\n0.0\n';
  t += '0\nENDTAB\n';

  // LAYER table
  t += `0\nTABLE\n2\nLAYER\n70\n${layers.size + 1}\n`;
  t += '0\nLAYER\n2\n0\n70\n0\n62\n7\n6\nCONTINUOUS\n';
  for (const [name, color] of layers) {
    t += `0\nLAYER\n2\n${name}\n70\n0\n62\n${color}\n6\nCONTINUOUS\n`;
  }
  t += '0\nENDTAB\n';

  // APPID table
  t += '0\nTABLE\n2\nAPPID\n70\n1\n';
  t += `0\nAPPID\n2\n${APPID}\n70\n0\n`;
  t += '0\nENDTAB\n';

  t += '0\nENDSEC\n';
  return t;
}

/**
 * Build the DXF preamble (HEADER + TABLES + BLOCKS + ENTITIES section start)
 * and the footer (ENTITIES section end + EOF).
 * @param {Set<string>} layerNames - Set of layer names encountered during streaming
 */
export function buildDxfEnvelope(layerNames) {
  // Assign ACI colours (1–255) round-robin, skip 0 (BYBLOCK) and 7 (white, used by layer 0)
  const layers = new Map();
  let colorIndex = 1;
  for (const name of layerNames) {
    if (name === '0') continue;
    layers.set(name, colorIndex);
    colorIndex = colorIndex >= 254 ? 1 : colorIndex + 1;
    if (colorIndex === 7) colorIndex = 8;
  }

  const header = generateHeader()
    + generateTables(layers)
    + '0\nSECTION\n2\nBLOCKS\n0\nENDSEC\n'
    + '0\nSECTION\n2\nENTITIES\n';

  const footer = '0\nENDSEC\n0\nEOF\n';

  return { header, footer };
}
