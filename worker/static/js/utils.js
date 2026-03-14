export function formatSize(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export function proxyUrl(url, { absolute = false } = {}) {
  const path = `/proxy?url=${encodeURIComponent(url)}`;
  return absolute ? `${window.location.origin}${path}` : path;
}

// OPFS file/dir name prefixes — each followed by TAB_ID.
// Longest prefixes first so extractTabId matches greedily.
const OPFS_PREFIXES = [];
function opfsPrefix(value) {
  OPFS_PREFIXES.push(value);
  return value;
}

export const OPFS_PREFIX_GPKG_TMP = opfsPrefix('dl_gpkg_tmp_');
export const OPFS_PREFIX_GPKG = opfsPrefix('dl_gpkg_');
export const OPFS_PREFIX_OUTPUT = opfsPrefix('dl_output_');
export const OPFS_PREFIX_TMP = opfsPrefix('dl_tmp_');
export const OPFS_PREFIX_SHP_TMP = opfsPrefix('dl_shp_tmp_');
export const OPFS_PREFIX_KML_TMP = opfsPrefix('dl_kml_tmp_');
export const OPFS_PREFIX_TMPDIR = opfsPrefix('tmpdir_');

export function getOpfsPrefixes() {
  return OPFS_PREFIXES;
}

/**
 * Maps 0–100 progress to a sub-range of a parent progress handler.
 * Supports nesting: a ScopedProgress can wrap another ScopedProgress.
 */
export class ScopedProgress {
  constructor(onProgress, start, end) {
    this._onProgress = onProgress;
    this._start = start;
    this._end = end;
    this.callback = this.report.bind(this);
  }

  report(pct) {
    const clamped = Math.max(0, Math.min(100, pct));
    const mapped = this._start + (clamped / 100) * (this._end - this._start);
    this._onProgress?.(Math.round(mapped));
  }
}

export async function getStorageEstimate() {
  const { usage, quota } = await navigator.storage.estimate();
  return { usage, quota };
}

export function fileToAsyncBuffer(file) {
  return {
    byteLength: file.size,
    slice(start, end) { return file.slice(start, end).arrayBuffer(); }
  };
}

// --- WKB hex → GeoJSON-style geometry objects ---

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2)
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  return bytes;
}

function readCoord(view, offset, le, coordSize) {
  const x = view.getFloat64(offset, le);
  const y = view.getFloat64(offset + 8, le);
  return { coord: [x, y], offset: offset + coordSize * 8 };
}

function readPoint(view, offset, le, coordSize) {
  const { coord, offset: newOffset } = readCoord(view, offset, le, coordSize);
  return { geom: { type: 'Point', coordinates: coord }, offset: newOffset };
}

function readLineString(view, offset, le, coordSize) {
  const numPoints = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numPoints; i++) {
    const { coord, offset: newOffset } = readCoord(view, offset, le, coordSize);
    coords.push(coord);
    offset = newOffset;
  }
  return { geom: { type: 'LineString', coordinates: coords }, offset };
}

function readPolygon(view, offset, le, coordSize) {
  const numRings = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const rings = [];
  for (let r = 0; r < numRings; r++) {
    const numPoints = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
    offset += 4;
    const ring = [];
    for (let i = 0; i < numPoints; i++) {
      const { coord, offset: newOffset } = readCoord(view, offset, le, coordSize);
      ring.push(coord);
      offset = newOffset;
    }
    rings.push(ring);
  }
  return { geom: { type: 'Polygon', coordinates: rings }, offset };
}

function readMultiPoint(view, offset, le, coordSize) {
  const numGeoms = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numGeoms; i++) {
    const { geom, offset: newOffset } = readGeometry(view, offset);
    coords.push(geom.coordinates);
    offset = newOffset;
  }
  return { geom: { type: 'MultiPoint', coordinates: coords }, offset };
}

function readMultiLineString(view, offset, le, coordSize) {
  const numGeoms = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numGeoms; i++) {
    const { geom, offset: newOffset } = readGeometry(view, offset);
    coords.push(geom.coordinates);
    offset = newOffset;
  }
  return { geom: { type: 'MultiLineString', coordinates: coords }, offset };
}

function readMultiPolygon(view, offset, le, coordSize) {
  const numGeoms = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;
  const coords = [];
  for (let i = 0; i < numGeoms; i++) {
    const { geom, offset: newOffset } = readGeometry(view, offset);
    coords.push(geom.coordinates);
    offset = newOffset;
  }
  return { geom: { type: 'MultiPolygon', coordinates: coords }, offset };
}

function readGeometry(view, offset) {
  const le = view.getUint8(offset) === 1;
  offset += 1;
  const rawType = le ? view.getUint32(offset, true) : view.getUint32(offset, false);
  offset += 4;

  // Handle ISO WKB type codes (base + Z/M/ZM offsets in thousands)
  const baseType = rawType % 1000;
  const hasZ = rawType >= 1000 && rawType < 2000 || rawType >= 3000;
  const coordSize = hasZ ? 3 : 2;

  switch (baseType) {
    case 1: return readPoint(view, offset, le, coordSize);
    case 2: return readLineString(view, offset, le, coordSize);
    case 3: return readPolygon(view, offset, le, coordSize);
    case 4: return readMultiPoint(view, offset, le, coordSize);
    case 5: return readMultiLineString(view, offset, le, coordSize);
    case 6: return readMultiPolygon(view, offset, le, coordSize);
    default: return { geom: { type: 'Unknown', coordinates: [] }, offset };
  }
}

/**
 * Parse a WKB geometry (as hex string) into { type, coordinates }.
 * Supports Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon.
 */
export function parseWkbHex(hex) {
  const bytes = hexToBytes(hex);
  const view = new DataView(bytes.buffer);
  const result = readGeometry(view, 0);
  return result.geom;
}

