// Shapefile binary format: WKB parsing, ShpWriter, DbfWriter, and helpers.
// ShpWriter and DbfWriter adapted from shp-write-stream by Calvin Metcalf (MIT license),
// rewritten from Node Buffers/streams to DataView/Uint8Array for browser use.
// https://github.com/calvinmetcalf/shp-write-stream

// --- WKB hex → GeoJSON-style coordinate arrays ---

function hexToBytes(hex) {
  const bytes = new Uint8Array(hex.length / 2);
  for (let i = 0; i < hex.length; i += 2)
    bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
  return bytes;
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

function readCoord(view, offset, le, coordSize) {
  const x = view.getFloat64(offset, le);
  const y = view.getFloat64(offset + 8, le);
  // Skip Z (and M) coordinates if present — we only use X,Y for shapefiles
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

// --- Geometry type mapping and promotion ---

// Maps DuckDB geometry type to base shapefile type.
// Point and MultiPoint are separate shp types; lines/polygons unify naturally.
const GEOM_TYPE_TO_SHP_BASE = {
  'POINT': 'point', 'MULTIPOINT': 'multipoint',
  'LINESTRING': 'line', 'MULTILINESTRING': 'line',
  'POLYGON': 'polygon', 'MULTIPOLYGON': 'polygon',
};

// Promote single → multi so we can unify into one file per base type
const PROMOTE_MAP = {
  'Point': 'MultiPoint', 'LineString': 'MultiLineString', 'Polygon': 'MultiPolygon',
};

export function promoteGeometry(geom) {
  const promoted = PROMOTE_MAP[geom.type];
  if (!promoted) return geom;
  switch (geom.type) {
    case 'Point':
      return { type: 'MultiPoint', coordinates: [geom.coordinates] };
    case 'LineString':
      return { type: 'MultiLineString', coordinates: [geom.coordinates] };
    case 'Polygon':
      return { type: 'MultiPolygon', coordinates: [geom.coordinates] };
  }
}

/**
 * Given the set of DuckDB geometry type names, resolve which shp file types
 * to create and which DuckDB types need promotion.
 * Returns { shpTypes: string[], typeMapping: Map<duckdbType, { shpType, needsPromote }> }
 */
export function resolveShpTypeMapping(duckdbGeomTypes) {
  const typeMapping = new Map();
  const hasPoint = duckdbGeomTypes.has('POINT');
  const hasMultiPoint = duckdbGeomTypes.has('MULTIPOINT');

  for (const t of duckdbGeomTypes) {
    if (t === 'POINT' && hasMultiPoint) {
      // Merge Point into MultiPoint file
      typeMapping.set(t, { shpType: 'multipoint', needsPromote: true });
    } else if (t === 'POINT') {
      typeMapping.set(t, { shpType: 'point', needsPromote: false });
    } else {
      const shpType = GEOM_TYPE_TO_SHP_BASE[t];
      if (shpType) typeMapping.set(t, { shpType, needsPromote: false });
    }
  }

  const shpTypes = [...new Set([...typeMapping.values()].map(v => v.shpType))];
  return { shpTypes, typeMapping };
}

// --- Shapefile type codes ---

const SHP_TYPE_MAP = { point: 1, multipoint: 8, line: 3, polygon: 5 };

// --- DBF field name truncation ---

/**
 * Truncate field names to 10 chars (DBF limit), resolving clashes.
 * Returns an array of { originalName, dbfName } objects.
 */
export function truncateFieldNames(names) {
  const result = [];
  const usedNames = new Set();

  for (const name of names) {
    let truncated = name.slice(0, 10);
    if (!usedNames.has(truncated.toUpperCase())) {
      usedNames.add(truncated.toUpperCase());
      result.push({ originalName: name, dbfName: truncated });
      continue;
    }
    // Clash: append numeric suffix
    for (let suffix = 1; suffix <= 999; suffix++) {
      const suffixStr = String(suffix);
      const candidate = name.slice(0, 10 - suffixStr.length) + suffixStr;
      if (!usedNames.has(candidate.toUpperCase())) {
        usedNames.add(candidate.toUpperCase());
        result.push({ originalName: name, dbfName: candidate });
        break;
      }
    }
  }
  return result;
}

// --- ShpWriter: writes .shp record data and generates header ---

export class ShpWriter {
  constructor(type) {
    this.type = type;
    this.shpTypeCode = SHP_TYPE_MAP[type];
    this.minx = Infinity;
    this.miny = Infinity;
    this.maxx = -Infinity;
    this.maxy = -Infinity;
    this.shpLength = 50; // header is 50 16-bit words
    this.recNum = 0;
    this.shxRecords = [];
    this._pendingChunks = [];
  }

  writeRecord(geom) {
    const data = this._createRecord(geom);
    this._pendingChunks.push(data);
    const lenWords = data.byteLength / 2;
    this.shxRecords.push({ offset: this.shpLength, length: lenWords });
    this.shpLength += lenWords;
  }

  /** Return and clear pending record chunks (call after each row group). */
  flushChunks() {
    const chunks = this._pendingChunks;
    this._pendingChunks = [];
    return chunks;
  }

  /** Generate .shp file header (100 bytes). */
  generateShpHeader() {
    const header = new ArrayBuffer(100);
    const hView = new DataView(header);
    hView.setInt32(0, 9994, false); // magic
    hView.setInt32(24, this.shpLength, false);
    hView.setInt32(28, 1000, true);
    hView.setInt32(32, this.shpTypeCode, true);
    this._writeBbox(hView, 36, [this.minx, this.miny, this.maxx, this.maxy]);
    return new Uint8Array(header);
  }

  /** Generate .shx file parts: { header, records }. */
  generateShxParts() {
    const shxLen = 50 + this.shxRecords.length * 4;
    const header = new ArrayBuffer(100);
    const hView = new DataView(header);
    hView.setInt32(0, 9994, false);
    hView.setInt32(24, shxLen, false);
    hView.setInt32(28, 1000, true);
    hView.setInt32(32, this.shpTypeCode, true);
    this._writeBbox(hView, 36, [this.minx, this.miny, this.maxx, this.maxy]);

    const records = new ArrayBuffer(this.shxRecords.length * 8);
    const rView = new DataView(records);
    for (let i = 0; i < this.shxRecords.length; i++) {
      rView.setInt32(i * 8, this.shxRecords[i].offset, false);
      rView.setInt32(i * 8 + 4, this.shxRecords[i].length, false);
    }
    return { header: new Uint8Array(header), records: new Uint8Array(records) };
  }

  // --- Private ---

  _writeBbox(view, offset, bbox) {
    view.setFloat64(offset, bbox[0], true);
    view.setFloat64(offset + 8, bbox[1], true);
    view.setFloat64(offset + 16, bbox[2], true);
    view.setFloat64(offset + 24, bbox[3], true);
  }

  _updateBbox(coords) {
    for (const c of coords) {
      if (c[0] < this.minx) this.minx = c[0];
      if (c[1] < this.miny) this.miny = c[1];
      if (c[0] > this.maxx) this.maxx = c[0];
      if (c[1] > this.maxy) this.maxy = c[1];
    }
  }

  _createRecord(geom) {
    switch (this.type) {
      case 'point': return this._writePoint(geom);
      case 'multipoint': return this._writeMultiPoint(geom);
      case 'line': return this._writePoly(geom, false);
      case 'polygon': return this._writePoly(geom, true);
    }
  }

  _writePoint(geom) {
    this.recNum++;
    const buf = new ArrayBuffer(28);
    const view = new DataView(buf);
    view.setInt32(0, this.recNum, false); // record number (big-endian)
    view.setInt32(4, 10, false); // content length in 16-bit words
    view.setInt32(8, 1, true); // shape type: Point
    view.setFloat64(12, geom.coordinates[0], true);
    view.setFloat64(20, geom.coordinates[1], true);
    this._updateBbox([geom.coordinates]);
    return new Uint8Array(buf);
  }

  _writeMultiPoint(geom) {
    this.recNum++;
    const points = geom.coordinates;
    const numPoints = points.length;
    // Header(8) + type(4) + bbox(32) + numPoints(4) + points(numPoints*16)
    const contentBytes = 4 + 32 + 4 + numPoints * 16;
    const recordBytes = 8 + contentBytes;
    const buf = new ArrayBuffer(recordBytes);
    const view = new DataView(buf);

    view.setInt32(0, this.recNum, false);
    view.setInt32(4, contentBytes / 2, false);
    view.setInt32(8, 8, true); // MultiPoint type

    // Calculate and write bbox
    let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    for (const p of points) {
      if (p[0] < minx) minx = p[0]; if (p[0] > maxx) maxx = p[0];
      if (p[1] < miny) miny = p[1]; if (p[1] > maxy) maxy = p[1];
    }
    this._writeBbox(view, 12, [minx, miny, maxx, maxy]);

    view.setInt32(44, numPoints, true);
    let off = 48;
    for (const p of points) {
      view.setFloat64(off, p[0], true);
      view.setFloat64(off + 8, p[1], true);
      off += 16;
    }
    this._updateBbox(points);
    return new Uint8Array(buf);
  }

  _writePoly(geom, isPolygon) {
    this.recNum++;
    const rings = this._extractRings(geom, isPolygon);
    const numParts = rings.length;
    const numPoints = rings.reduce((s, r) => s + r.length, 0);
    const contentBytes = 4 + 32 + 4 + 4 + numParts * 4 + numPoints * 16;
    const buf = new ArrayBuffer(8 + contentBytes);
    const view = new DataView(buf);

    view.setInt32(0, this.recNum, false);
    view.setInt32(4, contentBytes / 2, false);
    view.setInt32(8, this.shpTypeCode, true);

    let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
    const allCoords = [];
    for (const ring of rings) {
      for (const c of ring) {
        allCoords.push(c);
        if (c[0] < minx) minx = c[0]; if (c[0] > maxx) maxx = c[0];
        if (c[1] < miny) miny = c[1]; if (c[1] > maxy) maxy = c[1];
      }
    }
    this._writeBbox(view, 12, [minx, miny, maxx, maxy]);

    view.setInt32(44, numParts, true);
    view.setInt32(48, numPoints, true);

    let partOffset = 0;
    for (let i = 0; i < numParts; i++) {
      view.setInt32(52 + i * 4, partOffset, true);
      partOffset += rings[i].length;
    }

    let off = 52 + numParts * 4;
    for (const ring of rings) {
      for (const c of ring) {
        view.setFloat64(off, c[0], true);
        view.setFloat64(off + 8, c[1], true);
        off += 16;
      }
    }
    this._updateBbox(allCoords);
    return new Uint8Array(buf);
  }

  _extractRings(geom, isPolygon) {
    if (isPolygon) {
      // Multi/Polygon — each polygon has outer + holes
      const polygons = geom.type === 'Polygon' ? [geom.coordinates] : geom.coordinates;
      const rings = [];
      for (const poly of polygons) {
        for (let i = 0; i < poly.length; i++) {
          rings.push(i === 0 ? ensureClockwise(poly[i]) : ensureCounterClockwise(poly[i]));
        }
      }
      return rings;
    }
    // Multi/LineString
    return geom.type === 'LineString' ? [geom.coordinates] : geom.coordinates;
  }
}

// --- Ring winding helpers ---

function isClockwise(ring) {
  let total = 0;
  for (let i = 1; i < ring.length; i++) {
    total += (ring[i][0] - ring[i - 1][0]) * (ring[i][1] + ring[i - 1][1]);
  }
  return total >= 0;
}

function ensureClockwise(ring) {
  return isClockwise(ring) ? ring : ring.slice().reverse();
}

function ensureCounterClockwise(ring) {
  return isClockwise(ring) ? ring.slice().reverse() : ring;
}

// --- DbfWriter: writes .dbf record data and generates header ---

const DBF_TYPE_MAP = {
  number: 'N', character: 'C', logical: 'L', boolean: 'L', date: 'D',
};
const DBF_FIELD_SIZES = { C: 254, L: 1, D: 8, N: 18 };
const DATE_RE = /^(\d{4})-(\d\d)-(\d\d)/;

export class DbfWriter {
  constructor(fields) {
    this.fields = [];
    this.recordSize = 1; // 1 byte deletion flag per record
    this.records = 0;
    this._pendingChunks = [];

    for (const f of fields) {
      const dbfType = DBF_TYPE_MAP[f.type];
      if (!dbfType) continue;
      const length = f.length || DBF_FIELD_SIZES[dbfType];
      const field = { name: f.dbfName, originalName: f.originalName, type: dbfType, length };
      if (dbfType === 'N') {
        field.precision = Math.min(f.precision || 3, 15);
        if (field.precision >= field.length) field.precision = field.length - 1;
      }
      this.recordSize += length;
      this.fields.push(field);
    }
  }

  writeRecord(properties) {
    const buf = new ArrayBuffer(this.recordSize);
    const bytes = new Uint8Array(buf);
    bytes[0] = 0x20; // not deleted
    let cur = 1;

    for (const field of this.fields) {
      const value = properties[field.originalName];
      switch (field.type) {
        case 'L':
          bytes[cur] = value ? 0x54 : 0x46; // T or F
          cur++;
          break;
        case 'N': {
          const str = typeof value === 'number'
            ? formatNum(value, field.length, field.precision)
            : lpad('', field.length);
          writeAscii(bytes, cur, str, field.length);
          cur += field.length;
          break;
        }
        case 'C': {
          const str = typeof value === 'string' ? value : '';
          writeAscii(bytes, cur, rpad(str, field.length), field.length);
          cur += field.length;
          break;
        }
        case 'D': {
          const match = typeof value === 'string' ? DATE_RE.exec(value) : null;
          const str = match ? match[1] + match[2] + match[3] : '        ';
          writeAscii(bytes, cur, str, field.length);
          cur += field.length;
          break;
        }
      }
    }
    this.records++;
    this._pendingChunks.push(bytes);
  }

  /** Return and clear pending record chunks (call after each row group). */
  flushChunks() {
    const chunks = this._pendingChunks;
    this._pendingChunks = [];
    return chunks;
  }

  /** Generate .dbf header. */
  generateHeader() {
    const headerLen = 32 + this.fields.length * 32 + 1;
    const buf = new ArrayBuffer(headerLen);
    const view = new DataView(buf);
    const bytes = new Uint8Array(buf);

    const now = new Date();
    view.setUint8(0, 3); // version
    view.setUint8(1, now.getFullYear() - 1900);
    view.setUint8(2, now.getMonth() + 1);
    view.setUint8(3, now.getDate());
    view.setUint32(4, this.records, true);
    view.setUint16(8, headerLen, true);
    view.setUint16(10, this.recordSize, true);

    let cur = 32;
    for (const field of this.fields) {
      writeAscii(bytes, cur, field.name, 11);
      cur += 11;
      writeAscii(bytes, cur, field.type, 1);
      cur += 5;
      view.setUint8(cur, field.length);
      cur++;
      if (field.type === 'N') view.setInt8(cur, field.precision);
      cur += 15;
    }
    bytes[headerLen - 1] = 0x0D; // header terminator
    return new Uint8Array(buf);
  }

  /** DBF file terminator byte. */
  generateTerminator() {
    return new Uint8Array([0x1A]);
  }
}

// --- String helpers ---

const BLANK = '                                                  ';

function lpad(str, len) {
  return str.length >= len ? str : BLANK.slice(0, len - str.length) + str;
}

function rpad(str, len) {
  return str.length >= len ? str.slice(0, len) : str + BLANK.slice(0, len - str.length);
}

function formatNum(num, length, precision) {
  const isNeg = num < 0;
  if (isNeg) { num = -num; length--; }
  const parts = num.toString().split('.');
  let left = parts[0];
  let right = parts[1] || '';
  const llen = length - (precision + 1);
  if (left.length > llen) left = left.slice(-llen);
  else left = lpad(left, llen);
  if (right.length > precision) right = right.slice(0, precision);
  else right = rpad(right, precision);
  return `${isNeg ? '-' : ''}${left}.${right}`;
}

function writeAscii(bytes, offset, str, maxLen) {
  for (let i = 0; i < maxLen && i < str.length; i++)
    bytes[offset + i] = str.charCodeAt(i) & 0x7F;
}

// --- .prj file content (WGS 84) ---

export const PRJ_WGS84 = 'GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]]';

export const SHP_TYPE_LABELS = {
  point: 'point', multipoint: 'multipoint', line: 'polyline', polygon: 'polygon',
};
