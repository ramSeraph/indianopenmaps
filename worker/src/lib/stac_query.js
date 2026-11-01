import { booleanIntersects } from '@turf/boolean-intersects';
import { BadRequestError } from './errors.js';

export const PAGE_LIMIT = 1000;

function integer(value, fallback, minimum, name) {
  if (value === undefined) return fallback;
  if ((typeof value !== 'number' && !/^\d+$/.test(value)) || !Number.isSafeInteger(Number(value)) || Number(value) < minimum) {
    throw new BadRequestError(`${name} must be an integer >= ${minimum}`);
  }
  return Number(value);
}

function strings(value, name) {
  if (value === undefined) return undefined;
  const values = typeof value === 'string' ? value.split(',') : value;
  if (!Array.isArray(values) || !values.length || values.some(v => typeof v !== 'string' || !v.trim())) {
    throw new BadRequestError(`${name} must be a nonempty array or comma-separated list of strings`);
  }
  return [...new Set(values)];
}

export function timeRange(value) {
  if (typeof value !== 'string') throw new BadRequestError('datetime must be an RFC3339 timestamp or interval');
  const parts = value.includes('/') ? value.split('/') : [value, value];
  if (parts.length !== 2) throw new BadRequestError('Invalid datetime interval');
  const result = parts.map((v, i) => {
    if (v === '..' || v === '') return i === 0 ? -Infinity : Infinity;
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/i.test(v) || !Number.isFinite(Date.parse(v))) {
      throw new BadRequestError('datetime must use RFC3339 timestamps with timezones');
    }
    return Date.parse(v);
  });
  if (result[0] > result[1] || (!Number.isFinite(result[0]) && !Number.isFinite(result[1]))) {
    throw new BadRequestError('Invalid datetime interval');
  }
  return result;
}

export function bboxGeometry(bbox) {
  const [w, s, e, n] = bbox.length === 6 ? [bbox[0], bbox[1], bbox[3], bbox[4]] : bbox;
  const polygon = (west, east) => [[[west, s], [east, s], [east, n], [west, n], [west, s]]];
  return w <= e ? { type: 'Polygon', coordinates: polygon(w, e) }
    : { type: 'MultiPolygon', coordinates: [polygon(w, 180), polygon(-180, e)] };
}

function validateGeometry(geometry) {
  if (!geometry || typeof geometry !== 'object') throw new BadRequestError('intersects must be a GeoJSON geometry');
  if (geometry.type === 'GeometryCollection') {
    if (!Array.isArray(geometry.geometries) || !geometry.geometries.length) throw new BadRequestError('Empty geometry');
    geometry.geometries.forEach(validateGeometry);
    return;
  }
  const depths = { Point: 0, MultiPoint: 1, LineString: 1, MultiLineString: 2, Polygon: 2, MultiPolygon: 3 };
  if (!(geometry.type in depths)) throw new BadRequestError('Unsupported GeoJSON geometry');
  function coordinates(value, depth) {
    if (!Array.isArray(value) || !value.length) throw new BadRequestError('Empty or invalid coordinates');
    if (depth) value.forEach(v => coordinates(v, depth - 1));
    else if (value.length < 2 || value.some(v => typeof v !== 'number' || !Number.isFinite(v)) ||
             Math.abs(value[0]) > 180 || Math.abs(value[1]) > 90) throw new BadRequestError('Invalid WGS84 position');
  }
  coordinates(geometry.coordinates, depths[geometry.type]);
  const polygons = geometry.type === 'Polygon' ? [geometry.coordinates] : geometry.type === 'MultiPolygon' ? geometry.coordinates : [];
  for (const polygon of polygons) for (const ring of polygon) {
    if (ring.length < 4 || ring[0][0] !== ring.at(-1)[0] || ring[0][1] !== ring.at(-1)[1]) {
      throw new BadRequestError('Polygon rings must be closed and contain at least four positions');
    }
  }
}

export function normalizeQuery(params = {}) {
  if (!params || Array.isArray(params) || typeof params !== 'object') throw new BadRequestError('Expected a JSON object');
  const allowed = new Set(['limit', 'offset', 'bbox', 'datetime', 'collections', 'ids', 'intersects']);
  for (const key of Object.keys(params)) if (!allowed.has(key)) throw new BadRequestError(`Unsupported search parameter: ${key}`);
  const query = { limit: Math.min(PAGE_LIMIT, integer(params.limit, 100, 1, 'limit')), offset: integer(params.offset, 0, 0, 'offset') };
  for (const name of ['collections', 'ids']) if (params[name] !== undefined) query[name] = strings(params[name], name);
  if (params.bbox !== undefined) {
    const bbox = typeof params.bbox === 'string' ? params.bbox.split(',').map(v => v.trim() ? Number(v) : NaN) : params.bbox;
    if (!Array.isArray(bbox) || ![4, 6].includes(bbox.length) || bbox.some(v => typeof v !== 'number' || !Number.isFinite(v))) {
      throw new BadRequestError('bbox must contain four or six finite numbers');
    }
    const end = bbox.length / 2;
    if (Math.abs(bbox[0]) > 180 || Math.abs(bbox[end]) > 180 || Math.abs(bbox[1]) > 90 || Math.abs(bbox[end + 1]) > 90 ||
        bbox[1] > bbox[end + 1] || (end === 3 && bbox[2] > bbox[5])) throw new BadRequestError('Invalid WGS84 bbox');
    query.bbox = bbox;
  }
  if (params.intersects !== undefined) {
    if (query.bbox) throw new BadRequestError('bbox and intersects are mutually exclusive');
    try { query.intersects = typeof params.intersects === 'string' ? JSON.parse(params.intersects) : params.intersects; }
    catch { throw new BadRequestError('intersects must be GeoJSON'); }
    validateGeometry(query.intersects);
  }
  if (params.datetime !== undefined) { timeRange(params.datetime); query.datetime = params.datetime; }
  return query;
}

function bboxIntersects(a, b) {
  const ae = a.length / 2, be = b.length / 2;
  if (a[1] > b[be + 1] || a[ae + 1] < b[1]) return false;
  const longitudeRanges = (box, end) => box[0] <= box[end] ? [[box[0], box[end]]] : [[box[0], 180], [-180, box[end]]];
  return longitudeRanges(a, ae).some(([w, e]) => longitudeRanges(b, be).some(([bw, be]) => w <= be && e >= bw));
}

export function predicate(query) {
  const geometry = query.intersects || (query.bbox && bboxGeometry(query.bbox));
  const time = query.datetime && timeRange(query.datetime);
  const ids = query.ids && new Set(query.ids);
  return item => {
    // STAC IDs take precedence over spatial/temporal parameters.
    if (ids) return ids.has(item.id);
    // Reject distant sheets before the more expensive exact polygon test.
    if (query.bbox && item.bbox && !bboxIntersects(query.bbox, item.bbox)) return false;
    if (geometry && (!item.geometry || !booleanIntersects(item.geometry, geometry))) return false;
    if (query.bbox?.length === 6 && item.bbox?.length === 6 &&
        (item.bbox[5] < query.bbox[2] || item.bbox[2] > query.bbox[5])) return false;
    if (time) {
      const p = item.properties;
      const start = Date.parse(p.datetime || p.start_datetime), end = Date.parse(p.datetime || p.end_datetime);
      if (!Number.isFinite(start) || !Number.isFinite(end) || end < time[0] || start > time[1]) return false;
    }
    return true;
  };
}

export function queryUrl(url, query) {
  const result = new URL(url);
  result.search = '';
  for (const [key, value] of Object.entries(query)) {
    result.searchParams.set(key, key === 'intersects' ? JSON.stringify(value) : Array.isArray(value) ? value.join(',') : String(value));
  }
  return result.href;
}
