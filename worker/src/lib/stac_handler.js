import { wkbToGeojson } from 'hyparquet/src/wkb.js';
import { STACParquet } from './stac_parquet.js';
import { ResourceUnavailableError } from './errors.js';
import { bboxGeometry, normalizeQuery, predicate, queryUrl } from './stac_query.js';
import { DEFAULT_SOURCE_REFRESH_MS } from './cache_policy.js';

export const CONFORMANCE = [
  'https://api.stacspec.org/v1.0.0/core',
  'https://api.stacspec.org/v1.0.0/collections',
  'https://api.stacspec.org/v1.0.0/ogcapi-features',
  'https://api.stacspec.org/v1.0.0/item-search',
  'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core',
  'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson',
];
const VERSION = '1.1.0';
const JSON_TYPE = 'application/json';
const GEOJSON_TYPE = 'application/geo+json';
const CACHE_BYTES = 8 * 1024 * 1024;
const link = (rel, href, type = JSON_TYPE, extra = {}) => ({ rel, href, type, ...extra });
const cleanJSON = value => JSON.parse(JSON.stringify(value, (_, v) => typeof v === 'bigint' ? Number(v) : v));
const reserved = new Set(['id', 'assets', 'bbox', 'geometry', 'links', 'stac_extensions', 'stac_version', 'type', 'collection', 'properties']);

export function rowToItem(row) {
  const properties = { ...(row.properties || {}) };
  for (const [key, value] of Object.entries(row)) if (!reserved.has(key)) properties[key] = value;
  let geometry = row.geometry;
  if (geometry instanceof Uint8Array) {
    geometry = wkbToGeojson({ view: new DataView(geometry.buffer, geometry.byteOffset, geometry.byteLength), offset: 0 });
  } else if (typeof geometry === 'string') geometry = JSON.parse(geometry);
  const bbox = Array.isArray(row.bbox) ? row.bbox : row.bbox && [row.bbox.xmin, row.bbox.ymin, row.bbox.xmax, row.bbox.ymax];
  // Legacy indexes without WKB remain readable; never replace an existing footprint.
  if (!geometry && bbox) geometry = bboxGeometry(bbox);
  if (typeof row.id !== 'string' || !row.id || !geometry || !bbox) throw new Error('Index item lacks an ID or geometry');
  const extensions = [...(row.stac_extensions || [])];
  // The original 50k index stored projection fields without declaring their extension.
  if (properties['proj:epsg'] && !extensions.some(url => url.includes('/projection/'))) {
    extensions.push('https://stac-extensions.github.io/projection/v1.1.0/schema.json');
  }
  return cleanJSON({
    type: 'Feature', stac_version: row.stac_version || VERSION, id: row.id, geometry, bbox,
    properties, assets: row.assets || {}, stac_extensions: extensions, links: row.links || [],
  });
}

class STACHandler {
  constructor(catalog, logger, { refreshMs = DEFAULT_SOURCE_REFRESH_MS } = {}) {
    this.catalog = catalog;
    this.logger = logger;
    this.refreshMs = refreshMs;
    this.collections = new Map();
    this.itemsCache = new Map();
    this.indexRevisions = new Map();
    this.parquet = new STACParquet(logger, { refreshMs });
  }

  init() {
    for (const entry of this.catalog.links || []) if (entry.rel === 'child' && entry.geoparquet) {
      const id = entry.collection?.id || entry.href.replace(/^\.\//, '').replace(/\/$/, '');
      this.collections.set(id, { id, title: entry.title || id, geoparquetUrl: entry.geoparquet, metadata: entry.collection });
      this.indexRevisions.set(entry.geoparquet, entry.geoparquet_sha256);
    }
  }

  getLandingPage(base) {
    return {
      stac_version: VERSION, type: 'Catalog', id: this.catalog.id, title: this.catalog.title,
      description: this.catalog.description, conformsTo: CONFORMANCE,
      links: [link('self', base), link('root', base), link('data', `${base}/collections`),
        link('conformance', `${base}/conformance`), link('service-desc', `${base}/api.json`, 'application/vnd.oai.openapi+json;version=3.0'),
        link('service-doc', `${base}/api.html`, 'text/html'),
        ...['GET', 'POST'].map(method => link('search', `${base}/search`, GEOJSON_TYPE, { method })),
        ...Array.from(this.collections.values(), col => link('child', `${base}/collections/${encodeURIComponent(col.id)}`, JSON_TYPE, { title: col.title }))],
    };
  }

  getConformance() { return { conformsTo: CONFORMANCE }; }

  async getCollections(params, base) {
    const query = normalizeQuery(params);
    const ids = [...this.collections.keys()];
    const collections = [];
    for (const id of ids.slice(query.offset, query.offset + query.limit)) collections.push(await this.getCollection(id, base));
    const links = [link('self', queryUrl(`${base}/collections`, params)), link('root', base)];
    if (query.offset + collections.length < ids.length) links.push(link('next', queryUrl(`${base}/collections`, { ...query, offset: query.offset + collections.length })));
    return { collections, links, numberMatched: ids.length, numberReturned: collections.length };
  }

  async getCollection(id, base) {
    const info = this.collections.get(id);
    if (!info) return null;
    let metadata = info.metadata;
    if (!metadata) {
      const items = await this.loadItemsFromGeoparquet(info.geoparquetUrl);
      const bounds = items.reduce((b, item) => [Math.min(b[0], item.bbox[0]), Math.min(b[1], item.bbox[1]),
        Math.max(b[2], item.bbox[item.bbox.length / 2]), Math.max(b[3], item.bbox[item.bbox.length / 2 + 1])], [180, 90, -180, -90]);
      const times = items.flatMap(i => [i.properties.datetime || i.properties.start_datetime, i.properties.datetime || i.properties.end_datetime]).filter(Boolean).sort();
      metadata = { description: info.title, license: 'other', extent: {
        spatial: { bbox: items.length ? [bounds] : [[-180, -90, 180, 90]] }, temporal: { interval: [[times[0] || null, times.at(-1) || null]] },
      }, item_count: items.length };
    }
    const url = `${base}/collections/${encodeURIComponent(id)}`;
    return { ...metadata, stac_version: VERSION, type: 'Collection', id, title: info.title,
      assets: { ...metadata.assets, index: { href: info.geoparquetUrl, type: 'application/vnd.apache.parquet', roles: ['metadata'] } },
      links: [link('self', url), link('root', base), link('parent', base), link('items', `${url}/items`, GEOJSON_TYPE),
        ...(metadata.links || []).filter(l => !['self', 'root', 'parent', 'items', 'item'].includes(l.rel))] };
  }

  itemResponse(item, id, base) {
    const collection = `${base}/collections/${encodeURIComponent(id)}`;
    const extensions = [...(item.stac_extensions || [])];
    const maps = [];
    const cog = item.assets.cog;
    const code = item.properties['proj:code'] || `EPSG:${item.properties['proj:epsg']}`;
    if (cog && code === 'EPSG:3857') {
      const extension = 'https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json';
      if (!extensions.includes(extension)) extensions.push(extension);
      maps.push(link('xyz', `${new URL(base).origin}/cog-tiles/{z}/{x}/{y}?url=${encodeURIComponent(cog.href)}&format=png`,
        'image/png', { title: 'Map sheet tiles' }));
    }
    return { ...item, collection: id, stac_extensions: extensions, links: [
      link('self', `${collection}/items/${encodeURIComponent(item.id)}`, GEOJSON_TYPE),
      link('root', base), link('parent', collection), link('collection', collection),
      ...item.links.filter(l => !['self', 'root', 'parent', 'collection'].includes(l.rel)),
      ...maps,
    ] };
  }

  async getItem(id, itemId, base) {
    const info = this.collections.get(id);
    if (!info) return null;
    const position = (await this.loadItemsFromGeoparquet(info.geoparquetUrl)).findIndex(i => i.id === itemId);
    if (position < 0) return null;
    const [item] = await this.readGeoparquet(info.geoparquetUrl, { rowStart: position, rowEnd: position + 1 });
    return this.itemResponse(item, id, base);
  }

  async getItems(id, params, base) {
    if (!this.collections.has(id)) return null;
    const endpoint = `${base}/collections/${encodeURIComponent(id)}/items`;
    return this.search({ ...params, collections: [id] }, base, 'GET', endpoint, queryUrl(endpoint, params));
  }

  async search(params, base, method = 'GET', endpoint = `${base}/search`, selfUrl) {
    const query = normalizeQuery(params);
    const matches = predicate(query);
    const unfiltered = !query.bbox && !query.intersects && !query.datetime && !query.ids;
    const features = [];
    let numberMatched = 0;
    for (const id of query.collections || this.collections.keys()) {
      const info = this.collections.get(id);
      if (!info) continue;
      const count = info.metadata?.item_count;
      if (unfiltered && Number.isSafeInteger(count) && count >= 0) {
        // The publisher verifies these counts against immutable index URLs.
        // Pagination need not decode collections outside the requested page.
        if (query.offset < numberMatched + count && features.length < query.limit) {
          const start = Math.max(0, query.offset - numberMatched);
          const end = Math.min(count, start + query.limit - features.length);
          const items = await this.readGeoparquet(info.geoparquetUrl, { rowStart: start, rowEnd: end });
          if (items.length !== end - start) throw new ResourceUnavailableError('STAC collection count does not match its index');
          for (const item of items) features.push(this.itemResponse(item, id, base));
        }
        numberMatched += count;
        continue;
      }
      const index = await this.loadItemsFromGeoparquet(info.geoparquetUrl);
      const positions = [];
      for (let position = 0; position < index.length; position++) if (matches(index[position])) {
        if (numberMatched >= query.offset && features.length + positions.length < query.limit) positions.push(position);
        numberMatched++;
      }
      if (positions.length) for (const item of await this.readGeoparquet(info.geoparquetUrl, { positions })) features.push(this.itemResponse(item, id, base));
    }
    const pageLink = (rel, body) => method === 'POST'
      ? link(rel, endpoint, GEOJSON_TYPE, { method: 'POST', body, merge: false })
      : link(rel, queryUrl(endpoint, body), GEOJSON_TYPE);
    const self = method === 'POST' ? pageLink('self', query) : link('self', selfUrl || queryUrl(endpoint, params), GEOJSON_TYPE);
    const links = [self, link('root', base)];
    if (query.offset + features.length < numberMatched) links.push(pageLink('next', { ...query, offset: query.offset + features.length }));
    return { type: 'FeatureCollection', features, links, numberMatched, numberReturned: features.length,
      context: { returned: features.length, matched: numberMatched, limit: query.limit } };
  }

  async loadItemsFromGeoparquet(url) {
    const cached = this.itemsCache.get(url);
    if (cached && (this.refreshMs === 0 || Date.now() - cached.created < this.refreshMs)) {
      this.itemsCache.delete(url); this.itemsCache.set(url, cached);
      return cached.items;
    }
    this.itemsCache.delete(url);
    // Only IDs, geometry and dates belong in the search cache. Full assets and
    // source properties are read for the selected page, not for the whole index.
    const items = (await this.readGeoparquet(url, { searchOnly: true })).map(item => ({
      id: item.id, bbox: item.bbox, geometry: item.geometry, properties: {
        datetime: item.properties.datetime,
        start_datetime: item.properties.start_datetime,
        end_datetime: item.properties.end_datetime,
      },
    }));
    const bytes = JSON.stringify(items).length * 2;
    if (bytes <= CACHE_BYTES) {
      let total = [...this.itemsCache.values()].reduce((n, c) => n + c.bytes, 0);
      while (this.itemsCache.size && total + bytes > CACHE_BYTES) {
        const oldest = this.itemsCache.keys().next().value;
        total -= this.itemsCache.get(oldest).bytes; this.itemsCache.delete(oldest);
      }
      this.itemsCache.set(url, { items, bytes, created: Date.now() });
    }
    return items;
  }

  async readGeoparquet(url, options) {
    try {
      const items = (await this.parquet.read(url, { ...options, revision: this.indexRevisions.get(url) })).map(rowToItem);
      if (new Set(items.map(i => i.id)).size !== items.length) throw new Error('Duplicate STAC item IDs');
      return items;
    } catch (error) {
      this.logger.error(`Cannot read STAC index ${url}: ${error.message}`);
      throw new ResourceUnavailableError('STAC index is temporarily unavailable');
    }
  }
}

export default STACHandler;
