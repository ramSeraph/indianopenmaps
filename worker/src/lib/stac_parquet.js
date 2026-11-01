import { parquetMetadataAsync, parquetRead, parquetSchema } from 'hyparquet';
import { fetchWithRetry } from './request.js';
import { sharedRangeCache } from './range_cache.js';
import { etagMismatch, strongEtag } from './http_etag.js';

const SEARCH_COLUMNS = ['id', 'bbox', 'geometry', 'datetime', 'start_datetime', 'end_datetime', 'properties'];
const MAX_INDEX_BYTES = 256 * 1024 * 1024;
const MAX_RANGE_BYTES = 16 * 1024 * 1024;
const METADATA_BYTES = 2 * 1024 * 1024;

async function readBytes(response, expected) {
  if (Number(response.headers.get('content-length')) > expected) {
    await response.body?.cancel();
    throw new Error('Index range response is larger than requested');
  }
  const bytes = new Uint8Array(expected);
  let offset = 0;
  for await (const chunk of response.body) {
    if (offset + chunk.length > expected) throw new Error('Index range response is larger than requested');
    bytes.set(chunk, offset); offset += chunk.length;
  }
  if (offset !== expected) throw new Error('Truncated index range response');
  return bytes;
}

/** Request-scoped readers, with only completed metadata and ranges shared. */
export class STACParquet {
  constructor(logger, rangeCache = sharedRangeCache) {
    this.logger = logger;
    this.rangeCache = rangeCache;
    this.metadataCache = new Map();
  }

  async open(url, state) {
    const cacheUrl = state.cacheUrl;
    let cached = this.metadataCache.get(cacheUrl);
    if (cached && Date.now() - cached.created >= 300_000) {
      this.metadataCache.delete(cacheUrl); cached = undefined;
    }
    let byteLength = cached?.byteLength;
    let etag = cached?.etag;
    if (byteLength === undefined) {
      const response = await fetchWithRetry(url, { method: 'HEAD' });
      if (!response.ok) throw new Error(`Index HEAD returned HTTP ${response.status}`);
      byteLength = Number(response.headers.get('content-length'));
      etag = strongEtag(response.headers);
      if (!Number.isSafeInteger(byteLength) || byteLength < 8 || byteLength > MAX_INDEX_BYTES) throw new Error('Invalid index length');
    }
    let edge;
    try { edge = await globalThis.caches?.open('stac-ranges-v1'); }
    catch (error) { this.logger.error(`STAC range cache unavailable: ${error.message}`); }
    state.edge = edge;
    const reader = this.rangeCache.forRequest();
    // Public URLs stay stable; the catalog content hash separates cached revisions.
    const source = { url: new URL(cacheUrl) };
    if (etag) source.url.searchParams.set('iomaps-stac-etag', etag);
    state.rangeUrl = source.url.href;
    const file = { byteLength, slice: async (start, end = byteLength) => {
      if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end <= start || end > byteLength || end - start > MAX_RANGE_BYTES) {
        throw new Error('Invalid or oversized index byte range');
      }
      const bytes = await reader.fetch({ source, offset: start, length: end - start }, async () => {
        const key = new URL(source.url);
        key.searchParams.set('iomaps-stac-range', `${start}-${end}`);
        state.keys.add(key.href);
        let hit;
        try { hit = await edge?.match(key.href); }
        catch (error) { this.logger.error(`STAC range cache read failed: ${error.message}`); }
        if (hit) return readBytes(hit, end - start);
        const response = await fetchWithRetry(url, { headers: { Range: `bytes=${start}-${end - 1}` } });
        const contentRange = response.headers.get('content-range');
        if (response.status !== 206 || contentRange !== `bytes ${start}-${end - 1}/${byteLength}`) {
          await response.body?.cancel();
          throw new Error(`Invalid index range response: HTTP ${response.status}, ${contentRange}`);
        }
        if (etagMismatch(etag, strongEtag(response.headers), { requireReceived: true })) {
          await response.body?.cancel();
          throw new Error('Index changed during range read');
        }
        const result = await readBytes(response, end - start);
        // Write to the edge cache only after Parquet decoding succeeds.
        if (edge) state.downloads.set(key.href, result);
        return result;
      });
      return bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength);
    } };
    let metadata = cached?.metadata;
    if (!metadata) {
      metadata = await parquetMetadataAsync(file, { initialFetchSize: 64 * 1024 });
      const bytes = JSON.stringify(metadata, (_, v) => typeof v === 'bigint' ? String(v) : v).length * 2;
      if (bytes <= METADATA_BYTES) {
        let total = [...this.metadataCache.values()].reduce((sum, entry) => sum + entry.bytes, 0);
        while (this.metadataCache.size && total + bytes > METADATA_BYTES) {
          const oldest = this.metadataCache.keys().next().value;
          total -= this.metadataCache.get(oldest).bytes; this.metadataCache.delete(oldest);
        }
        this.metadataCache.set(cacheUrl, { metadata, byteLength, etag, bytes, created: Date.now() });
      }
    } else {
      this.metadataCache.delete(cacheUrl); this.metadataCache.set(cacheUrl, cached);
    }
    return { file, metadata };
  }

  async read(url, { searchOnly = false, rowStart, rowEnd, positions, revision } = {}) {
    if (positions?.length === 0) return [];
    const cacheUrl = new URL(url);
    if (revision) cacheUrl.searchParams.set('iomaps-stac-revision', revision);
    const state = { keys: new Set(), downloads: new Map(), cacheUrl: cacheUrl.href };
    try {
      const { file, metadata } = await this.open(url, state);
      const names = parquetSchema(metadata).children.map(c => c.element.name);
      const columns = searchOnly ? SEARCH_COLUMNS.filter(name => names.includes(name)) : undefined;
      let rows;
      // Hyparquet 1.25 misdecodes some dictionary strings with offset indexes.
      // Row-group and column pruning work without that optional page-level path.
      const options = { file, metadata, rowFormat: 'object', utf8: false, columns, rowStart, rowEnd };
      if (positions) {
        rows = [];
        let groupStart = 0;
        for (const group of metadata.row_groups) {
          const groupEnd = groupStart + Number(group.num_rows);
          const selected = positions.filter(position => position >= groupStart && position < groupEnd);
          // Skip unrelated groups and release temporary decoded columns between groups.
          if (selected.length) await parquetRead({ ...options, rowStart: selected[0], rowEnd: selected.at(-1) + 1,
            onComplete: values => { for (const position of selected) rows.push(values[position - selected[0]]); } });
          groupStart = groupEnd;
        }
      } else await parquetRead({ ...options, onComplete: values => { rows = values; } });
      for (const [key, bytes] of state.downloads) {
        try {
          // Cache API rejects 206, so store validated ranges as HTTP 200.
          await state.edge.put(key, new Response(bytes, { headers: { 'Cache-Control': 'public, max-age=86400' } }));
        } catch (error) { this.logger.error(`STAC range cache write failed: ${error.message}`); }
      }
      return rows;
    } catch (error) {
      this.metadataCache.delete(state.cacheUrl);
      this.rangeCache.invalidate(state.rangeUrl || state.cacheUrl);
      for (const key of state.keys) {
        try { await state.edge?.delete(key); }
        catch (cacheError) { this.logger.error(`STAC range cache invalidation failed: ${cacheError.message}`); }
      }
      throw error;
    }
  }
}
