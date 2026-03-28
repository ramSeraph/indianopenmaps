// Source resolver for indianopenmaps tile sources.
// Resolves .mosaic.json/.pmtiles URLs to concrete parquet files with optional meta.json bboxes.

import { SourceResolver } from 'geoparquet-extractor';
import { proxyUrl } from './utils.js';

export class IomSourceResolver extends SourceResolver {
  constructor() {
    super();
    this._metaJsonCache = new Map();
    this._partitionedHints = new Map();
  }

  /** Register whether a sourceUrl is partitioned, so resolve() can skip meta.json fetches. */
  setPartitioned(sourceUrl, partitioned) {
    this._partitionedHints.set(sourceUrl, partitioned);
  }

  getBaseUrl(sourceUrl) {
    const lastSlash = sourceUrl.lastIndexOf('/');
    return sourceUrl.substring(0, lastSlash + 1);
  }

  getParquetUrl(sourceUrl) {
    return sourceUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet');
  }

  getMetaJsonUrl(sourceUrl) {
    return this.getParquetUrl(sourceUrl) + '.meta.json';
  }

  async resolve(sourceUrl, { bbox, partitioned } = {}) {
    const isPartitioned = partitioned ?? this._partitionedHints.get(sourceUrl) ?? undefined;
    if (isPartitioned === false) {
      const parquetUrl = this.getParquetUrl(sourceUrl);
      const fileName = parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1);
      return { files: [{ id: fileName, url: parquetUrl, bbox: null }] };
    }

    const metaJson = await this._fetchMetaJson(this.getMetaJsonUrl(sourceUrl));
    if (!metaJson?.extents) {
      const parquetUrl = this.getParquetUrl(sourceUrl);
      const fileName = parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1);
      return { files: [{ id: fileName, url: parquetUrl, bbox: null }] };
    }

    const baseUrl = this.getBaseUrl(sourceUrl);
    let files = Object.entries(metaJson.extents).map(([id, fileBbox]) => ({
      id,
      url: baseUrl + id,
      bbox: fileBbox,
    }));

    if (bbox) {
      const [west, south, east, north] = bbox;
      files = files.filter(file => {
        const ext = file.bbox;
        if (!ext || ext.length < 4) return true;
        const [minx, miny, maxx, maxy] = ext;
        return minx <= east && maxx >= west && miny <= north && maxy >= south;
      });
    }

    return { files };
  }

  async _fetchMetaJson(metaUrl) {
    if (this._metaJsonCache.has(metaUrl)) {
      return this._metaJsonCache.get(metaUrl);
    }

    const pending = (async () => {
      const response = await fetch(proxyUrl(metaUrl));
      if (!response.ok) {
        throw new Error(`Failed to fetch meta.json: ${response.status}`);
      }

      return response.json();
    })().catch((error) => {
      this._metaJsonCache.delete(metaUrl);
      console.error('Error fetching partition metadata:', error);
      return null;
    });

    this._metaJsonCache.set(metaUrl, pending);
    return pending;
  }
}

export const sourceResolver = new IomSourceResolver();

// Extract a human-readable label from partition filenames or row-group keys.
// e.g. "data.0.parquet" → "0", "rg_15" → "15"
export function extractLabel(name) {
  const clean = name.replace(/\.parquet$/, '');
  const dotMatch = clean.match(/\.(\d+)$/);
  if (dotMatch) return dotMatch[1];
  const rgMatch = clean.match(/^rg_(\d+)$/);
  if (rgMatch) return rgMatch[1];
  return null;
}
