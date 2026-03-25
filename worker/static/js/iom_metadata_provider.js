// Metadata provider for indianopenmaps tile sources.
// Extends MetadataProvider with meta.json partition support
// and .mosaic.json/.pmtiles URL resolution.

import { MetadataProvider } from 'geoparquet-extractor';
import { proxyUrl } from './utils.js';

export class IomMetadataProvider extends MetadataProvider {
  constructor() {
    super();
    /** @type {Map<string, object>} */
    this._metaJsonCache = new Map();
    /** @type {Map<string, string[]>} */
    this._partitionCache = new Map();
  }

  getParquetUrl(sourceUrl) {
    return sourceUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet');
  }

  getMetaJsonUrl(sourceUrl) {
    return this.getParquetUrl(sourceUrl) + '.meta.json';
  }

  /** @override */
  async getPartitions(sourceUrl) {
    const metaUrl = this.getMetaJsonUrl(sourceUrl);
    if (this._partitionCache.has(metaUrl)) {
      return this._partitionCache.get(metaUrl);
    }
    const metaJson = await this._fetchMetaJson(metaUrl);
    if (!metaJson) return null;
    return this._partitionCache.get(metaUrl);
  }

  /** @override */
  async getExtents(sourceUrl) {
    const metaUrl = this.getMetaJsonUrl(sourceUrl);
    const metaJson = await this._fetchMetaJson(metaUrl);
    return metaJson?.extents ?? null;
  }

  /** @override */
  async getParquetUrls(sourceUrl, partitioned, bbox) {
    if (!partitioned) {
      return [this.getParquetUrl(sourceUrl)];
    }

    const metaUrl = this.getMetaJsonUrl(sourceUrl);
    const metaJson = await this._fetchMetaJson(metaUrl);
    if (!metaJson) return [this.getParquetUrl(sourceUrl)];

    const baseUrl = this.getBaseUrl(sourceUrl);
    const partitions = metaJson.extents ? Object.keys(metaJson.extents) : [];

    if (!bbox || !metaJson.extents) {
      return partitions.map(p => baseUrl + p);
    }

    const [west, south, east, north] = bbox;
    return partitions
      .filter(p => {
        const ext = metaJson.extents[p];
        if (!ext || ext.length < 4) return true;
        const [pMinx, pMiny, pMaxx, pMaxy] = ext;
        return pMinx <= east && pMaxx >= west && pMiny <= north && pMaxy >= south;
      })
      .map(p => baseUrl + p);
  }

  async _fetchMetaJson(metaUrl) {
    if (this._metaJsonCache.has(metaUrl)) {
      return this._metaJsonCache.get(metaUrl);
    }

    try {
      const response = await fetch(proxyUrl(metaUrl));
      if (!response.ok) {
        throw new Error(`Failed to fetch meta.json: ${response.status}`);
      }

      const metaJson = await response.json();
      this._metaJsonCache.set(metaUrl, metaJson);
      const partitions = metaJson.extents ? Object.keys(metaJson.extents) : [];
      this._partitionCache.set(metaUrl, partitions);
      return metaJson;
    } catch (error) {
      console.error('Error fetching partition metadata:', error);
      return null;
    }
  }
}

export const metadataProvider = new IomMetadataProvider();

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
