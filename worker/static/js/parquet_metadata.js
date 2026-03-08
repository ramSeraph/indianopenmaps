// Shared module for parquet metadata fetching, caching, and URL helpers.
// Used by download_panel_control.js and extent_handler.js.

class ParquetMetadata {
  constructor() {
    this.metaJsonCache = new Map();
    this.partitionCache = new Map();
  }

  getParquetUrl(originalUrl) {
    return originalUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet');
  }

  getMetaJsonUrl(originalUrl) {
    return originalUrl.replace(/\.(mosaic\.json|pmtiles)$/, '.parquet.meta.json');
  }

  getBaseUrl(originalUrl) {
    const lastSlash = originalUrl.lastIndexOf('/');
    return originalUrl.substring(0, lastSlash + 1);
  }

  async fetchMetaJson(metaUrl) {
    if (this.metaJsonCache.has(metaUrl)) {
      return this.metaJsonCache.get(metaUrl);
    }

    try {
      const proxyUrl = `/proxy?url=${encodeURIComponent(metaUrl)}`;
      const response = await fetch(proxyUrl);

      if (!response.ok) {
        throw new Error(`Failed to fetch meta.json: ${response.status}`);
      }

      const metaJson = await response.json();
      this.metaJsonCache.set(metaUrl, metaJson);
      const partitions = metaJson.extents ? Object.keys(metaJson.extents) : [];
      this.partitionCache.set(metaUrl, partitions);
      return metaJson;
    } catch (error) {
      console.error('Error fetching partition metadata:', error);
      return null;
    }
  }

  async getPartitions(metaUrl) {
    if (this.partitionCache.has(metaUrl)) {
      return this.partitionCache.get(metaUrl);
    }
    const metaJson = await this.fetchMetaJson(metaUrl);
    if (!metaJson) return null;
    return this.partitionCache.get(metaUrl);
  }

  getExtents(metaUrl) {
    const metaJson = this.metaJsonCache.get(metaUrl);
    if (!metaJson || !metaJson.extents) return null;
    return metaJson.extents;
  }
}

export const parquetMetadata = new ParquetMetadata();
