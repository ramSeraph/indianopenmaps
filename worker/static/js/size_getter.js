// Fetches and caches file sizes via HEAD requests through CORS proxy
import { proxyUrl, formatSize } from './utils.js';

export class SizeGetter {
  constructor() {
    this.cache = new Map(); // url → bytes (integer)
  }

  async getSizeBytes(url) {
    if (this.cache.has(url)) {
      return this.cache.get(url);
    }

    try {
      const proxied = proxyUrl(url);
      const response = await fetch(proxied, { method: 'HEAD' });

      if (response.ok) {
        const contentLength = response.headers.get('content-length');
        if (contentLength) {
          const bytes = parseInt(contentLength, 10);
          this.cache.set(url, bytes);
          return bytes;
        }
      }
    } catch (error) {
      console.error('Error fetching file size:', error);
    }

    return null;
  }

  async getSize(url) {
    const bytes = await this.getSizeBytes(url);
    return bytes != null ? formatSize(bytes) : null;
  }
}
