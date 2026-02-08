// Fetches and caches file sizes via HEAD requests through CORS proxy
export class SizeGetter {
  constructor() {
    this.cache = new Map();
  }

  formatSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
  }

  async getSize(url) {
    if (this.cache.has(url)) {
      return this.cache.get(url);
    }

    try {
      const corsProxyUrl = `/cors-proxy?url=${encodeURIComponent(url)}`;
      const response = await fetch(corsProxyUrl, { method: 'HEAD' });
      
      if (response.ok) {
        const contentLength = response.headers.get('content-length');
        if (contentLength) {
          const size = this.formatSize(parseInt(contentLength, 10));
          this.cache.set(url, size);
          return size;
        }
      }
    } catch (error) {
      console.error('Error fetching file size:', error);
    }
    
    return null;
  }

  async updateElement(url, element) {
    const size = await this.getSize(url);
    // Check if element is still in DOM before updating
    if (!element.isConnected) return;
    
    if (size) {
      element.textContent = size;
    } else {
      element.textContent = '';
    }
    element.classList.remove('loading');
  }
}
