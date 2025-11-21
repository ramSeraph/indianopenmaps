const fs = require('node:fs');
const path = require('node:path');

class STACHandler {
  constructor(catalogPath, logger) {
    this.catalogPath = catalogPath;
    this.logger = logger;
    this.catalog = null;
    this.collections = new Map();
    this.itemsCache = new Map();
    this.parquetRead = null;
  }

  async init() {
    try {
      const { parquetRead } = await import('hyparquet');
      this.parquetRead = parquetRead;
      
      const catalogData = await fs.promises.readFile(this.catalogPath, 'utf-8');
      this.catalog = JSON.parse(catalogData);
      this.logger.info(`Loaded STAC catalog from ${this.catalogPath}`);
      
      if (this.catalog.links) {
        for (const link of this.catalog.links) {
          if (link.rel === 'child' && link.geoparquet) {
            const collectionId = link.href.replace(/^\.\//, '').replace(/\/$/, '');
            this.collections.set(collectionId, {
              id: collectionId,
              geoparquetUrl: link.geoparquet,
              title: link.title || collectionId
            });
          }
        }
      }
      this.logger.info(`Loaded ${this.collections.size} collections`);
    } catch (err) {
      this.logger.error(`Error loading STAC catalog: ${err.message}`);
      throw err;
    }
  }

  async getLandingPage() {
    const landing = {
      stac_version: '1.0.0',
      type: 'Catalog',
      id: this.catalog.id || 'stac-api',
      title: this.catalog.title || 'STAC API',
      description: this.catalog.description || 'A STAC API serving collections from geoparquet files',
      conformsTo: [
        'https://api.stacspec.org/v1.0.0/core',
        'https://api.stacspec.org/v1.0.0/collections',
        'https://api.stacspec.org/v1.0.0/item-search',
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core',
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson'
      ],
      links: [
        {
          rel: 'self',
          type: 'application/json',
          href: '/stac'
        },
        {
          rel: 'root',
          type: 'application/json',
          href: '/stac'
        },
        {
          rel: 'data',
          type: 'application/json',
          href: '/stac/collections'
        },
        {
          rel: 'conformance',
          type: 'application/json',
          href: '/stac/conformance'
        },
        {
          rel: 'search',
          type: 'application/json',
          href: '/stac/search',
          method: 'GET'
        },
        {
          rel: 'search',
          type: 'application/json',
          href: '/stac/search',
          method: 'POST'
        }
      ]
    };

    for (const [collectionId, collection] of this.collections) {
      landing.links.push({
        rel: 'child',
        type: 'application/json',
        href: `/stac/collections/${collectionId}`
      });
    }

    return landing;
  }

  getConformance() {
    return {
      conformsTo: [
        'https://api.stacspec.org/v1.0.0/core',
        'https://api.stacspec.org/v1.0.0/collections',
        'https://api.stacspec.org/v1.0.0/item-search',
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core',
        'http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson'
      ]
    };
  }

  async getCollections(limit = 100, offset = 0) {
    const collectionsArray = Array.from(this.collections.values()).slice(offset, offset + limit);
    
    const collections = await Promise.all(
      collectionsArray.map(async (col) => {
        return await this.getCollection(col.id);
      })
    );

    return {
      collections: collections.filter(c => c !== null),
      links: [
        {
          rel: 'self',
          type: 'application/json',
          href: '/stac/collections'
        },
        {
          rel: 'root',
          type: 'application/json',
          href: '/stac'
        }
      ]
    };
  }

  async getCollection(collectionId) {
    const collectionInfo = this.collections.get(collectionId);
    if (!collectionInfo) {
      return null;
    }

    return {
      stac_version: '1.0.0',
      type: 'Collection',
      id: collectionId,
      title: collectionInfo.title,
      description: `Collection ${collectionId}`,
      license: 'proprietary',
      extent: {
        spatial: {
          bbox: [[-180, -90, 180, 90]]
        },
        temporal: {
          interval: [[null, null]]
        }
      },
      links: [
        {
          rel: 'self',
          type: 'application/json',
          href: `/stac/collections/${collectionId}`
        },
        {
          rel: 'root',
          type: 'application/json',
          href: '/stac'
        },
        {
          rel: 'items',
          type: 'application/geo+json',
          href: `/stac/collections/${collectionId}/items`
        }
      ]
    };
  }

  async getItems(collectionId, limit = 10, offset = 0, bbox = null) {
    const collectionInfo = this.collections.get(collectionId);
    if (!collectionInfo) {
      return null;
    }

    const items = await this.loadItemsFromGeoparquet(collectionInfo.geoparquetUrl, limit, offset, bbox);

    return {
      type: 'FeatureCollection',
      features: items,
      links: [
        {
          rel: 'self',
          type: 'application/geo+json',
          href: `/stac/collections/${collectionId}/items`
        },
        {
          rel: 'root',
          type: 'application/json',
          href: '/stac'
        },
        {
          rel: 'collection',
          type: 'application/json',
          href: `/stac/collections/${collectionId}`
        }
      ],
      context: {
        returned: items.length,
        limit: limit
      }
    };
  }

  async getItem(collectionId, itemId) {
    const collectionInfo = this.collections.get(collectionId);
    if (!collectionInfo) {
      return null;
    }

    const items = await this.loadItemsFromGeoparquet(collectionInfo.geoparquetUrl, 1000, 0);
    const item = items.find(i => i.id === itemId);
    
    if (item) {
      item.links = [
        {
          rel: 'self',
          type: 'application/geo+json',
          href: `/stac/collections/${collectionId}/items/${itemId}`
        },
        {
          rel: 'root',
          type: 'application/json',
          href: '/stac'
        },
        {
          rel: 'collection',
          type: 'application/json',
          href: `/stac/collections/${collectionId}`
        },
        {
          rel: 'parent',
          type: 'application/json',
          href: `/stac/collections/${collectionId}`
        }
      ];
    }

    return item;
  }

  async search(params) {
    const { collections, limit = 10, bbox, datetime } = params;
    let allItems = [];

    const collectionsToSearch = collections 
      ? collections.split(',').filter(c => this.collections.has(c))
      : Array.from(this.collections.keys());

    for (const collectionId of collectionsToSearch) {
      const collectionInfo = this.collections.get(collectionId);
      if (collectionInfo) {
        const items = await this.loadItemsFromGeoparquet(
          collectionInfo.geoparquetUrl, 
          limit, 
          0, 
          bbox
        );
        allItems = allItems.concat(items);
        if (allItems.length >= limit) {
          allItems = allItems.slice(0, limit);
          break;
        }
      }
    }

    return {
      type: 'FeatureCollection',
      features: allItems,
      links: [
        {
          rel: 'self',
          type: 'application/geo+json',
          href: '/stac/search'
        },
        {
          rel: 'root',
          type: 'application/json',
          href: '/stac'
        }
      ],
      context: {
        returned: allItems.length,
        limit: limit
      }
    };
  }

  async loadItemsFromGeoparquet(url, limit = 10, offset = 0, bbox = null) {
    const cacheKey = `${url}`;
    
    if (!this.itemsCache.has(cacheKey)) {
      try {
        const items = await this.readGeoparquet(url);
        this.itemsCache.set(cacheKey, items);
      } catch (err) {
        this.logger.error(`Error reading geoparquet from ${url}: ${err.message}`);
        return [];
      }
    }

    let items = this.itemsCache.get(cacheKey) || [];

    if (bbox) {
      const [minLon, minLat, maxLon, maxLat] = bbox.split(',').map(parseFloat);
      items = items.filter(item => {
        if (!item.geometry || !item.geometry.coordinates) return false;
        const [lon, lat] = item.geometry.coordinates;
        return lon >= minLon && lon <= maxLon && lat >= minLat && lat <= maxLat;
      });
    }

    return items.slice(offset, offset + limit);
  }

  async readGeoparquet(url) {
    const fetch = (await import('node-fetch')).default;
    
    try {
      this.logger.info(`Fetching geoparquet from ${url}`);
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`Failed to fetch ${url}: ${response.statusText}`);
      }

      const buffer = await response.arrayBuffer();
      this.logger.info(`Downloaded ${buffer.byteLength} bytes`);

      const items = [];
      
      await this.parquetRead({
        file: buffer,
        onComplete: (data) => {
          this.logger.info(`Parquet read complete, processing ${Object.keys(data).length} columns`);
          
          const numRows = data[Object.keys(data)[0]]?.length || 0;
          this.logger.info(`Processing ${numRows} rows`);
          this.logger.info(`Available columns: ${Object.keys(data).slice(0, 20).join(', ')}...`);
          
          for (let i = 0; i < numRows; i++) {
            const row = {};
            for (const key in data) {
              row[key] = data[key][i];
            }
            
            // Try to parse geometry if it's a string or binary
            if (row.geometry) {
              if (typeof row.geometry === 'string') {
                try {
                  row.geometry = JSON.parse(row.geometry);
                } catch (e) {
                  this.logger.warn(`Failed to parse geometry for row ${i}`);
                }
              }
            }
            
            // Try to parse properties if it's a string
            if (row.properties) {
              if (typeof row.properties === 'string') {
                try {
                  row.properties = JSON.parse(row.properties);
                } catch (e) {
                  row.properties = {};
                }
              }
            } else {
              row.properties = {};
            }

            // Try to parse assets if it's a string
            if (row.assets) {
              if (typeof row.assets === 'string') {
                try {
                  row.assets = JSON.parse(row.assets);
                } catch (e) {
                  row.assets = {};
                }
              }
            } else {
              row.assets = {};
            }

            const item = {
              type: 'Feature',
              stac_version: row.stac_version || '1.0.0',
              id: row.id || `item-${i}`,
              geometry: row.geometry || null,
              properties: row.properties,
              assets: row.assets,
              collection: row.collection || null,
              links: []
            };

            if (row.bbox) {
              item.bbox = typeof row.bbox === 'string' ? JSON.parse(row.bbox) : row.bbox;
            }

            items.push(item);
          }
        }
      });

      this.logger.info(`Loaded ${items.length} items from ${url}`);
      return items;
    } catch (err) {
      this.logger.error(`Error reading geoparquet: ${err.message}`);
      return [];
    }
  }
}

module.exports = STACHandler;
