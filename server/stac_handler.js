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
      license: 'various',
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
          // hyparquet returns data with row indices as keys
          // Each row is an array of column values in schema order:
          // [id, assets, bbox, geometry, links, stac_extensions, stac_version, type, datetime, proj:epsg, proj:shape, proj:transform]
          const rowKeys = Object.keys(data);
          this.logger.info(`Parquet read complete, processing ${rowKeys.length} rows`);
          
          for (const rowKey of rowKeys) {
            const row = data[rowKey];
            if (!Array.isArray(row)) continue;
            
            const [id, assets, bbox, geometryWkb, links, stacExtensions, stacVersion, type, datetime, projEpsg, projShape, projTransform] = row;
            
            // Convert bbox struct to array format and create geometry from it
            let bboxArray = null;
            let geometry = null;
            if (bbox && typeof bbox === 'object') {
              bboxArray = [bbox.xmin, bbox.ymin, bbox.xmax, bbox.ymax];
              // Create polygon geometry from bbox
              geometry = {
                type: 'Polygon',
                coordinates: [[
                  [bbox.xmin, bbox.ymin],
                  [bbox.xmax, bbox.ymin],
                  [bbox.xmax, bbox.ymax],
                  [bbox.xmin, bbox.ymax],
                  [bbox.xmin, bbox.ymin]
                ]]
              };
            }
            
            // Build properties from extra columns
            const properties = {};
            if (datetime) {
              properties.datetime = datetime instanceof Date ? datetime.toISOString() : datetime;
            }
            if (projEpsg) properties['proj:epsg'] = Number(projEpsg);
            if (projShape) properties['proj:shape'] = Array.isArray(projShape) ? projShape.map(Number) : projShape;
            if (projTransform) properties['proj:transform'] = projTransform;

            const item = {
              type: 'Feature',
              stac_version: stacVersion || '1.0.0',
              id: id || `item-${rowKey}`,
              geometry: geometry,
              bbox: bboxArray,
              properties: properties,
              assets: assets || {},
              collection: null,
              links: []
            };

            if (stacExtensions && Array.isArray(stacExtensions) && stacExtensions.length > 0) {
              item.stac_extensions = stacExtensions;
            }

            items.push(item);
          }
        }
      });

      this.logger.info(`Loaded ${items.length} items from ${url}`);
      return items;
    } catch (err) {
      this.logger.error(`Error reading geoparquet: ${err.message}`);
      this.logger.error(err.stack);
      return [];
    }
  }
}

module.exports = STACHandler;
