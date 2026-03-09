// Extent visualization handler: shows parquet data extents and row group extents
// as rectangles on the map. Single checkbox toggles both layers simultaneously.

import { parquetMetadata } from './parquet_metadata.js';

const LAYER_CONFIGS = {
  data: {
    sourceId: 'data-extents',
    labelSourceId: 'data-extents-labels-src',
    fillLayer: 'data-extents-fill',
    lineLayer: 'data-extents-line',
    labelLayer: 'data-extents-labels',
    fillColor: 'rgba(255, 152, 0, 0.12)',
    fillHoverColor: 'rgba(255, 152, 0, 0.35)',
    lineColor: 'rgba(255, 152, 0, 0.8)',
    lineHoverColor: 'rgba(255, 200, 0, 1)',
    textColor: '#FF9800',
  },
  rg: {
    sourceId: 'rg-extents',
    labelSourceId: 'rg-extents-labels-src',
    fillLayer: 'rg-extents-fill',
    lineLayer: 'rg-extents-line',
    labelLayer: 'rg-extents-labels',
    fillColor: 'rgba(0, 188, 212, 0.10)',
    fillHoverColor: 'rgba(0, 188, 212, 0.30)',
    lineColor: 'rgba(0, 188, 212, 0.7)',
    lineHoverColor: 'rgba(0, 230, 255, 1)',
    textColor: '#00BCD4',
  },
};

export class ExtentHandler {
  constructor(routesHandler) {
    this.routesHandler = routesHandler;
    this.map = null;
    this.checkbox = null;
    this._hoverHandlers = [];
    this._hoveredFeatures = new Map();
    this.statusEl = null;
    this.loading = false;
    this.onLoadingChange = null;
  }

  createCheckbox() {
    const container = document.createElement('div');
    container.className = 'extents-checkbox-container';

    const row = document.createElement('div');
    row.className = 'extents-checkbox-row';

    const label = document.createElement('label');
    label.className = 'extents-checkbox-label';

    this.checkbox = document.createElement('input');
    this.checkbox.type = 'checkbox';
    this.checkbox.className = 'extents-checkbox';

    label.appendChild(this.checkbox);
    label.appendChild(document.createTextNode(' Show Data Extents'));
    row.appendChild(label);

    this.statusEl = document.createElement('span');
    this.statusEl.className = 'extents-status';
    row.appendChild(this.statusEl);

    this.checkbox.addEventListener('change', () => this._onToggle());
    container.appendChild(row);
    return container;
  }

  setMap(map) {
    this.map = map;
  }

  reset() {
    this._removeAll();
    if (this.checkbox) this.checkbox.checked = false;
    this._setStatus('');
  }

  setSourcePath(sourcePath) {
    this._sourcePath = sourcePath;
  }

  destroy() {
    this._removeAll();
    this.map = null;
  }

  // --- Toggle ---

  async _onToggle() {
    if (!this._sourcePath) { this.checkbox.checked = false; return; }
    if (this.checkbox.checked) {
      await this._showAll(this._sourcePath);
    } else {
      this._removeAll();
    }
  }

  async _showAll(sourcePath) {
    if (!this.map) return;
    this._removeAll();
    this._setLoading(true);

    try {
      const routes = this.routesHandler.getVectorSources();
      const routeInfo = routes[sourcePath];
      if (!routeInfo?.url) {
        this._setStatus('No source URL');
        return;
      }

      const isPartitioned = routeInfo.partitioned_parquet === true;

      if (isPartitioned) {
        await this._showPartitioned(routeInfo);
      } else {
        await this._showSingle(routeInfo);
      }
    } catch (error) {
      console.error('[ExtentHandler] Failed to show extents:', error);
      this._setStatus('Error loading extents');
    } finally {
      this._setLoading(false);
    }
  }

  async _showPartitioned(routeInfo) {
    const metaUrl = parquetMetadata.getMetaJsonUrl(routeInfo.url);
    await parquetMetadata.fetchMetaJson(metaUrl);
    const dataExtents = parquetMetadata.getExtents(metaUrl);

    let rgExtents = null;
    const partitions = await parquetMetadata.getPartitions(metaUrl);
    if (partitions?.length) {
      const baseUrl = parquetMetadata.getBaseUrl(routeInfo.url);
      this._setStatus('Loading row groups...');
      const allRgBboxes = await parquetMetadata.getRowGroupBboxesMulti(
        partitions.map(p => baseUrl + p)
      );
      if (allRgBboxes) {
        rgExtents = {};
        for (const [filename, rgGroups] of Object.entries(allRgBboxes)) {
          const partLabel = this._extractLabel(filename);
          for (const [rgKey, bbox] of Object.entries(rgGroups)) {
            rgExtents[partLabel ? `${partLabel}.${rgKey.replace('rg_', '')}` : rgKey] = bbox;
          }
        }
        if (!Object.keys(rgExtents).length) rgExtents = null;
      }
    }

    this._showExtentLayers(dataExtents, rgExtents);
  }

  async _showSingle(routeInfo) {
    const parquetUrl = parquetMetadata.getParquetUrl(routeInfo.url);
    const bbox = await parquetMetadata.getParquetBbox(parquetUrl);

    this._setStatus('Loading row groups...');
    const rgExtents = await parquetMetadata.getRowGroupBboxes(parquetUrl);

    let dataExtents = null;
    if (bbox) {
      const filename = parquetUrl.substring(parquetUrl.lastIndexOf('/') + 1);
      dataExtents = { [filename]: bbox };
    }
    this._showExtentLayers(dataExtents, rgExtents);
  }

  /** Add layers to map: row groups (bottom), then file extents (top) */
  _showExtentLayers(dataExtents, rgExtents) {
    const hasRg = rgExtents && Object.keys(rgExtents).length;
    const hasData = dataExtents && Object.keys(dataExtents).length;
    if (!hasRg && !hasData) {
      this._setStatus('Extents are invalid and cannot be displayed');
      return;
    }
    if (hasRg) this._addExtentLayer(LAYER_CONFIGS.rg, rgExtents);
    if (hasData) this._addExtentLayer(LAYER_CONFIGS.data, dataExtents);
    this._setStatus('');
  }

  _removeAll() {
    if (!this.map) return;
    this._removeHoverHandlers();
    for (const cfg of Object.values(LAYER_CONFIGS)) {
      this._removeExtentLayer(cfg);
    }
  }

  // --- Map layer helpers ---

  _bboxCoords(bbox) {
    if (Array.isArray(bbox)) return bbox;
    return [bbox.minx, bbox.miny, bbox.maxx, bbox.maxy];
  }

  _buildGeoJSON(extents) {
    const polyFeatures = [];
    const labelFeatures = [];
    for (const [name, bbox] of Object.entries(extents)) {
      const [minx, miny, maxx, maxy] = this._bboxCoords(bbox);
      const label = this._extractLabel(name) ?? '';
      polyFeatures.push({
        type: 'Feature',
        properties: { name, label },
        geometry: {
          type: 'Polygon',
          coordinates: [[[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]],
        },
      });
      if (label) {
        labelFeatures.push({
          type: 'Feature',
          properties: { label },
          geometry: { type: 'Point', coordinates: [minx, maxy] },
        });
      }
    }
    return {
      polygons: { type: 'FeatureCollection', features: polyFeatures },
      labelPoints: { type: 'FeatureCollection', features: labelFeatures },
    };
  }

  _addExtentLayer(cfg, extents) {
    const { polygons, labelPoints } = this._buildGeoJSON(extents);
    this.map.addSource(cfg.sourceId, { type: 'geojson', data: polygons, generateId: true });
    this.map.addLayer({
      id: cfg.fillLayer, type: 'fill', source: cfg.sourceId,
      paint: {
        'fill-color': ['case', ['boolean', ['feature-state', 'hover'], false],
          cfg.fillHoverColor, cfg.fillColor],
      },
    });
    this.map.addLayer({
      id: cfg.lineLayer, type: 'line', source: cfg.sourceId,
      paint: {
        'line-color': ['case', ['boolean', ['feature-state', 'hover'], false],
          cfg.lineHoverColor, cfg.lineColor],
        'line-width': ['case', ['boolean', ['feature-state', 'hover'], false], 2.5, 1.5],
      },
    });
    if (labelPoints.features.length > 1) {
      this.map.addSource(cfg.labelSourceId, { type: 'geojson', data: labelPoints });
      this.map.addLayer({
        id: cfg.labelLayer, type: 'symbol', source: cfg.labelSourceId,
        layout: {
          'text-field': ['get', 'label'],
          'text-size': 11,
          'text-anchor': 'top-left',
          'text-offset': [0.3, 0.3],
          'text-allow-overlap': true,
          'text-font': ['Open Sans Semibold'],
        },
        paint: {
          'text-color': cfg.textColor,
          'text-halo-color': 'rgba(0, 0, 0, 0.7)',
          'text-halo-width': 1,
        },
      });
    }
    this._addHoverHandlers(cfg);
  }

  _removeExtentLayer(cfg) {
    for (const layer of [cfg.labelLayer, cfg.lineLayer, cfg.fillLayer]) {
      if (this.map.getLayer(layer)) this.map.removeLayer(layer);
    }
    for (const src of [cfg.labelSourceId, cfg.sourceId]) {
      if (this.map.getSource(src)) this.map.removeSource(src);
    }
  }

  _addHoverHandlers(cfg) {
    const onMove = (e) => {
      const features = this.map.queryRenderedFeatures(e.point, { layers: [cfg.fillLayer] });
      const prevIds = this._hoveredFeatures.get(cfg.sourceId) || new Set();
      const nextIds = new Set(features.map(f => f.id));
      for (const id of prevIds) {
        if (!nextIds.has(id)) {
          this.map.setFeatureState({ source: cfg.sourceId, id }, { hover: false });
        }
      }
      for (const id of nextIds) {
        if (!prevIds.has(id)) {
          this.map.setFeatureState({ source: cfg.sourceId, id }, { hover: true });
        }
      }
      this._hoveredFeatures.set(cfg.sourceId, nextIds);
      this.map.getCanvas().style.cursor = nextIds.size ? 'pointer' : '';
    };
    const onLeave = () => {
      const prevIds = this._hoveredFeatures.get(cfg.sourceId);
      if (prevIds) {
        for (const id of prevIds) {
          this.map.setFeatureState({ source: cfg.sourceId, id }, { hover: false });
        }
        this._hoveredFeatures.delete(cfg.sourceId);
      }
      this.map.getCanvas().style.cursor = '';
    };
    this.map.on('mousemove', cfg.fillLayer, onMove);
    this.map.on('mouseleave', cfg.fillLayer, onLeave);
    this._hoverHandlers.push({ layer: cfg.fillLayer, onMove, onLeave });
  }

  _removeHoverHandlers() {
    for (const { layer, onMove, onLeave } of this._hoverHandlers) {
      this.map.off('mousemove', layer, onMove);
      this.map.off('mouseleave', layer, onLeave);
    }
    this._hoverHandlers.length = 0;
    this._hoveredFeatures.clear();
  }

  _extractLabel(name) {
    const clean = name.replace(/\.parquet$/, '');
    const dotMatch = clean.match(/\.(\d+)$/);
    if (dotMatch) return dotMatch[1];
    const rgMatch = clean.match(/^rg_(\d+)$/);
    if (rgMatch) return rgMatch[1];
    return null;
  }

  _setStatus(text) {
    if (this.statusEl) this.statusEl.textContent = text;
  }

  _setLoading(isLoading) {
    this.loading = isLoading;
    if (isLoading) this._setStatus('Loading...');
    if (this.onLoadingChange) this.onLoadingChange(isLoading);
  }
}
