import {
  getProtomapsBaseLayers,
  loadProtomapsStyle,
  fetchMapJSON,
  resolveBaseLayerName,
  PROTOMAPS_DEFAULT_LAYER_NAME,
} from './protomaps_basemaps.js';

// Extracted BaseLayerPicker control
// Base layer definitions
export const ESRI_WORLD_IMAGERY_LAYER_NAME = 'ESRI World Imagery';
export const OSM_CARTO_LAYER_NAME = 'OSM Carto';
export const GOOGLE_STREETS_LAYER_NAME = 'Google Streets';
export const GOOGLE_HYBRID_LAYER_NAME = 'Google Hybrid';

export function getDefaultBaseLayers() {
  const Esri_WorldImagery = {
    'name': ESRI_WORLD_IMAGERY_LAYER_NAME,
    'sources': {
      'esri-world-imagery': {
        'type': 'raster',
        'tiles': [ 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}' ],
        'attribution': '<strong>ESRI World Imagery:</strong> Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
        'layers': [
          {
            'id': 'esri-world-imagery-layer',
            'type': 'raster',
            'minZoom': 0,
            'maxZoom': 17,
          }
        ],
        'maxZoom': 17,
      }
    }
  };

  const OSM_Carto = {
    'name': OSM_CARTO_LAYER_NAME,
    'sources': {
      'osm-carto': {
        'type': 'raster',
        'tiles': [
          "ibc://https://a.tile.openstreetmap.org/{z}/{x}/{y}.png",
          "ibc://https://b.tile.openstreetmap.org/{z}/{x}/{y}.png",
          "ibc://https://c.tile.openstreetmap.org/{z}/{x}/{y}.png"
        ],
        'attribution': '<strong>OSM Carto:</strong> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        'layers': [
          {
            'id': 'osm-carto-layer',
            'type': 'raster',
            'minZoom': 0,
            'maxZoom': 19,
          }
        ],
        'maxZoom': 19,
        'tileSize': 256,
      }
    }
  };

  const Google_Streets = {
    'name': GOOGLE_STREETS_LAYER_NAME,
    'sources': {
      'google-streets': {
        'type': 'raster',
        'tiles': [
          'https://mt0.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
          'https://mt1.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
          'https://mt2.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
          'https://mt3.google.com/vt/lyrs=m&x={x}&y={y}&z={z}'
        ],
        'attribution': '<strong>Google Streets:</strong> Map data &copy; 2026 Google',
        'layers': [
          {
            'id': 'google-streets-layer',
            'type': 'raster',
            'minZoom': 0,
            'maxZoom': 20,
          }
        ],
        'maxZoom': 20,
      }
    }
  };

  const Google_Hybrid = {
    'name': GOOGLE_HYBRID_LAYER_NAME,
    'sources': {
      'google-hybrid': {
        'type': 'raster',
        'tiles': [
          'https://mt0.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}',
          'https://mt1.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}',
          'https://mt2.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}',
          'https://mt3.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}'
        ],
        'attribution': '<strong>Google Hybrid:</strong> Map data &copy; 2026 Google, Imagery &copy; 2026 TerraMetrics',
        'layers': [
          {
            'id': 'google-hybrid-layer',
            'type': 'raster',
            'minZoom': 0,
            'maxZoom': 20,
          }
        ],
        'maxZoom': 20,
      }
    }
  };

  return [
    ...getProtomapsBaseLayers(),
    OSM_Carto,
    Esri_WorldImagery,
    Google_Streets,
    Google_Hybrid,
  ];
}

export function getLayersAndSources(layerInfo) {
  if (layerInfo.layers) return [layerInfo.sources, layerInfo.layers];
  var sources = JSON.parse(JSON.stringify(layerInfo.sources));
  var layers = [];
  for (const [sname, source] of Object.entries(sources)) {
    var slayers = source.layers;
    for (var layer of slayers) {
      layer['source'] = sname;
    }
    layers = layers.concat(slayers);
    delete source.layers;
  }
  return [sources, layers];
}

export class BaseLayerPicker {
  constructor({ map, colorHandler, searchParams, routesHandler, terrainHandler, vectorSourceHandler }) {
    this.map = map;
    this.colorHandler = colorHandler;
    this.routesHandler = routesHandler;
    this.searchParams = searchParams;
    this.terrainHandler = terrainHandler;
    this.HILLSHADE_LAYER_ID = terrainHandler.HILLSHADE_LAYER_ID;
    this.vectorSourceHandler = vectorSourceHandler;

    this.baseLayers = [];
    this.container = null;
    this.layerList = null;
    this.currentLayerName = null;
    this.switchRequest = 0;
    this.statusElement = null;
  }


  getLayerInfo(label) {
    for (let layerInfo of this.baseLayers) {
      if (layerInfo.name == label) return layerInfo;
    }
    return null;
  }

  loadRasterSources() {
    try {
      const routes = this.routesHandler.getRasterSources();
      const rasterSources = [];
      for (const [path, info] of Object.entries(routes)) {
        const tileJsonUrl = `${window.location.origin}${path}tiles.json`;
        rasterSources.push({ name: info.name, path, tileJsonUrl });
      }
      return rasterSources;
    } catch (e) {
      console.error('Error loading raster sources:', e);
      return [];
    }
  }

  async loadLayerInfo(layerInfo) {
    if (layerInfo.styleUrl && !layerInfo.layers) {
      Object.assign(layerInfo, await loadProtomapsStyle(layerInfo));
    } else if (layerInfo.tileJsonUrl && Object.keys(layerInfo.sources).length === 0) {
      const tileJson = await fetchMapJSON(layerInfo.tileJsonUrl);
      const srcId = `raster-${layerInfo.path.replace(/\//g, '-')}`;
      layerInfo.sources = {
        [srcId]: {
          type: 'raster',
          tiles: tileJson.tiles || [],
          attribution: `<strong>${layerInfo.name}:</strong> ${tileJson.attribution || ''}`,
          minzoom: tileJson.minzoom ?? 0,
          maxzoom: tileJson.maxzoom ?? 15,
          ...(tileJson.bounds ? { bounds: tileJson.bounds } : {}),
          layers: [{
            id: `raster-layer-${layerInfo.path.replace(/\//g, '-')}`,
            type: 'raster',
          }],
        },
      };
    }
  }

  async switchLayer(layerName) {
    const request = ++this.switchRequest;
    layerName = resolveBaseLayerName(layerName);
    const newLayerInfo = this.getLayerInfo(layerName);
    if (!newLayerInfo) return;
    this.setStatus('');
    if (this.currentLayerName === layerName) return;

    try {
      this.setStatus(`Loading ${layerName}…`);
      // Keep the current map visible until the requested style is ready.
      await this.loadLayerInfo(newLayerInfo);
      if (request !== this.switchRequest) return;

      const [sources, layers] = getLayersAndSources(newLayerInfo);
      const currentLayerInfo = this.getLayerInfo(this.currentLayerName);
      if (currentLayerInfo) {
        const [oldSources, oldLayers] = getLayersAndSources(currentLayerInfo);
        for (const layer of oldLayers) {
          if (this.map.getLayer(layer.id)) this.map.removeLayer(layer.id);
        }
        for (const id of Object.keys(oldSources)) {
          // Protomaps themes share tiles; retain those sources when switching.
          if (!sources[id] && this.map.getSource(id)) this.map.removeSource(id);
        }
      }

      if (newLayerInfo.glyphs && this.map.getGlyphs() !== newLayerInfo.glyphs) {
        this.map.setGlyphs(newLayerInfo.glyphs);
      }
      this.map.setSprite(newLayerInfo.sprite || null);
      for (const [id, source] of Object.entries(sources)) {
        if (!this.map.getSource(id)) this.map.addSource(id, source);
      }
      // Insert below terrain and user data without resetting the map style.
      const beforeId = this.map.getLayer(this.HILLSHADE_LAYER_ID)
        ? this.HILLSHADE_LAYER_ID : this.map.getStyle().layers[0]?.id;
      for (const layer of layers) this.map.addLayer(layer, beforeId);

      this.currentLayerName = layerName;
      this.searchParams.updateBaseLayer(layerName);
      this._updateActiveState();
      this.setStatus('');
    } catch (error) {
      if (request !== this.switchRequest) return;
      console.error(`Error loading basemap ${layerName}:`, error);
      this.setStatus(`Could not load ${layerName}. Try selecting it again.`);
      this._updateActiveState();
    }
  }

  setStatus(message) {
    if (this.statusElement) this.statusElement.textContent = message;
  }

  _updateActiveState() {
    if (!this.layerList) return;
    
    this.layerList.querySelectorAll('.base-layer-option').forEach(option => {
      const isActive = option.dataset.layerName === this.currentLayerName;
      option.classList.toggle('active', isActive);
      const radio = option.querySelector('input[type="radio"]');
      if (radio) radio.checked = isActive;
    });
  }

  async initialize() {
    this.baseLayers = getDefaultBaseLayers();
    const rasterSources = this.loadRasterSources();
    for (const raster of rasterSources) {
      this.baseLayers.push({ name: raster.name, path: raster.path, tileJsonUrl: raster.tileJsonUrl, sources: {} });
    }
    
    // Populate layer list
    this.baseLayers.forEach(layerInfo => {
      const option = document.createElement('div');
      option.className = 'base-layer-option';
      option.dataset.layerName = layerInfo.name;
      
      const radio = document.createElement('input');
      radio.type = 'radio';
      radio.name = 'base-layer';
      radio.id = `base-layer-${layerInfo.name.replace(/\s+/g, '-')}`;
      
      const label = document.createElement('label');
      label.htmlFor = radio.id;
      label.textContent = layerInfo.name;
      
      option.appendChild(radio);
      option.appendChild(label);
      
      option.addEventListener('click', () => {
        this.switchLayer(layerInfo.name);
      });
      
      this.layerList.appendChild(option);
    });

    const requestedChoice = resolveBaseLayerName(this.searchParams.getBaseLayer(PROTOMAPS_DEFAULT_LAYER_NAME));
    const initialChoice = this.getLayerInfo(requestedChoice) ? requestedChoice : PROTOMAPS_DEFAULT_LAYER_NAME;
    
    // Wait for map style to be loaded before switching layers
    if (this.map.isStyleLoaded()) {
      this.switchLayer(initialChoice);
    } else {
      this.map.once('load', () => {
        this.switchLayer(initialChoice);
      });
    }
  }

  /**
   * Create the panel element for sidebar mounting
   * @returns {HTMLElement} The panel element
   */
  createPanel() {
    this.container = document.createElement('div');
    this.container.className = 'base-layer-panel';
    
    const title = document.createElement('div');
    title.className = 'panel-title';
    title.textContent = 'Base Map';
    
    this.layerList = document.createElement('div');
    this.layerList.className = 'base-layer-list';
    
    this.container.appendChild(title);
    this.container.appendChild(this.layerList);

    this.statusElement = document.createElement('div');
    this.statusElement.className = 'base-layer-status';
    this.statusElement.setAttribute('role', 'status');
    this.container.appendChild(this.statusElement);
    
    // Initialize layers
    this.initialize();
    
    return this.container;
  }

}
