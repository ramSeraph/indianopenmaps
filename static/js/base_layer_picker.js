

// Extracted BaseLayerPicker control
// Base layer definitions
export const ESRI_WORLD_IMAGERY_LAYER_NAME = 'ESRI World Imagery';
export const CARTO_OSM_DARK_LAYER_NAME = 'Carto OSM Dark';

const boundaryPmtilesUrl = 'https://raw.githubusercontent.com/ramSeraph/india_boundary_corrector/main/packages/data/india_bounndary_corrections.pmtiles';

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

  const Carto_Dark = {
    'name': CARTO_OSM_DARK_LAYER_NAME,
    'sources': {
      'carto-dark': {
        'type': 'raster',
        'tiles': [
          "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
          "https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
          "https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
          "https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png"
        ],
        'attribution': '<strong>Carto OSM Dark:</strong> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        'layers': [
          {
            'id': 'carto-dark-layer',
            'type': 'raster',
            'minZoom': 0,
            'maxZoom': 20,
          }
        ],
        'maxZoom': 20,
      },
      'india-boundary-correcter': {
        'type': 'vector',
        'url': `pmtiles://${boundaryPmtilesUrl}`,
        'layers': [
          {
            'id': 'to-add-osm',
            'source-layer': 'to-add-osm',
            'type': 'line',
            'layout': {
              'line-join': 'round',
              'line-cap': 'round',
            },
            'paint': {
              'line-color': "#262626",
              'line-width': [
                  "step",
                  ["zoom"],
                  2, 5,
                  3, 6,
                  4 
              ]
            }
          },
          {
            'id': 'to-del-osm',
            'source-layer': 'to-del-osm',
            'type': 'line',
            'layout': {
              'line-join': 'round',
              'line-cap': 'round',
            },
            'paint': {
              'line-color': "#090909",
              'line-width': [
                  "step",
                  ["zoom"],
                  3, 5,
                  4, 6,
                  5 
              ]
            }
          }
        ]
      }
    }
  };

  return [
    Carto_Dark,
    Esri_WorldImagery,
  ];
}

export function getLayersAndSources(layerInfo) {
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
  constructor(map, colorHandler, searchParams, routesHandler, terrainHandler, vectorSourceHandler) {
    this.map = map;

    this.colorHandler = colorHandler;
    this.routesHandler = routesHandler;
    this.searchParams = searchParams;
    this.terrainHandler = terrainHandler;
    this.HILLSHADE_LAYER_ID = terrainHandler.HILLSHADE_LAYER_ID;
    this.vectorSourceHandler = vectorSourceHandler;

    this.baseLayers = [];
    this.select = null;
    this.currentLayerName = null;
  }


  getLayerInfo(label) {
    for (let layerInfo of this.baseLayers) {
      if (layerInfo.name == label) return layerInfo;
    }
    return null;
  }

  colorChoice() {
    if (this.currentLayerName !== CARTO_OSM_DARK_LAYER_NAME && this.currentLayerName !== ESRI_WORLD_IMAGERY_LAYER_NAME) {
      return this.colorHandler.DARK;
    }
    return this.colorHandler.LIGHT;
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

  async switchLayer(layerName) {
    if (this.currentLayerName === layerName) return;

    if (this.currentLayerName) {
      const currentLayerInfo = this.getLayerInfo(this.currentLayerName);
      if (currentLayerInfo) {
        const [sources, layers] = getLayersAndSources(currentLayerInfo);
        for (const layer of layers) if (this.map.getLayer(layer.id)) this.map.removeLayer(layer.id);
        for (const [sname] of Object.entries(sources)) if (this.map.getSource(sname)) this.map.removeSource(sname);
      }
    }

    const newLayerInfo = this.getLayerInfo(layerName);
    if (newLayerInfo) {
      if (newLayerInfo.tileJsonUrl && Object.keys(newLayerInfo.sources).length === 0) {
        try {
          const resp = await fetch(newLayerInfo.tileJsonUrl);
          const tileJson = await resp.json();
          const attribution = tileJson.attribution || '';
          const attributionWithName = `<strong>${newLayerInfo.name}:</strong> ${attribution}`;
          const srcId = `raster-${newLayerInfo.path.replace(/\//g, '-')}`;
          const srcLayerId = `raster-layer-${newLayerInfo.path.replace(/\//g, '-')}`;
          newLayerInfo.sources = {
            srcId : {
              type: 'raster',
              tiles: tileJson.tiles || [],
              attribution: attributionWithName,
              minzoom: tileJson.minzoom || 0,
              maxzoom: tileJson.maxzoom || 15,
              bounds: tileJson.bounds,
              layers: [{
                id: srcLayerId,
                type: 'raster',
                minZoom: tileJson.minzoom || 0,
                maxZoom: tileJson.maxzoom || 15
              }]
            }
          };
        } catch (err) {
          console.error(`Error loading TileJSON for ${newLayerInfo.name}:`, err);
          return;
        }
      }

      const [sources, layers] = getLayersAndSources(newLayerInfo);
      for (const [sname, source] of Object.entries(sources)) this.map.addSource(sname, source);
      for (const layer of layers) {
        // Only specify 'before' if the hills layer exists
        if (this.map.getLayer(this.HILLSHADE_LAYER_ID)) {
          this.map.addLayer(layer, this.HILLSHADE_LAYER_ID);
        } else {
          this.map.addLayer(layer);
        }
      }
    }

    this.currentLayerName = layerName;
    this.searchParams.updateBaseLayer(layerName);
    this.vectorSourceHandler.updateColorChoice(this.colorChoice());
  }

  async initialize() {
    this.baseLayers = getDefaultBaseLayers();
    const rasterSources = this.loadRasterSources();
    for (const raster of rasterSources) {
      this.baseLayers.push({ name: raster.name, path: raster.path, tileJsonUrl: raster.tileJsonUrl, sources: {} });
    }
    this.baseLayers.forEach(layerInfo => {
      const option = document.createElement('option');
      option.value = layerInfo.name;
      option.textContent = layerInfo.name;
      this.select.appendChild(option);
    });

    const initialChoice = this.searchParams.getBaseLayer(CARTO_OSM_DARK_LAYER_NAME);
    this.select.value = initialChoice;
    
    // Wait for map style to be loaded before switching layers
    if (this.map.isStyleLoaded()) {
      this.switchLayer(initialChoice);
    } else {
      this.map.once('load', () => {
        this.switchLayer(initialChoice);
      });
    }
  }

  onAdd(map) {
    this.map = map;
    const div = document.createElement('div');
    div.className = 'maplibregl-ctrl maplibregl-ctrl-group';
    const select = document.createElement('select');
    select.style.padding = '5px 8px';
    select.style.fontSize = '12px';
    select.style.border = 'none';
    select.style.background = 'white';
    select.style.cursor = 'pointer';
    select.style.fontFamily = "'Open Sans', sans-serif";
    select.style.minWidth = 'auto';
    select.style.maxWidth = '300px';
    select.addEventListener('change', (e) => this.switchLayer(e.target.value));
    this.select = select;
    div.appendChild(select);
    this.initialize();
    return div;
  }
}
