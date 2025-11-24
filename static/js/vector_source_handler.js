// Vector source and layer management
import * as maplibregl from 'https://esm.sh/maplibre-gl@5.6.2';

export class VectorSourceHandler {
  constructor(map, colorHandler, searchParams) {
    this.map = map;
    this.layers = {
      pts: [],
      lines: [],
      polygons: []
    };
    this.selectedSources = new Map();

    this.colorHandler = colorHandler;
    this.searchParams = searchParams;
    this.colorType = colorHandler.LIGHT;
  }

  getSourceName(sourcePath) {
    return `source-${sourcePath.replace(/\//g, '-')}`;
  }
 
  updateUrlWithSelectedSources() {
    const sourcePaths = Array.from(this.selectedSources.keys());
    this.searchParams.updateSources(sourcePaths);
  }

  updateColorChoice(colorType) {
    this.colorType = colorType;
    this.updateVectorColours();
  }

  getColor(sourceData) {
    return sourceData.colors[this.colorType];
  }

  addLayers(e, sourcePath) {
    const srcName = this.getSourceName(sourcePath);
    if (!this.map.getSource(srcName) || !this.map.isSourceLoaded(srcName) || !e.isSourceLoaded) {
      return;
    }
    
    const sourceData = this.selectedSources.get(sourcePath);
    
    if (!sourceData || !sourceData.vectorLayers) return;
    
    const vectorLayerIds = sourceData.vectorLayers;
    const layerColor = '#' + this.getColor(sourceData);
    
    for (const layerId of vectorLayerIds) {
      const fullLayerId = `${sourcePath}-${layerId}`;
      
      this.map.addLayer({
        'id': `${fullLayerId}-polygons`,
        'type': 'fill',
        'source': srcName,
        'source-layer': `${layerId}`,
        'filter': ["==", "$type", "Polygon"],
        'layout': {},
        'paint': {
          'fill-opacity': 0.1,
          'fill-color': layerColor
        }
      });
      
      this.map.addLayer({
        'id': `${fullLayerId}-polygons-outline`,
        'type': 'line',
        'source': srcName,
        'source-layer': `${layerId}`,
        'filter': ["==", "$type", "Polygon"],
        'layout': {
          'line-join': 'round',
          'line-cap': 'round'
        },
        'paint': {
          'line-color': layerColor,
          'line-width': 1,
          'line-opacity': 0.75
        }
      });

      this.map.addLayer({
        'id': `${fullLayerId}-polygons-extrusions`,
        'type': 'fill-extrusion',
        'source': srcName,
        'source-layer': `${layerId}`,
        'minzoom': 14,
        'filter': [
          "all",
          ["==", "$type", "Polygon"],
          [
            "any",
            ["has", "Height"],
            ["has", "HEIGHT"],
            ["has", "height"],
          ]
        ],
        'paint': {
          'fill-extrusion-color': layerColor,
          'fill-extrusion-height': [
            "case", 
            ["has", "Height"], 
            ['get', 'Height'],
            ["has", "HEIGHT"], 
            ['get', 'HEIGHT'],
            ["has", "height"], 
            ['get', 'height'],
            0
          ],
          'fill-extrusion-opacity': 0.60,
        }
      });
     
      this.map.addLayer({
        'id': `${fullLayerId}-lines`,
        'type': 'line',
        'source': srcName,
        'source-layer': `${layerId}`,
        'filter': ["==", "$type", "LineString"],
        'layout': {
          'line-join': 'round',
          'line-cap': 'round'
        },
        'paint': {
          'line-color': layerColor,
          'line-width': 1,
          'line-opacity': 0.75
        }
      });
      
      this.map.addLayer({
        'id': `${fullLayerId}-pts`,
        'type': 'circle',
        'source': srcName,
        'source-layer': `${layerId}`,
        'filter': ["==", "$type", "Point"],
        'paint': {
          'circle-color': layerColor,
          'circle-radius': 2.5,
          'circle-opacity': 0.75
        }
      });
      
      this.layers.polygons.push(`${fullLayerId}-polygons`);
      this.layers.polygons.push(`${fullLayerId}-polygons-outline`);
      this.layers.lines.push(`${fullLayerId}-lines`);
      this.layers.pts.push(`${fullLayerId}-pts`);
    }
  }

  removeVectorSource(sourcePath) {
    const srcName = this.getSourceName(sourcePath);
    const sourceData = this.selectedSources.get(sourcePath);
    
    if (!sourceData) return;
    
    // Get layers from stored source data before removing
    if (this.map.getSource(srcName) && sourceData.vectorLayers) {
      const vectorLayerIds = sourceData.vectorLayers;
      
      for (const layerId of vectorLayerIds) {
        const fullLayerId = `${sourcePath}-${layerId}`;
        const layersToRemove = [
          `${fullLayerId}-polygons`,
          `${fullLayerId}-polygons-outline`,
          `${fullLayerId}-polygons-extrusions`,
          `${fullLayerId}-lines`,
          `${fullLayerId}-pts`
        ];
        for (const layer of layersToRemove) {
          if (this.map.getLayer(layer)) {
            this.map.removeLayer(layer);
            // Remove from tracking arrays
            this.layers.polygons = this.layers.polygons.filter(l => l !== layer);
            this.layers.lines = this.layers.lines.filter(l => l !== layer);
            this.layers.pts = this.layers.pts.filter(l => l !== layer);
          }
        }
      }
      
      this.map.removeSource(srcName);
    }
    
    this.colorHandler.releaseColors(sourceData.colors);
    this.selectedSources.delete(sourcePath);
    this.updateUrlWithSelectedSources();
  }

  getMaxSources() {
    return this.colorHandler.minColorLength();
  }

  addVectorSource(sourceInfo) {
    if (this.selectedSources.has(sourceInfo.path)) {
      return; // Already added
    }

    const maxSourcesAllowed = this.colorHandler.minColorLength();
    
    if (this.selectedSources.size >= maxSourcesAllowed) {
      alert(`Maximum ${maxSourcesAllowed} sources can be selected at once.`);
      return;
    }
    
    const colors = this.colorHandler.assignColors();
    this.selectedSources.set(sourceInfo.path, {
      name: sourceInfo.name,
      colors: colors,
      vectorLayers: null
    });
    
    const srcName = this.getSourceName(sourceInfo.path);
    const tileJsonUrl = new URL(`${sourceInfo.path}tiles.json`, window.location.origin).href;
    
    // Fetch TileJSON first to get attribution, then add source
    fetch(tileJsonUrl)
      .then(response => response.json())
      .then(tileJson => {
        const attribution = tileJson.attribution || '';
        const attributionWithName = `<strong>${sourceInfo.name}:</strong> ${attribution}`;
        
        // Store vector layer IDs from TileJSON
        const vectorLayerIds = (tileJson.vector_layers || []).map(layer => layer.id);
        const sourceData = this.selectedSources.get(sourceInfo.path);
        if (sourceData) {
          sourceData.vectorLayers = vectorLayerIds;
        }
        
        // Add source with modified TileJSON
        this.map.addSource(srcName, {
          'type': 'vector',
          'tiles': tileJson.tiles,
          'attribution': attributionWithName,
          'minzoom': tileJson.minzoom,
          'maxzoom': tileJson.maxzoom,
          'bounds': tileJson.bounds,
          'scheme': tileJson.scheme
        });
        
        const handler = (e) => {
          if (e.sourceId === srcName && e.isSourceLoaded) {
            const sourceData = this.selectedSources.get(sourceInfo.path);
            if (sourceData && sourceData.vectorLayers && sourceData.vectorLayers.length > 0) {
              this.addLayers(e, sourceInfo.path);
              this.map.off('sourcedata', handler);
            }
          }
        };
        
        this.map.on('sourcedata', handler);
      })
      .catch(error => {
        console.error(`Error loading TileJSON for ${sourceInfo.name}:`, error);
        // Remove from selected sources since it failed
        const sourceData = this.selectedSources.get(sourceInfo.path);
        if (sourceData) {
          this.colorHandler.releaseColors(sourceData.colors);
        }
        this.selectedSources.delete(sourceInfo.path);
        this.updateUrlWithSelectedSources();
      });
    
    this.updateUrlWithSelectedSources();
  }

  updateVectorColours() {
    for (const [sourcePath, sourceData] of this.selectedSources) {
      const srcName = this.getSourceName(sourcePath);
      if (!this.map.getSource(srcName)) continue;
      
      const src = this.map.getSource(srcName);
      const vectorLayerIds = src.vectorLayerIds || [];
      const layerColor = '#' + this.colorHandler.getColor(sourceData);
      
      for (const layerId of vectorLayerIds) {
        const fullLayerId = `${sourcePath}-${layerId}`;
        
        if (this.map.getLayer(`${fullLayerId}-polygons`)) {
          this.map.setPaintProperty(`${fullLayerId}-polygons`, 'fill-color', layerColor);
        }
        if (this.map.getLayer(`${fullLayerId}-polygons-outline`)) {
          this.map.setPaintProperty(`${fullLayerId}-polygons-outline`, 'line-color', layerColor);
        }
        if (this.map.getLayer(`${fullLayerId}-lines`)) {
          this.map.setPaintProperty(`${fullLayerId}-lines`, 'line-color', layerColor);
        }
        if (this.map.getLayer(`${fullLayerId}-pts`)) {
          this.map.setPaintProperty(`${fullLayerId}-pts`, 'circle-color', layerColor);
        }
        if (this.map.getLayer(`${fullLayerId}-polygons-extrusions`)) {
          this.map.setPaintProperty(`${fullLayerId}-polygons-extrusions`, 'fill-extrusion-color', layerColor);
        }
      }
    }
  }
}
