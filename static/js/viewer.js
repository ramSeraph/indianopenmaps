import * as maplibregl from 'https://esm.sh/maplibre-gl@5.6.2';
import { Protocol } from 'https://esm.sh/pmtiles@4.3.0';

import { BaseLayerPicker } from '/js/base_layer_picker.js';
import { SearchParamHandler } from '/js/search_param_handler.js';
import { ColorHandler } from '/js/color_handler.js';
import { VectorSourceHandler } from '/js/vector_source_handler.js';
import { PopupHandler, InspectButton } from '/js/inspect_control.js';
import { GeocoderControl } from '/js/geocoder_control.js';
import { SourcePanelControl } from '/js/source_panel_control.js';
import { RoutesHandler } from '/js/routes_handler.js';
import { TerrainHandler } from '/js/terrain_handler.js';


// Initialize search param handler
const searchParams = new SearchParamHandler();
const colorHandler = new ColorHandler();
const routesHandler = new RoutesHandler();
const terrainHandler = new TerrainHandler(searchParams);

var map = null;

const INDIA_CENTER = [76.5,22.5];
const INDIA_ZOOM = 4;

function getMapConfig() {

  const mapConfig = {
    'container': 'map',
    'hash': 'map',
    'style': {
      'version': 8,
      'sources': {},
      'layers': [],
    },
    'center': INDIA_CENTER,
    'zoom': INDIA_ZOOM,
    'maxZoom': 30,
  };

  const terrainSource = terrainHandler.getTerrainSource();

  mapConfig.style.sources[terrainHandler.TERRAIN_SOURCE_ID] = terrainSource;
  mapConfig.style.sources[terrainHandler.HILLSHADE_SOURCE_ID] = terrainSource;
  
  mapConfig.style.layers.push(terrainHandler.getHillShadeLayer());
  mapConfig.style['sky'] = {}

  return mapConfig;
}

function setupMarker(map) {
  // Add marker if coordinates provided
  const markerLat = searchParams.getMarkerLat();
  const markerLon = searchParams.getMarkerLon();

  if (markerLat === null || markerLon === null) {
    return;
  }

  const marker = new maplibregl.Marker({ color: '#DBDBDB', draggable: false })
                               .setLngLat([markerLon, markerLat])
                               .addTo(map);

  map.setCenter([markerLon, markerLat]);
  map.setZoom(14);
}


function setupMap() {

  const mapConfig = getMapConfig();
  map = new maplibregl.Map(mapConfig);

  map.addControl(new maplibregl.FullscreenControl(), 'top-right');
  map.addControl(new maplibregl.NavigationControl(), 'top-right');

  map.addControl(new maplibregl.GeolocateControl({
    positionOptions: { enableHighAccuracy: true },
    trackUserLocation: true
  }), 'top-right');

  map.addControl(new GeocoderControl(), 'top-left');
  
  let vectorSourceHandler = new VectorSourceHandler(map, colorHandler, searchParams);

  let baseLayerPicker = new BaseLayerPicker(map, colorHandler, searchParams, routesHandler, terrainHandler, vectorSourceHandler);
  map.addControl(baseLayerPicker, 'top-left');
  
  let sourcePanelControl = new SourcePanelControl(searchParams, routesHandler, vectorSourceHandler);
  map.addControl(sourcePanelControl, 'top-left');

  let popupHandler = new PopupHandler(map, vectorSourceHandler.layers);
  map.addControl(new InspectButton(true, (enabled) => popupHandler.enable(enabled)), 'top-right');

  map.addControl(terrainHandler.getControl(), 'top-right');
  
  map.once('load', function () {
    terrainHandler.initTerrain(map);
    sourcePanelControl.loadAvailableSources();
  });
  
  map.on('terrain', (e) => {
    terrainHandler.terrainChangeCallback(map);
  });

  map.on('mousemove', (e) => popupHandler.showPopup(e));

  setupMarker(map);

}

function setupPmtilesProtocol() {
  let protocol = new Protocol();
  maplibregl.addProtocol("pmtiles", protocol.tile);
}

function init() {
  routesHandler.fetchRoutes().then(() => {
    setupMap();
  }).catch((error) => {
    console.error('Error initializing routes:', error);
  });
}

// actual initialization

setupPmtilesProtocol();

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
