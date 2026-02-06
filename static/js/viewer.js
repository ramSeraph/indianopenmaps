import * as maplibregl from 'https://esm.sh/maplibre-gl@5.6.2';
import { Protocol } from 'https://esm.sh/pmtiles@4.3.0';

import { BaseLayerPicker } from '/js/base_layer_picker.js';
import { SearchParamHandler } from '/js/search_param_handler.js';
import { ColorHandler } from '/js/color_handler.js';
import { VectorSourceHandler } from '/js/vector_source_handler.js';
import { PopupHandler, InspectButton } from '/js/inspect_control.js';
import { nominatimGeocoder } from '/js/nominatim_geocoder.js';
import { SourcePanelControl } from '/js/source_panel_control.js';
import { DownloadPanelControl } from '/js/download_panel_control.js';
import { RoutesHandler } from '/js/routes_handler.js';
import { TerrainHandler } from '/js/terrain_handler.js';

import { registerCorrectionProtocol } from 'https://esm.sh/@india-boundary-corrector/maplibre-protocol@latest';


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

  map.flyTo({
    center: [markerLon, markerLat],
    zoom: 14
  });
}


function setupMap() {

  const mapConfig = getMapConfig();
  mapConfig.attributionControl = false; // Disable default attribution
  
  map = new maplibregl.Map(mapConfig);
  
  // Add custom attribution control - always collapsible, collapsed by default on mobile
  const isMobile = window.innerWidth <= 480;
  const attributionControl = new maplibregl.AttributionControl({
    compact: true
  });
  map.addControl(attributionControl);
  
  // Collapse attribution on mobile after attributions are loaded
  if (isMobile) {
    const collapseAttribution = () => {
      const container = document.querySelector('.maplibregl-ctrl-attrib');
      if (container) {
        container.removeAttribute('open');
        container.classList.remove('maplibregl-compact-show');
      }
    };
    map.once('styledata', () => setTimeout(collapseAttribution, 100));
  }

  map.addControl(new maplibregl.FullscreenControl(), 'top-right');
  map.addControl(new maplibregl.NavigationControl(), 'top-right');

  map.addControl(new maplibregl.GeolocateControl({
    positionOptions: { enableHighAccuracy: true },
    trackUserLocation: true
  }), 'top-right');

  let geocoderMarker = null;

  const geocoder = new MaplibreGeocoder(nominatimGeocoder, {
    maplibregl: maplibregl,
    placeholder: 'Search using Nominatim..',
    showResultsWhileTyping: true,
    minLength: 3,
    marker: false
  });
  
  geocoder.on('result', (e) => {
    if (geocoderMarker) {
      geocoderMarker.remove();
    }
    geocoderMarker = new maplibregl.Marker({ color: '#FF5733' })
      .setLngLat(e.result.center)
      .setPopup(new maplibregl.Popup().setHTML(`<strong>${e.result.place_name}</strong>`))
      .addTo(map)
      .togglePopup();
  });
  
  map.addControl(geocoder, 'top-left');
  
  let vectorSourceHandler = new VectorSourceHandler(map, colorHandler, searchParams, routesHandler);

  let baseLayerPicker = new BaseLayerPicker(map, colorHandler, searchParams, routesHandler, terrainHandler, vectorSourceHandler);
  map.addControl(baseLayerPicker, 'top-left');

  registerCorrectionProtocol(maplibregl);
  
  let sourcePanelControl = new SourcePanelControl(searchParams, routesHandler, vectorSourceHandler);
  map.addControl(sourcePanelControl, 'top-left');

  let downloadPanelControl = new DownloadPanelControl(routesHandler, vectorSourceHandler);
  map.addControl(downloadPanelControl, 'top-left');
  
  // Wire up source change notifications
  sourcePanelControl.setOnSourceChangeCallback(() => {
    downloadPanelControl.updateSourceDropdown();
  });

  let popupHandler = new PopupHandler(map, vectorSourceHandler.layers, routesHandler, vectorSourceHandler);
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
