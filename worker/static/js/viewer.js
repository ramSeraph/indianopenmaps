import * as maplibregl from 'https://esm.sh/maplibre-gl@5.6.2';
import { Protocol } from 'https://esm.sh/pmtiles@4.3.0';

import { BaseLayerPicker } from '/js/base_layer_picker.js';
import { SearchParamHandler } from '/js/search_param_handler.js';
import { ColorHandler } from '/js/color_handler.js';
import { VectorSourceHandler } from '/js/vector_source_handler.js';
import { PopupHandler, HoverPopupToggleControl } from '/js/inspect_control.js';
import { nominatimGeocoder } from '/js/nominatim_geocoder.js';
import { SourcePanelControl } from '/js/source_panel_control.js';
import { DownloadPanelControl } from '/js/download_panel_control.js';
import { RoutesHandler } from '/js/routes_handler.js';
import { TerrainHandler } from '/js/terrain_handler.js';
import { SidebarControl } from '/js/sidebar_control.js';

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
      'glyphs': 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
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
  
  let vectorSourceHandler = new VectorSourceHandler({ map, colorHandler, searchParams, routesHandler });

  let baseLayerPicker = new BaseLayerPicker({ map, colorHandler, searchParams, routesHandler, terrainHandler, vectorSourceHandler });

  registerCorrectionProtocol(maplibregl);
  
  let sourcePanelControl = new SourcePanelControl({ searchParams, routesHandler, vectorSourceHandler });

  let downloadPanelControl = new DownloadPanelControl({ map, routesHandler, vectorSourceHandler });
  
  // Wire up source change notifications
  sourcePanelControl.addEventListener('sourcechange', () => {
    downloadPanelControl.updateSourceDropdown();
  });

  // Create sidebar and register panels
  const sidebar = new SidebarControl();
  
  // Create geocoder panel wrapper
  const geocoderPanel = document.createElement('div');
  geocoderPanel.className = 'geocoder-panel';
  const geocoderContainer = geocoder.onAdd(map);
  geocoderPanel.appendChild(geocoderContainer);
  
  // Register all panels with sidebar
  sidebar.registerPanel('search', 'search', 'Search Location', geocoderPanel);
  sidebar.registerPanel('layers', 'layers', 'Base Map', baseLayerPicker.createPanel());
  sidebar.registerPanel('sources', 'database', 'Vector Sources', sourcePanelControl.createPanel());
  sidebar.registerPanel('download', 'download', 'Download Data', downloadPanelControl.createPanel());
  
  // Add sidebar to map
  map.addControl(sidebar, 'top-left');

  let popupHandler = new PopupHandler({ map, layers: vectorSourceHandler.layers, routesHandler, vectorSourceHandler });
  let hoverToggleControl = new HoverPopupToggleControl(popupHandler);

  map.addControl(terrainHandler.getControl(), 'top-right');
  map.addControl(hoverToggleControl, 'top-right');
  map.addControl({
    onAdd() {
      const container = document.createElement('div');
      container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
      const link = document.createElement('a');
      link.href = 'https://github.com/ramSeraph/indianopenmaps';
      link.target = '_blank';
      link.title = 'GitHub';
      link.style.cssText = 'display:flex;align-items:center;justify-content:center;width:29px;height:29px;color:#333;';
      link.innerHTML = '<svg width="18" height="18" viewBox="0 0 16 16" fill="currentColor"><path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27s1.36.09 2 .27c1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z"/></svg>';
      container.appendChild(link);
      return container;
    },
    onRemove() {}
  }, 'top-right');
  
  map.once('load', function () {
    terrainHandler.initTerrain(map);
    sourcePanelControl.loadAvailableSources();
  });
  
  map.on('terrain', (e) => {
    terrainHandler.terrainChangeCallback(map);
  });

  map.on('mousemove', (e) => popupHandler.handleMouseMove(e));
  map.on('click', (e) => popupHandler.handleClick(e));

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
