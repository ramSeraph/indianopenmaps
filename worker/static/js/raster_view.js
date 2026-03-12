// Raster viewer with dual layer picker
// some of this html/js was copied from https://kokoalberti.com/articles/georeferencing-and-digitizing-old-maps-with-gdal/ and https://server.nikhilvj.co.in/pmgsy

import { extendLeaflet } from 'https://esm.sh/@india-boundary-corrector/leaflet-layer@latest';

// Preset base layers with short identifiers
const PRESETS = {
  'osm': {
    name: 'OpenStreetMap',
    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    options: { maxZoom: 19, subdomains: 'abc' },
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    indiaBoundary: true
  },
  'otm': {
    name: 'OpenTopoMap',
    url: 'https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png',
    options: { maxZoom: 17, subdomains: 'abc' },
    attribution: 'Map data: OpenStreetMap, SRTM | Map style: &copy; OpenTopoMap (CC-BY-SA)',
    indiaBoundary: true
  },
  'esri': {
    name: 'ESRI Satellite',
    url: 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
    options: { maxNativeZoom: 18, maxZoom: 20 },
    attribution: 'Tiles &copy; Esri',
    indiaBoundary: false
  },
  'gstreets': {
    name: 'Google Streets',
    url: 'https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}',
    options: { maxZoom: 20, subdomains: ['mt0','mt1','mt2','mt3'] },
    attribution: 'Map data &copy; Google',
    indiaBoundary: false
  },
  'ghybrid': {
    name: 'Google Hybrid',
    url: 'https://{s}.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}',
    options: { maxZoom: 20, subdomains: ['mt0','mt1','mt2','mt3'] },
    attribution: 'Map data &copy; Google, Imagery &copy; TerraMetrics',
    indiaBoundary: false
  }
};

let rasterRoutes = {};
let map1, map2;
let currentLeftLayer = null;
let currentRightLayer = null;
let geocoderMarker1 = null;
let geocoderMarker2 = null;

// Each panel shows exactly one layer, so we track and swap attribution explicitly
// instead of relying on Leaflet's reference-counted add/removeAttribution.
let shownLeftAttr = '';
let shownRightAttr = '';

function updateMapAttribution(map, newAttr) {
  const ctrl = map?.attributionControl;
  if (!ctrl) return;
  const isLeft = map === map1;
  const old = isLeft ? shownLeftAttr : shownRightAttr;
  if (old) ctrl.removeAttribution(old);
  if (newAttr) ctrl.addAttribution(newAttr);
  if (isLeft) shownLeftAttr = newAttr || '';
  else shownRightAttr = newAttr || '';
}

// Parse hash params
function getHashParams() {
  return new URLSearchParams(window.location.hash.substring(1));
}

// Update hash without triggering reload
function updateHash(params) {
  window.history.replaceState(null, null, '#' + params.toString());
}

// Get display name for identifier
function getDisplayName(id) {
  if (PRESETS[id]) return PRESETS[id].name;
  if (rasterRoutes[id]) return rasterRoutes[id].name || id;
  return id;
}

const TRANSPARENT_TILE = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAAC0lEQVQI12NgAAIABQABNjN9GQAAAABJRU5ErkJggg==';

// Build base layers object for L.control.layers
// Raster route tile JSONs are loaded lazily when the layer is first added to a map
function buildLayersObject() {
  const layers = {};
  
  // Add presets
  for (const [id, preset] of Object.entries(PRESETS)) {
    let layer;
    const opts = { ...preset.options }; // no attribution — managed explicitly
    if (preset.indiaBoundary) {
      layer = L.tileLayer.indiaBoundaryCorrected(preset.url, opts);
    } else {
      layer = L.tileLayer(preset.url, opts);
    }
    layer._layerId = id;
    layer._layerAttribution = preset.attribution;
    layers[preset.name] = layer;
  }
  
  // Add raster routes with lazy tile JSON loading
  for (const [id, info] of Object.entries(rasterRoutes)) {
    const layer = L.tileLayer(TRANSPARENT_TILE, {});
    layer._layerId = id;
    layer._tileJsonLoaded = false;
    layer._layerAttribution = '';
    
    layer.on('add', async function() {
      if (this._tileJsonLoaded) return;
      try {
        const response = await fetch(`${id}tiles.json`);
        const tileJSON = await response.json();
        this._tileJsonLoaded = true;
        this.options.maxNativeZoom = tileJSON.maxzoom;
        this.options.minZoom = tileJSON.minzoom;
        this._layerAttribution = tileJSON.attribution;
        this.setUrl(tileJSON.tiles[0]);
        if (this._map) {
          updateMapAttribution(this._map, tileJSON.attribution);
        }
      } catch (err) {
        console.error(`Failed to load ${id}:`, err);
      }
    });
    
    layers[info.name || id] = layer;
  }
  
  return layers;
}

// Fetch raster routes from API
async function fetchRasterRoutes() {
  try {
    const response = await fetch('/api/routes');
    const routes = await response.json();
    
    for (const [path, info] of Object.entries(routes)) {
      if (info.type === 'raster') {
        rasterRoutes[path] = info;
      }
    }
  } catch (err) {
    console.error('Failed to fetch routes:', err);
  }
}

// Initialize the viewer
async function init() {
  // Extend Leaflet with India boundary corrector
  extendLeaflet(L);
  
  // Fetch raster routes
  await fetchRasterRoutes();
  
  // Build layer objects (tile JSONs loaded lazily on demand)
  const leftLayers = buildLayersObject();
  const rightLayers = buildLayersObject();
  
  // Parse URL params
  const params = getHashParams();
  const leftId = params.get('left') || 'osm';
  const rightId = params.get('right') || 'osm';
  
  // Find initial layers by id
  function findLayerById(layers, id) {
    for (const [name, layer] of Object.entries(layers)) {
      if (layer._layerId === id) return { name, layer };
    }
    // Default to first layer (OSM)
    const firstName = Object.keys(layers)[0];
    return { name: firstName, layer: layers[firstName] };
  }
  
  const leftInitial = findLayerById(leftLayers, leftId);
  const rightInitial = findLayerById(rightLayers, rightId);
  
  // Parse map position
  let center = [20.5937, 78.9629]; // Default: India center
  let zoom = 5;
  
  const mapParam = params.get('map');
  if (mapParam) {
    const parts = mapParam.split('/');
    if (parts.length >= 3) {
      const z = parseFloat(parts[0]);
      const lat = parseFloat(parts[1]);
      const lng = parseFloat(parts[2]);
      if (!isNaN(z) && !isNaN(lat) && !isNaN(lng)) {
        zoom = z;
        center = [lat, lng];
      }
    }
  }
  
  // Update title based on left layer
  document.title = `${leftInitial.name} - Raster Viewer`;
  
  const mapOptions = {
    center: center,
    zoom: zoom,
    minZoom: 0,
    maxZoom: 20,
    attributionControl: false,
    zoomControl: false
  };
  
  // Create maps with initial layers
  map1 = L.map('map-left', { ...mapOptions, layers: [leftInitial.layer] });
  map2 = L.map('map-right', { ...mapOptions, layers: [rightInitial.layer] });
  
  currentLeftLayer = leftInitial.layer;
  currentRightLayer = rightInitial.layer;
  
  // Add controls
  L.control.attribution({prefix: '', position: 'bottomleft'}).addTo(map1);
  L.control.attribution({prefix: '', position: 'bottomright'}).addTo(map2);
  L.control.scale({metric: true, imperial: false, position: 'bottomright'}).addTo(map2);
  L.control.zoom({ position: 'bottomright' }).addTo(map2);
  
  // Set initial layer attributions (controls now exist)
  updateMapAttribution(map1, currentLeftLayer._layerAttribution || '');
  updateMapAttribution(map2, currentRightLayer._layerAttribution || '');
  
  // Add layer controls (Leaflet's native control)
  L.control.layers(leftLayers, {}, { collapsed: true, position: 'topleft' }).addTo(map1);
  L.control.layers(rightLayers, {}, { collapsed: true, position: 'topright' }).addTo(map2);
  
  // Track layer changes and update URL
  map1.on('baselayerchange', (e) => {
    updateMapAttribution(map1, e.layer._layerAttribution || '');
    currentLeftLayer = e.layer;
    const params = getHashParams();
    params.set('left', e.layer._layerId || 'osm');
    updateHash(params);
    document.title = `${e.name} - Raster Viewer`;
  });
  
  map2.on('baselayerchange', (e) => {
    updateMapAttribution(map2, e.layer._layerAttribution || '');
    currentRightLayer = e.layer;
    const params = getHashParams();
    params.set('right', e.layer._layerId || 'osm');
    updateHash(params);
  });
  
  // Sync maps
  map1.sync(map2, {offsetFn: L.Sync.offsetHelper([1, 1], [0, 1])});
  map2.sync(map1, {offsetFn: L.Sync.offsetHelper([0, 1], [1, 1])});
  
  // Update hash on move
  map1.on('moveend', () => {
    const c = map1.getCenter();
    const z = map1.getZoom();
    const params = getHashParams();
    params.set('map', `${z.toFixed(1)}/${c.lat.toFixed(5)}/${c.lng.toFixed(5)}`);
    updateHash(params);
  });
  
  // Setup geocoder
  setupGeocoder();
}

// Geocoder Control
const GeocoderControl = L.Control.extend({
  options: { position: 'topleft' },
  
  onAdd: function(map) {
    const container = L.DomUtil.create('div', 'leaflet-control-geocoder leaflet-bar');
    
    // Toggle button with magnifying glass
    const toggle = L.DomUtil.create('a', 'leaflet-control-geocoder-toggle', container);
    toggle.href = '#';
    toggle.title = 'Search';
    
    // Search wrapper (hidden by default)
    const searchWrapper = L.DomUtil.create('div', 'leaflet-control-geocoder-form auto-search-wrapper', container);
    const input = L.DomUtil.create('input', '', searchWrapper);
    input.type = 'text';
    input.autocomplete = 'off';
    input.id = 'search';
    input.placeholder = 'Search location...';
    
    const collapse = () => {
      container.classList.remove('leaflet-control-geocoder-expanded');
    };
    
    // Toggle expand/collapse
    L.DomEvent.on(toggle, 'click', function(e) {
      L.DomEvent.preventDefault(e);
      container.classList.add('leaflet-control-geocoder-expanded');
      input.focus();
    });
    
    // Collapse on Escape or blur
    L.DomEvent.on(input, 'keydown', function(e) {
      if (e.key === 'Escape') {
        collapse();
        input.blur();
      }
    });
    
    L.DomEvent.on(input, 'blur', function() {
      // Delay to allow click on autocomplete results
      setTimeout(collapse, 200);
    });
    
    L.DomEvent.disableClickPropagation(container);
    L.DomEvent.disableScrollPropagation(container);
    
    return container;
  }
});

function setupGeocoder() {
  // Add geocoder control to map1
  new GeocoderControl({ position: 'topleft' }).addTo(map1);
  
  new Autocomplete("search", {
    selectFirst: true,
    howManyCharacters: 3,
    
    onSearch: ({ currentValue }) => {
      const api = `https://nominatim.openstreetmap.org/search?format=geojson&limit=5&q=${encodeURIComponent(currentValue)}`;
      return new Promise((resolve) => {
        fetch(api)
          .then((response) => response.json())
          .then((data) => resolve(data.features))
          .catch((error) => console.error(error));
      });
    },
    
    onResults: ({ currentValue, matches, template }) => {
      const regex = new RegExp(currentValue, "gi");
      return matches === 0
        ? template
        : matches.map((el) => `<li><p>${el.properties.display_name.replace(regex, (str) => `<b>${str}</b>`)}</p></li>`).join("");
    },
    
    onSubmit: ({ object }) => {
      if (geocoderMarker1) map1.removeLayer(geocoderMarker1);
      if (geocoderMarker2) map2.removeLayer(geocoderMarker2);
      
      const { display_name } = object.properties;
      const [lng, lat] = object.geometry.coordinates;
      
      const markerOptions = {
        icon: L.icon({
          iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
          iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
          shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
          iconSize: [25, 41],
          iconAnchor: [12, 41],
          popupAnchor: [1, -34],
          shadowSize: [41, 41]
        })
      };
      
      geocoderMarker1 = L.marker([lat, lng], markerOptions);
      geocoderMarker2 = L.marker([lat, lng], markerOptions);
      
      geocoderMarker1.addTo(map1).bindPopup(display_name, { autoPan: false });
      geocoderMarker2.addTo(map2).bindPopup(display_name, { autoPan: false });
      
      // Sync popup open/close between markers (with flag to prevent recursion)
      let syncing = false;
      geocoderMarker1.on('popupopen', () => { if (!syncing) { syncing = true; geocoderMarker2.openPopup(); syncing = false; } });
      geocoderMarker1.on('popupclose', () => { if (!syncing) { syncing = true; geocoderMarker2.closePopup(); syncing = false; } });
      geocoderMarker2.on('popupopen', () => { if (!syncing) { syncing = true; geocoderMarker1.openPopup(); syncing = false; } });
      geocoderMarker2.on('popupclose', () => { if (!syncing) { syncing = true; geocoderMarker1.closePopup(); syncing = false; } });
      
      geocoderMarker1.openPopup();
      map1.setView([lat, lng], 14);
    },
    
    noResults: ({ currentValue, template }) => template(`<li>No results found: "${currentValue}"</li>`)
  });
}

init();
