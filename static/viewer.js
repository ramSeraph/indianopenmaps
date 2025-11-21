const currUrl = window.location.href;

// Parse URL parameters
const params = new Proxy(new URLSearchParams(window.location.search), {
  get: (searchParams, prop) => searchParams.get(prop),
});

const markerLat = params.markerLat ? parseFloat(params.markerLat) : null;
const markerLon = params.markerLon ? parseFloat(params.markerLon) : null;
const initialSourcePath = params.source || null;

const srcName = 'source-to-view';

let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);
var boundary_pmtiles_url = 'https://raw.githubusercontent.com/ramSeraph/indianopenmaps/main/india_boundary_correcter/osm_corrections.pmtiles';

var layers = {
  pts: [],
  lines: [],
  polygons: []
};

var lightColors = [
  'FC49A3', 'CC66FF', '66CCFF', '66FFCC', '00FF00', 'FFCC66',
  'FF6666', 'FF0000', 'FF8000', 'FFFF66', '00FFFF'
];

var soiColors = [
  'C71585', '663399', '4682B4', '7B68EE', '228B22', 'DAA520',
  'D2691E', 'B22222', 'FF8C00', 'DAA520', '003366',
];

const ESRI_WORLD_IMAGERY_LAYER_NAME = 'ESRI World Imagery';
const CARTO_OSM_DARK_LAYER_NAME = 'Carto OSM Dark';

var Esri_WorldImagery = {
  'name': ESRI_WORLD_IMAGERY_LAYER_NAME,
  'sources': {
    'esri-world-imagery': {
      'type': 'raster',
      'tiles': [ 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}' ],
      'attribution': 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
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

var Carto_Dark = {
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
      'attribution': '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
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
      'url': `pmtiles://${boundary_pmtiles_url}`,
      'layers': [
        {
          'id': 'to-add',
          'source-layer': 'to-add',
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
          'id': 'to-del',
          'source-layer': 'to-del',
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

var baseLayers = [
  Carto_Dark,
  Esri_WorldImagery,
];

function randomColor(colors) {
  var randomNumber = parseInt(Math.random() * colors.length);
  return colors[randomNumber];
}

function getLayersAndSources(layerInfo) {
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

const INDIA_CENTER = [76.5,22.5];
const INDIA_ZOOM = 4;

const hashParams = new URLSearchParams(window.location.hash.substring(1));
const initialBaseLayerName = hashParams.get('base') || CARTO_OSM_DARK_LAYER_NAME;
const initialTerrainSetting = hashParams.get('terrain') || 'false';
const initialVectorSource = hashParams.get('source') || null;

let mapConfig = {    
  'container': 'map',
  'hash': 'map',
  'style': {
    'version': 8,
    'sources': {},
    'layers': [],
  },
  'center': markerLat && markerLon ? [markerLon, markerLat] : INDIA_CENTER,
  'zoom': markerLat && markerLon ? 14 : INDIA_ZOOM,
  'maxZoom': 30,
};

const terrainTileUrl = decodeURI(new URL('/dem/terrain-rgb/cartodem-v3r1/bhuvan/{z}/{x}/{y}.webp', currUrl).href);

const terrainSource = {
  'type': 'raster-dem',
  'tiles': [terrainTileUrl],
  'tileSize': 256,
  'maxzoom': 12,
  'minzoom': 5,
  'attribution': 'Terrain: <a href="https://bhuvan-app3.nrsc.gov.in/data/download/index.php" target="_blank">CartoDEM 30m v3r1</a>'
}

const HILLSHADE_LAYER_ID = 'hills';
const hlayer = {
  id: HILLSHADE_LAYER_ID,
  type: 'hillshade',
  maxZoom: 14,
  source: 'hillshade-source',
  layout: {visibility: initialTerrainSetting === 'false' ? 'none' : 'visible'},
  paint: {'hillshade-shadow-color': '#473B24'}
}

function updateMapConfig(baseLayerName) {
  let baseLayerInfo = baseLayers.find(l => l.name === baseLayerName);

  // If layer not found yet (might be a raster layer loaded later), use default
  if (!baseLayerInfo) {
    console.log(`Layer ${baseLayerName} not found, using default`);
    baseLayerInfo = baseLayers[0]; // Use first available layer (Carto Dark)
  }

  const [sources, layers] = getLayersAndSources(baseLayerInfo);
  for (const [sname, source] of Object.entries(sources)) {
    mapConfig.style.sources[sname] = source;
  }
  mapConfig.style.sources['terrain-source'] = terrainSource;
  mapConfig.style.sources['hillshade-source'] = terrainSource;
  
  for (const layer of layers) {
    mapConfig.style.layers.push(layer);
  }
  mapConfig.style.layers.push(hlayer);
  mapConfig.style['sky'] = {}
}

function updateUrlHash(paramName, paramValue) {
    const currentHash = window.location.hash;
    const urlParams = new URLSearchParams(currentHash.substring(1));
    urlParams.set(paramName, paramValue);
    const newHash = '#' + urlParams.toString().replaceAll('%2F', '/');
    console.log(`Updating URL hash: ${newHash}`);
    window.location.hash = newHash;
}

updateMapConfig(initialBaseLayerName);

var map = null;
var vectorLayerIds = [];
var lightColorMapping = {};
var soiColorMapping = {};
var currentVectorSource = null;
var currentVectorSourceName = null;
var availableSources = {};
var allSources = [];
var selectedCategories = new Set();

function addLayers(e) {
  if (!map.getSource(srcName) || !map.isSourceLoaded(srcName) || !e.isSourceLoaded) {
    return;
  }
  map.off('sourcedata', addLayers);
  const src = map.getSource(srcName);
  vectorLayerIds = src.vectorLayerIds;
  
  for (const layerId of vectorLayerIds) {
    lightColorMapping[layerId] = '#' + randomColor(lightColors);
    soiColorMapping[layerId] = '#' + randomColor(soiColors);
  }

  const colorMapping = (initialBaseLayerName && initialBaseLayerName.includes('SOI')) ? soiColorMapping : lightColorMapping;

  for (const layerId of vectorLayerIds) {
    var layerColor = colorMapping[layerId];
    map.addLayer({
      'id': `${layerId}-polygons`,
      'type': 'fill',
      'source': `${srcName}`,
      'source-layer': `${layerId}`,
      'filter': ["==", "$type", "Polygon"],
      'layout': {},
      'paint': {
        'fill-opacity': 0.1,
        'fill-color': layerColor
      }
    });
    
    map.addLayer({
      'id': `${layerId}-polygons-outline`,
      'type': 'line',
      'source': `${srcName}`,
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

    map.addLayer({
      'id': `${layerId}-polygons-extrusions`,
      'type': 'fill-extrusion',
      'source': `${srcName}`,
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
   
    map.addLayer({
      'id': `${layerId}-lines`,
      'type': 'line',
      'source': `${srcName}`,
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
    
    map.addLayer({
      'id': `${layerId}-pts`,
      'type': 'circle',
      'source': `${srcName}`,
      'source-layer': `${layerId}`,
      'filter': ["==", "$type", "Point"],
      'paint': {
        'circle-color': layerColor,
        'circle-radius': 2.5,
        'circle-opacity': 0.75
      }
    });
    
    layers.polygons.push(`${layerId}-polygons`);
    layers.polygons.push(`${layerId}-polygons-outline`);
    layers.lines.push(`${layerId}-lines`);
    layers.pts.push(`${layerId}-pts`);
  }
}

function removeVectorLayers() {
  for (const layerId of vectorLayerIds) {
    const layersToRemove = [
      `${layerId}-polygons`,
      `${layerId}-polygons-outline`,
      `${layerId}-polygons-extrusions`,
      `${layerId}-lines`,
      `${layerId}-pts`
    ];
    for (const layer of layersToRemove) {
      if (map.getLayer(layer)) {
        map.removeLayer(layer);
      }
    }
  }
  layers.polygons = [];
  layers.lines = [];
  layers.pts = [];
  vectorLayerIds = [];
  lightColorMapping = {};
  soiColorMapping = {};
}

function switchVectorSource(sourceInfo) {
  if (currentVectorSource === sourceInfo.path) {
    return;
  }

  removeVectorLayers();
  
  if (map.getSource(srcName)) {
    map.removeSource(srcName);
  }

  const tileJsonUrl = new URL(`${sourceInfo.path}tiles.json`, window.location.origin).href;
  
  map.addSource(srcName, {
    'type': 'vector',
    'url': tileJsonUrl,
  });

  currentVectorSource = sourceInfo.path;
  currentVectorSourceName = sourceInfo.name;
  updateUrlHash('source', sourceInfo.path);
  updatePanelTitle();
  map.on('sourcedata', addLayers);
}

function updatePanelTitle() {
  const panelTitle = document.querySelector('.panel-header h3');
  if (panelTitle) {
    const panel = document.getElementById('source-panel');
    if (panel.classList.contains('collapsed') && currentVectorSourceName) {
      panelTitle.textContent = currentVectorSourceName;
    } else {
      panelTitle.textContent = 'Vector Sources';
    }
  }
}

function updateVectorColours(baseLayerName) {
  const colorMapping = (baseLayerName && baseLayerName.includes('SOI')) ? soiColorMapping : lightColorMapping;

  for (const layerId of vectorLayerIds) {
    const layerColor = colorMapping[layerId];
    if (map.getLayer(`${layerId}-polygons`)) {
      map.setPaintProperty(`${layerId}-polygons`, 'fill-color', layerColor);
    }
    if (map.getLayer(`${layerId}-polygons-outline`)) {
      map.setPaintProperty(`${layerId}-polygons-outline`, 'line-color', layerColor);
    }
    if (map.getLayer(`${layerId}-lines`)) {
      map.setPaintProperty(`${layerId}-lines`, 'line-color', layerColor);
    }
    if (map.getLayer(`${layerId}-pts`)) {
      map.setPaintProperty(`${layerId}-pts`, 'circle-color', layerColor);
    }
    if (map.getLayer(`${layerId}-polygons-extrusions`)) {
        map.setPaintProperty(`${layerId}-polygons-extrusions`, 'fill-extrusion-color', layerColor);
    }
  }
}
   

function displayValue(value,propName) {
  if (propName === '@timestamp'){
    return value.toString() + "<br>[ " + (new Date(value*1000)).toISOString() + " ]";
  }
  if (typeof value === 'undefined' || value === null) return value;
  if (typeof value === 'object' ||
      typeof value === 'number' ||
      typeof value === 'string') return value.toString()
                                             .replaceAll('&', '&amp;')
                                             .replaceAll('<', '&lt;')
                                             .replaceAll('>', '&gt;')
                                             .replaceAll("'", '&#39;')
                                             .replaceAll('"', '&quot;');
  return value;
}

function renderProperty(propertyName, property) {
  return '<div class="maplibregl-ctrl-inspect_property">' +
    '<div class="maplibregl-ctrl-inspect_property-name">' + propertyName + '</div>' +
    '<div class="maplibregl-ctrl-inspect_property-value">' + displayValue(property,propertyName) + '</div>' +
    '</div>';
}

function renderLayer(layerId) {
  return '<div class="maplibregl-ctrl-inspect_layer">' + layerId + '</div>';
}

function renderProperties(feature) {
  var sourceProperty = renderLayer(feature.layer['source-layer'] || feature.layer.source);
  var idProperty = renderProperty('$id', feature.id);
  var typeProperty = renderProperty('$type', feature.geometry.type);
  var properties = Object.keys(feature.properties).map(function (propertyName) {
    return renderProperty(propertyName, feature.properties[propertyName]);
  });
  return (feature.id ? [sourceProperty, idProperty, typeProperty]
    : [sourceProperty, typeProperty]).concat(properties).join('');
}

function renderFeatures(features) {
  return features.map(function (ft) {
    return '<div class="maplibregl-ctrl-inspect_feature">' + renderProperties(ft) + '</div>';
  }).join('');
}

function renderCoordinates(lngLat) {
  return `<div class="maplibregl-ctrl-inspect_layer">Coords: ${(lngLat.lng).toFixed(5)},${(lngLat.lat).toFixed(5)}</div>`;
}

function renderPopup(features, lngLat) {
  return '<div class="maplibregl-ctrl-inspect_popup">' + renderCoordinates(lngLat) + renderFeatures(features) + '</div>';
}

var popup = new maplibregl.Popup({
  closeButton: false,
  closeOnClick: false
});

var initialInspect = true;

var wantPopup = initialInspect;

function showPopup(e) {
  var selectThreshold = 3;
  var queryBox = [
    [
      e.point.x - selectThreshold,
      e.point.y + selectThreshold
    ],
    [
      e.point.x + selectThreshold,
      e.point.y - selectThreshold
    ]
  ];

  var features = map.queryRenderedFeatures(queryBox, {
    layers: layers.polygons.concat(layers.lines.concat(layers.pts))
  }) || [];
  map.getCanvas().style.cursor = (features.length) ? 'pointer' : '';

  if (!features.length || !wantPopup) {
    popup.remove();
  } else {
    popup.setLngLat(e.lngLat)
      .setHTML(renderPopup(features, e.lngLat))
      .addTo(map);
  }
}

function enablePopup(inspect) {
  wantPopup = inspect;
}

class InspectButton {
  constructor(inspect) {
    this.inspect = inspect;
  }

  getClass() {
    if (this.inspect) {
      return 'maplibregl-ctrl-icon maplibregl-ctrl-map';
    } else {
      return 'maplibregl-ctrl-icon maplibregl-ctrl-inspect';
    }
  }

  toggle() {
    this.inspect = !this.inspect;
    var btn = document.querySelector('#show-popup');
    btn.className = this.getClass();
    enablePopup(this.inspect);
  }

  onAdd(map) {
    const div = document.createElement("div");
    div.className = "maplibregl-ctrl maplibregl-ctrl-group";
    const classStr = this.getClass();
    div.innerHTML = `<button id='show-popup' class='${classStr}'></button>`;
    div.addEventListener("contextmenu", (e) => e.preventDefault());
    div.addEventListener("click", () => this.toggle());
    return div;
  }
}

class BaseLayerPicker {

  constructor(baseLayers) {
    this.baseLayers = baseLayers;
    this.map = null;
    this.container = null;
    this.select = null;
    this.currentLayerName = null;
  }

  getLayerInfo(label) {
    for (let layerInfo of this.baseLayers) {
      if (layerInfo.name == label) {
        return layerInfo;
      }
    }
    return null;
  }

  async loadRasterSources() {
    try {
      const response = await fetch('/api/routes');
      const routes = await response.json();
      
      const rasterSources = [];
      for (const [path, info] of Object.entries(routes)) {
        if (info.type === 'raster') {
          const tileUrl = `${window.location.origin}${path}{z}/{x}/{y}.webp`;
          rasterSources.push({
            name: info.name,
            path: path,
            url: tileUrl,
            maxZoom: 15
          });
        }
      }
      
      return rasterSources;
    } catch (error) {
      console.error('Error loading raster sources:', error);
      return [];
    }
  }

  switchLayer(layerName) {
    if (this.currentLayerName === layerName) return;

    // Remove current layers
    if (this.currentLayerName) {
      const currentLayerInfo = this.getLayerInfo(this.currentLayerName);
      if (currentLayerInfo) {
        const [sources, layers] = getLayersAndSources(currentLayerInfo);
        for (const layer of layers) {
          if (this.map.getLayer(layer.id)) {
            this.map.removeLayer(layer.id);
          }
        }
        for (const [sname, source] of Object.entries(sources)) {
          if (this.map.getSource(sname)) {
            this.map.removeSource(sname);
          }
        }
      }
    }

    // Add new layers
    const newLayerInfo = this.getLayerInfo(layerName);
    if (newLayerInfo) {
      const [sources, layers] = getLayersAndSources(newLayerInfo);
      for (const [sname, source] of Object.entries(sources)) {
        this.map.addSource(sname, source);
      }
      for (const layer of layers) {
        this.map.addLayer(layer, HILLSHADE_LAYER_ID);
      }
    }

    this.currentLayerName = layerName;
    updateUrlHash('base', layerName);
    updateVectorColours(layerName);
  }

  async initialize() {
    const rasterSources = await this.loadRasterSources();
    
    // Add raster sources to baseLayers
    for (const raster of rasterSources) {
      const layerInfo = {
        name: raster.name,
        sources: {
          [`raster-${raster.path.replace(/\//g, '-')}`]: {
            type: 'raster',
            tiles: [raster.url],
            attribution: 'Survey of India',
            layers: [{
              id: `raster-layer-${raster.path.replace(/\//g, '-')}`,
              type: 'raster',
              minZoom: 0,
              maxZoom: raster.maxZoom
            }],
            maxZoom: raster.maxZoom
          }
        }
      };
      this.baseLayers.push(layerInfo);
    }

    // Populate dropdown
    this.baseLayers.forEach((layerInfo) => {
      const option = document.createElement('option');
      option.value = layerInfo.name;
      option.textContent = layerInfo.name;
      this.select.appendChild(option);
    });

    // Set initial selection and switch to it if needed
    this.select.value = initialBaseLayerName;
    
    // Check if we need to switch from the default layer
    const initialLayerInfo = this.getLayerInfo(initialBaseLayerName);
    if (initialLayerInfo && initialBaseLayerName !== this.currentLayerName) {
      this.switchLayer(initialBaseLayerName);
    } else {
      this.currentLayerName = initialBaseLayerName;
    }
  }

  onAdd(map) {
    this.map = map;
    const div = document.createElement("div");
    div.className = "maplibregl-ctrl maplibregl-ctrl-group";
    
    const select = document.createElement('select');
    select.style.padding = '5px 8px';
    select.style.fontSize = '12px';
    select.style.border = 'none';
    select.style.background = 'white';
    select.style.cursor = 'pointer';
    select.style.fontFamily = "'Open Sans', sans-serif";
    select.style.minWidth = 'auto';
    select.style.maxWidth = '300px';
    
    select.addEventListener('change', (e) => {
      this.switchLayer(e.target.value);
    });
    
    this.select = select;
    div.appendChild(select);
    
    // Initialize asynchronously
    this.initialize();
    
    return div;
  }
}

function loadAvailableSources() {
  fetch('/api/routes')
    .then(response => response.json())
    .then(data => {
      const sourcesByCategory = {};
      allSources = [];
      
      for (const [path, info] of Object.entries(data)) {
        const type = info.type || 'vector';
        if (type === 'raster') continue;
        
        const categories = Array.isArray(info.category) ? info.category : [info.category];
        const source = {
          name: info.name,
          path: path,
          url: info.url,
          categories: categories
        };
        
        allSources.push(source);
        
        for (const category of categories) {
          if (!sourcesByCategory[category]) {
            sourcesByCategory[category] = [];
          }
          sourcesByCategory[category].push(source);
        }
      }
      
      availableSources = sourcesByCategory;
      initializeCategoryFilters();
      renderSourcePanel();
    })
    .catch(error => {
      console.error('Error loading sources:', error);
    });
}

function initializeCategoryFilters() {
  const categories = Object.keys(availableSources).sort();
  const filtersEl = document.getElementById('categoryFilters');
  
  filtersEl.innerHTML = categories.map(category => 
    `<div class="category-filter" data-category="${category}">${category}</div>`
  ).join('');
  
  filtersEl.querySelectorAll('.category-filter').forEach(filter => {
    filter.addEventListener('click', () => {
      const category = filter.dataset.category;
      toggleCategoryFilter(category);
    });
  });
}

function toggleCategoryFilter(category) {
  if (selectedCategories.has(category)) {
    selectedCategories.delete(category);
  } else {
    selectedCategories.add(category);
  }
  
  document.querySelectorAll('.category-filter').forEach(filter => {
    if (filter.dataset.category === category) {
      filter.classList.toggle('active', selectedCategories.has(category));
    }
  });
  
  renderSourcePanel();
}

function filterSources() {
  const query = document.getElementById('searchInput').value.toLowerCase();
  let filtered = allSources;
  
  // Filter by selected categories (AND logic - source must have ALL selected categories)
  if (selectedCategories.size > 0) {
    filtered = filtered.filter(source => {
      // Check if source has ALL selected categories
      for (const selectedCat of selectedCategories) {
        if (!source.categories.includes(selectedCat)) {
          return false;
        }
      }
      return true;
    });
  }
  
  // Filter by search query
  if (query) {
    filtered = filtered.filter(source => 
      source.name.toLowerCase().includes(query) || 
      source.path.toLowerCase().includes(query) ||
      source.categories.some(cat => cat.toLowerCase().includes(query))
    );
  }
  
  return filtered;
}

function renderSourcePanel() {
  const sourceList = document.getElementById('source-list');
  const noResultsEl = document.getElementById('noResults');
  sourceList.innerHTML = '';
  
  const filtered = filterSources();
  
  if (filtered.length === 0) {
    noResultsEl.style.display = 'block';
    return;
  }
  
  noResultsEl.style.display = 'none';
  
  let sourceToSelect = null;
  
  filtered.forEach((source, index) => {
    const sourceOption = document.createElement('div');
    sourceOption.className = 'source-option';
    
    const radioId = `source-${index}`;
    const radio = document.createElement('input');
    radio.type = 'radio';
    radio.name = 'vector-source';
    radio.id = radioId;
    radio.value = source.path;
    
    const label = document.createElement('label');
    label.htmlFor = radioId;
    label.textContent = source.name;
    
    radio.addEventListener('change', () => {
      if (radio.checked) {
        switchVectorSource(source);
      }
    });
    
    sourceOption.appendChild(radio);
    sourceOption.appendChild(label);
    sourceList.appendChild(sourceOption);
    
    // Check if this is the source from URL hash or query parameter
    if ((initialVectorSource && source.path === initialVectorSource) ||
        (initialSourcePath && source.path === initialSourcePath)) {
      sourceToSelect = { radio, source, element: sourceOption };
    }
  });
  
  // Select source from URL hash/query param if available, otherwise select first source
  if (sourceToSelect) {
    sourceToSelect.radio.checked = true;
    switchVectorSource(sourceToSelect.source);
    
    // Scroll the selected source into view
    setTimeout(() => {
      sourceToSelect.element.scrollIntoView({ behavior: 'auto', block: 'center' });
    }, 100);
  } else if (filtered.length > 0 && !currentVectorSource) {
    const firstRadio = document.querySelector('input[name="vector-source"]');
    if (firstRadio) {
      firstRadio.checked = true;
      const firstSource = filtered[0];
      switchVectorSource(firstSource);
    }
  }
}

let baseLayerPicker = null;

document.addEventListener("DOMContentLoaded", (event) => {
  map = new maplibregl.Map(mapConfig);
  map.addControl(new maplibregl.FullscreenControl());
  map.addControl(new maplibregl.NavigationControl());

  map.addControl(new maplibregl.GeolocateControl({
    positionOptions: { enableHighAccuracy: true },
    trackUserLocation: true
  }));
  map.addControl(new InspectButton(initialInspect));

  baseLayerPicker = new BaseLayerPicker(baseLayers);
  map.addControl(baseLayerPicker, 'top-left');

  map.addControl(new maplibregl.TerrainControl({
    source: 'terrain-source',
    exaggeration: 1
  }));
  
  map.once('load', function () {
    if (initialTerrainSetting === 'false') {
      map.setTerrain(null);
    } else {
      map.setTerrain({ 'source': 'terrain-source', 'exaggeration': 1 });
    }
    
    loadAvailableSources();
  });
  
  map.on('mousemove', showPopup);
  map.on('terrain', (e) => {
    if (map.getTerrain()) {
      map.setLayoutProperty(HILLSHADE_LAYER_ID, 'visibility', 'visible');
      updateUrlHash('terrain', 'true');
    }
    else {
      map.setLayoutProperty(HILLSHADE_LAYER_ID, 'visibility', 'none');
      updateUrlHash('terrain', 'false');
    }
  });

  // Add marker if coordinates provided
  if (markerLat !== null && markerLon !== null) {
    const marker = new maplibregl.Marker({ color: '#DBDBDB', draggable: false })
                                 .setLngLat([markerLon, markerLat])
                                 .addTo(map);
  }
  
  // Add search input listener
  document.getElementById('searchInput').addEventListener('input', renderSourcePanel);
  
  // Add collapse/expand functionality for main panel
  document.getElementById('panelHeader').addEventListener('click', () => {
    const panel = document.getElementById('source-panel');
    panel.classList.toggle('collapsed');
    updatePanelTitle();
  });
  
  // Add collapse/expand functionality for filter section
  document.getElementById('filterTitle').addEventListener('click', () => {
    const filterSection = document.getElementById('filterSection');
    filterSection.classList.toggle('collapsed');
  });
});
