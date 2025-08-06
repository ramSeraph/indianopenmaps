const currUrl = window.location.href;


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
  'FC49A3', // pink
  'CC66FF', // purple-ish
  '66CCFF', // sky blue
  '66FFCC', // teal
  '00FF00', // lime green
  'FFCC66', // light orange
  'FF6666', // salmon
  'FF0000', // red
  'FF8000', // orange
  'FFFF66', // yellow
  '00FFFF'  // turquoise
];

var soiColors = [
  'd62728', // red
  'ff7f0e', // orange
  '2ca02c', // green
  '1f77b4', // blue
  '9467bd', // purple
  '8c564b', // brown
  'e377c2', // pink
  '7f7f7f', // gray
  'bcbd22', // yellow-green
  '17becf'  // cyan
];

const soiTileUrl = decodeURI(new URL('/soi/osm/{z}/{x}/{y}.webp', currUrl).href);
// TODO: pick different colors for layers for SOI basemap to make things more visible
const SOI_OSM_IMAGERY_LAYER_NAME = 'SOI OSM Imagery';
const ESRI_WORLD_IMAGERY_LAYER_NAME = 'ESRI World Imagery';
const CARTO_OSM_DARK_LAYER_NAME = 'Carto OSM Dark';

var SOI_OSM_Imagery = {
  'name': SOI_OSM_IMAGERY_LAYER_NAME,
  'sources': {
    'soi-osm-imagery': {
      'type': 'raster',
      'tiles': [ soiTileUrl ],
      'attribution': 'Tiles &copy; Survey of India &mdash; Source: <a href="https://onlinemaps.surveyofindia.gov.in/">1:50000 Open Series Maps</a>',
      'layers': [
        {
          'id': 'soi-osm-layer',
          'type': 'raster',
          'minZoom': 0,
          'maxZoom': 14,
        }
      ],
      'maxZoom': 14,
    }
  }
};

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
  SOI_OSM_Imagery,
];

function randomColor(colors) {
  var randomNumber = parseInt(Math.random() * colors.length);
  return colors[randomNumber];
}

function getLayersAndSources(layerInfo) {
  // TODO: why is the jsonification needed?
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

const params = new URLSearchParams(window.location.hash.substring(1));
const initialBaseLayerName = params.get('base') || CARTO_OSM_DARK_LAYER_NAME;

let mapConfig = {    
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


// using tile url as the tilejson setup is causing more rendering artifacts
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
  layout: {visibility: 'visible'},
  paint: {'hillshade-shadow-color': '#473B24'}
}

function updateMapConfig(baseLayerName) {
  let baseLayerInfo = baseLayers.find(l => l.name === baseLayerName);

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
  mapConfig.style['terrain'] = { 'source': 'terrain-source', 'exaggeration': 1 };
  mapConfig.style['sky'] = {}

}

function updateUrlHash(paramName, paramValue) {
    const currentHash = window.location.hash;
    const urlParams = new URLSearchParams(currentHash.substring(1));
    urlParams.set(paramName, paramValue);
    const newHash = '#' + urlParams.toString();
    console.log(`Updating URL hash: ${newHash}`);
    window.location.hash = newHash;
}

let selectedBaseLayerName = initialBaseLayerName;
updateMapConfig(initialBaseLayerName);

updateUrlHash('base', initialBaseLayerName);

var map = null;
var vectorLayerIds = [];
var lightColorMapping = {};
var soiColorMapping = {};

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

  const colorMapping = (initialBaseLayerName === SOI_OSM_IMAGERY_LAYER_NAME) ? soiColorMapping : lightColorMapping;

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

function updateVectorColours(baseLayerName) {
  const colorMapping = (baseLayerName === SOI_OSM_IMAGERY_LAYER_NAME) ? soiColorMapping : lightColorMapping;

  for (const layerId of vectorLayerIds) {
    const layerColor = colorMapping[layerId];
    if (this.map.getLayer(`${layerId}-polygons`)) {
      this.map.setPaintProperty(`${layerId}-polygons`, 'fill-color', layerColor);
    }
    if (this.map.getLayer(`${layerId}-polygons-outline`)) {
      this.map.setPaintProperty(`${layerId}-polygons-outline`, 'line-color', layerColor);
    }
    if (this.map.getLayer(`${layerId}-lines`)) {
      this.map.setPaintProperty(`${layerId}-lines`, 'line-color', layerColor);
    }
    if (this.map.getLayer(`${layerId}-pts`)) {
      this.map.setPaintProperty(`${layerId}-pts`, 'circle-color', layerColor);
    }
    if (this.map.getLayer(`${layerId}-polygons-extrusions`)) {
        this.map.setPaintProperty(`${layerId}-polygons-extrusions`, 'fill-extrusion-color', layerColor);
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
  // set a bbox around the pointer
  var selectThreshold = 3;
  var queryBox = [
    [
      e.point.x - selectThreshold,
      e.point.y + selectThreshold
    ], // bottom left (SW)
    [
      e.point.x + selectThreshold,
      e.point.y - selectThreshold
    ] // top right (NE)
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
    this.buttons = [];
  }

  getLayerInfo(label) {
    for (let layerInfo of this.baseLayers) {
      if (layerInfo.name == label) {
        return layerInfo;
      }
    }
    return null;
  }

  updateButtons() {
	  this.buttons.forEach((button) => {
      button.classList.remove('-active');
      let label = button.title;
      const layerInfo = this.getLayerInfo(button.title);
      const [sources, layers] = getLayersAndSources(layerInfo);
      for (const [sname, source] of Object.entries(sources)) {
        const src = this.map.getSource(sname);
        if (src) {
          button.classList.add('-active');
          break;
        }
      }
    });
  }

  expanded() {
    this.baseLayers.forEach((layerInfo) => {
      const button = document.createElement('button');
      button.type = 'button';
      button.title = layerInfo.name;
      button.textContent = layerInfo.name;
      button.addEventListener('click', () => {
		    if (button.classList.contains('-active')) return;

        // remove all layers
        this.buttons.forEach((b) => {
          const layerInfo = this.getLayerInfo(b.title);
          const [sources, layers] = getLayersAndSources(layerInfo);
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
        });

        const label = button.title;
        const newLayerInfo = this.getLayerInfo(label);
        const [sources, layers] = getLayersAndSources(newLayerInfo);
        for (const [sname, source] of Object.entries(sources)) {
          this.map.addSource(sname, source);
        }
        for (const layer of layers) {
          this.map.addLayer(layer, HILLSHADE_LAYER_ID);
        }

        updateUrlHash('base', label);
        updateVectorColours(label);
	    });
      this.buttons.push(button);
      this.container.appendChild(button);

    });

    this.map.on('styledata', () => { this.updateButtons(); });
  }

  onAdd(map) {
    this.map = map;
    const div = document.createElement("div");
    div.className = "maplibregl-ctrl maplibregl-ctrl-group maplibre-ctrl-styles-expanded";
    this.container = div;
    this.expanded();
	  return div;
  }
}

function setTitle() {
  const titleUrl = new URL('./title', currUrl).href;
  fetch(titleUrl)
  .then(response => {
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }
    return response.json();
  })
  .then(data => {
    document.title = data['title'];
  })
  .catch(error => {
    console.error('Title fetch error:', error);
  });
}

setTitle();

let baseLayerPicker = null;

document.addEventListener("DOMContentLoaded", (event) => {
  const tileJsonUrl = new URL('./tiles.json', currUrl).href;
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
    map.addSource(srcName, {
      'type': 'vector',
      'url': tileJsonUrl,
    });
  });
  map.on('sourcedata', addLayers);
  map.on('mousemove', showPopup);

  const params = new Proxy(new URLSearchParams(window.location.search), {
    get: (searchParams, prop) => searchParams.get(prop),
  });
  if ((params.markerLon !== undefined && params.markerLat !== undefined) &&
      (params.markerLon !== null && params.markerLat !== null))
  {
    const marker = new maplibregl.Marker({ color: '#DBDBDB',
                                           draggable: false })
                                 .setLngLat([params.markerLon, params.markerLat])
                                 .addTo(map);
  }
});
