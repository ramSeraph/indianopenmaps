
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

const soiTileUrl = decodeURI(new URL('/soi/osm/{z}/{x}/{y}.webp', currUrl).href);
console.log(soiTileUrl);
// TODO: pick different colors for layers for SOI basemap to make things more visible
var SOI_OSM_Imagery = {
  'name': 'SOI OSM Imagery',
  'initial': false,
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
  'name': 'ESRI World Imagery',
  'initial': false,
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
  'name': 'Carto OSM Dark',
  'initial': true,
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

let mapConfig = {    
  'container': 'map',
  'hash': true,
  'style': {
    'version': 8,
    'sources': {},
    'layers': [],
  },
  'center': INDIA_CENTER,
  'zoom': INDIA_ZOOM,
  'maxZoom': 30,
};

baseLayers.forEach((layerInfo) => {
  if (!layerInfo.initial) {
    return;
  }
  const [sources, layers] = getLayersAndSources(layerInfo);
  for (const [sname, source] of Object.entries(sources)) {
    mapConfig.style.sources[sname] = source;
  }
  for (const layer of layers) {
    mapConfig.style.layers.push(layer);
  }
});

var map = null;
var firstLayerId = null;

function addLayers(e) {
  if (!map.getSource(srcName) || !map.isSourceLoaded(srcName) || !e.isSourceLoaded) {
    return;
  }
  map.off('sourcedata', addLayers);
  const src = map.getSource(srcName);
  
  for (layerId of src.vectorLayerIds) {
    var layerColor = '#' + randomColor(lightColors);
    if (firstLayerId === null) {
      firstLayerId = `${layerId}-polygons`;
    }
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
        ["has", "Height"],
      ],
      'paint': {
        'fill-extrusion-color': layerColor,
        'fill-extrusion-height': ['get', 'Height'],
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

   
function displayValue(value,propName) {
  if (propName=== '@timestamp'){
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
        const layerInfo = this.getLayerInfo(button.title);
        const [sources, layers] = getLayersAndSources(layerInfo);
        for (const [sname, source] of Object.entries(sources)) {
          this.map.addSource(sname, source);
        }
        for (const layer of layers) {
          this.map.addLayer(layer, firstLayerId);
        }
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
  map.addControl(new BaseLayerPicker(baseLayers), 'top-left');
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
  if (params.markerLon !== undefined && params.markerLat !== undefined) {
    const marker = new maplibregl.Marker({ color: '#DBDBDB',
                                           draggable: false })
                                 .setLngLat([params.markerLon, params.markerLat])
                                 .addTo(map);
  }
});

