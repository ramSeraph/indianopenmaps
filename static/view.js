
const currUrl = window.location.href;
const baseUrl = currUrl.replace(/\/view.*$/, '');
const tileJsonUrl = baseUrl + '/tiles.json';
const srcName = 'source-to-view';

var layers = {
  pts: [],
  lines: [],
  polygons: []
}

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

var Esri_WorldImagery = {
  'name': 'ESRI World Imagery',
  'source-id': 'esri-world-imagery',
  'layer-id': 'esri-world-imagery-layer',
  'initial': false,
  'tiles': [ 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}' ],
  'minZoom': 0,
  'maxZoom': 18,
  'attribution': 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community'
};

var Carto_Dark = {
  'name': 'Carto OSM Dark',
  'source-id': 'carto-dark',
  'layer-id': 'carto-dark-layer',
  'initial': false,
  'tiles': [
    "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
    "https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
    "https://c.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
    "https://d.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png"
  ],
  'minZoom': 0,
  'maxZoom': 20,
  'attribution': '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
};

var Stadia_AlidadeSmoothDark = {
  'name': 'Stadia OSM Dark',
  'source-id': 'stadia-dark',
  'layer-id': 'stadia-dark-layer',
  'initial': true,
  'tiles': [ 'https://tiles.stadiamaps.com/tiles/alidade_smooth_dark/{z}/{x}/{y}@2x.png' ], 
  'minZoom': 0,
  'maxZoom': 20,
  'attribution': '&copy; <a href="https://www.stadiamaps.com/" target="_blank">Stadia Maps</a> &copy; <a href="https://openmaptiles.org/" target="_blank">OpenMapTiles</a> &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
};

var baseLayers = [
  //Stadia_AlidadeSmoothDark,
  Carto_Dark,
  Esri_WorldImagery
];

function randomColor(colors) {
  var randomNumber = parseInt(Math.random() * colors.length);
  return colors[randomNumber];
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
  mapConfig.style.sources[layerInfo['source-id']] = {
    'type': 'raster',
    'tiles': layerInfo.tiles,
    'attribution': layerInfo.attribution,
  };
  mapConfig.style.layers.push({
    'id': layerInfo['layer-id'],
    'source': layerInfo['source-id'],
    'type': 'raster',
    'minzoom': layerInfo.minZoom,
    'maxzoom': layerInfo.maxZoom,
  });
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
      typeof value === 'string') return value.toString();
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

function renderPopup(features) {
  return '<div class="maplibregl-ctrl-inspect_popup">' + renderFeatures(features) + '</div>';
}

var popup = new maplibregl.Popup({
  closeButton: false,
  closeOnClick: false
});

var wantPopup = false;

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
      .setHTML(renderPopup(features))
      .addTo(map);
  }
}

function enablePopup(inspect) {
  wantPopup = inspect;
}

class InspectButton {
  constructor(inspect) {
    this.inspect = inspect;
    enablePopup(this.inpect);
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

  getLayer(label) {
    const layerInfo = this.getLayerInfo(label);
    const layerId = layerInfo['layer-id'];
    const layer = this.map.getLayer(layerId);
    if (layer === undefined) return null;
    return layer;
  }

  getSource(label) {
    const layerInfo = this.getLayerInfo(label);
    const sourceId = layerInfo['source-id'];
    const src = this.map.getSource(sourceId);
    if (src === undefined) return null;
    return src;
  }

  updateButtons() {
	this.buttons.forEach((button) => {
      button.classList.remove('-active');
      let label = button.title;
      const layer = this.getLayer(label);
      if (layer !== null) {
        button.classList.add('-active');
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
          const layer = this.getLayer(b.title);
          if (layer === null) {
            return;
          }
          this.map.removeLayer(layer.id);
          const src = this.getSource(b.title);
          if (src === null) {
            return;
          }
          this.map.removeSource(src.id);
        });
        const layerInfo = this.getLayerInfo(button.title);
        this.map.addSource(layerInfo['source-id'], {
          'type': 'raster',
          'tiles': layerInfo.tiles,
          'attribution': layerInfo.attribution,
        });
        this.map.addLayer({
          'id': layerInfo['layer-id'],
          'source': layerInfo['source-id'],
          'minzoom': layerInfo.minZoom,
          'maxzoom': layerInfo.maxZoom,
          'type': 'raster',
        }, firstLayerId);
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

document.addEventListener("DOMContentLoaded", (event) => {
  map = new maplibregl.Map(mapConfig);
  map.addControl(new maplibregl.FullscreenControl());
  map.addControl(new maplibregl.NavigationControl());
  map.addControl(new maplibregl.GeolocateControl({
    positionOptions: { enableHighAccuracy: true },
    trackUserLocation: true
  }));
  map.addControl(new InspectButton(false));
  map.addControl(new BaseLayerPicker(baseLayers), 'top-left');
  map.once('load', function () {
    map.addSource(srcName, {
      'type': 'vector',
      'url': tileJsonUrl,
    });
  });
  map.on('sourcedata', addLayers);
  map.on('mousemove', showPopup);
});

