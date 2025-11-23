// Extracted InspectButton control and PopupHandler
import * as maplibregl from 'https://esm.sh/maplibre-gl@5.6.2';

function displayValue(value, propName) {
  if (propName === '@timestamp') {
    return value.toString() + "<br>[ " + (new Date(value * 1000)).toISOString() + " ]";
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
    '<div class="maplibregl-ctrl-inspect_property-value">' + displayValue(property, propertyName) + '</div>' +
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

export class PopupHandler {
  constructor(map, layers) {
    this.map = map;
    this.layers = layers;
    this.wantPopup = true;
    this.popup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false
    });
  }

  enable(enabled) {
    this.wantPopup = enabled;
  }

  showPopup(e) {
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

    var features = this.map.queryRenderedFeatures(queryBox, {
      layers: this.layers.polygons.concat(this.layers.lines.concat(this.layers.pts))
    }) || [];
    this.map.getCanvas().style.cursor = (features.length) ? 'pointer' : '';

    if (!features.length || !this.wantPopup) {
      this.popup.remove();
    } else {
      this.popup.setLngLat(e.lngLat)
        .setHTML(renderPopup(features, e.lngLat))
        .addTo(this.map);
    }
  }
}

export class InspectButton {
  constructor(initialInspect, onToggle) {
    this.inspect = initialInspect;
    this.onToggle = onToggle;
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
    if (this.onToggle) {
      this.onToggle(this.inspect);
    }
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
