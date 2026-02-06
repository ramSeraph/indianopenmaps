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

function renderLayer(layerId, color = null) {
  const style = color ? ` style="color: ${color}; border-left: 3px solid ${color}; padding-left: 5px;"` : '';
  return '<div class="maplibregl-ctrl-inspect_layer"' + style + '>' + layerId + '</div>';
}

function renderProperties(feature, layerColor = null) {
  var sourceProperty = renderLayer(feature.layer['source-layer'] || feature.layer.source, layerColor);
  var idProperty = renderProperty('$id', feature.id);
  var typeProperty = renderProperty('$type', feature.geometry.type);
  var properties = Object.keys(feature.properties).map(function (propertyName) {
    return renderProperty(propertyName, feature.properties[propertyName]);
  });
  return (feature.id ? [sourceProperty, idProperty, typeProperty]
    : [sourceProperty, typeProperty]).concat(properties).join('');
}

function renderFeatures(features, getLayerColor) {
  return features.map(function (ft) {
    const color = getLayerColor ? getLayerColor(ft) : null;
    return '<div class="maplibregl-ctrl-inspect_feature">' + renderProperties(ft, color) + '</div>';
  }).join('');
}

function renderCoordinates(lngLat) {
  return `<div class="maplibregl-ctrl-inspect_layer">Coords: ${(lngLat.lng).toFixed(5)},${(lngLat.lat).toFixed(5)}</div>`;
}

function renderPopup(features, lngLat, getLayerColor) {
  return '<div class="maplibregl-ctrl-inspect_popup">' + renderCoordinates(lngLat) + renderFeatures(features, getLayerColor) + '</div>';
}

export class PopupHandler {
  constructor(map, layers, routesHandler, vectorSourceHandler) {
    this.map = map;
    this.layers = layers;
    this.routesHandler = routesHandler;
    this.vectorSourceHandler = vectorSourceHandler;
    this.popup = new maplibregl.Popup({
      closeButton: true,
      closeOnClick: true
    });
    this.highlightedFeatures = [];
  }

  clearHighlight() {
    for (const { source, sourceLayer, id } of this.highlightedFeatures) {
      this.map.setFeatureState(
        { source, sourceLayer, id },
        { highlight: false }
      );
    }
    this.highlightedFeatures = [];
  }

  highlightFeatures(features) {
    this.clearHighlight();
    
    const routes = this.routesHandler.getVectorSources();
    
    for (const feature of features) {
      const sourceId = feature.source;
      
      // Find the source path from vectorSourceHandler
      let sourcePath = null;
      for (const [path, _] of this.vectorSourceHandler.selectedSources) {
        if (this.vectorSourceHandler.getSourceName(path) === sourceId) {
          sourcePath = path;
          break;
        }
      }
      
      if (!sourcePath || !routes[sourcePath]) continue;
      
      const promoteId = routes[sourcePath].promoteid;
      if (!promoteId) continue;
      
      // Get the feature id from the promoteId property
      const featureId = feature.properties[promoteId];
      if (featureId === undefined || featureId === null) continue;
      
      const sourceLayer = feature.sourceLayer || feature.layer['source-layer'];
      if (!sourceLayer) continue;
      
      this.map.setFeatureState(
        { source: sourceId, sourceLayer, id: featureId },
        { highlight: true }
      );
      
      this.highlightedFeatures.push({ source: sourceId, sourceLayer, id: featureId });
    }
  }

  getLayerColor(feature) {
    const sourceId = feature.source;
    for (const [path, _] of this.vectorSourceHandler.selectedSources) {
      if (this.vectorSourceHandler.getSourceName(path) === sourceId) {
        return this.vectorSourceHandler.getColorForPath(path);
      }
    }
    return null;
  }

  queryFeatures(e) {
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

    return this.map.queryRenderedFeatures(queryBox, {
      layers: this.layers.polygons.concat(this.layers.lines.concat(this.layers.pts))
    }) || [];
  }

  handleMouseMove(e) {
    var features = this.queryFeatures(e);
    this.map.getCanvas().style.cursor = (features.length) ? 'pointer' : '';

    if (!features.length) {
      this.clearHighlight();
    } else {
      this.highlightFeatures(features);
    }
  }

  handleClick(e) {
    var features = this.queryFeatures(e);

    if (!features.length) {
      this.popup.remove();
    } else {
      this.popup.setLngLat(e.lngLat)
        .setHTML(renderPopup(features, e.lngLat, (ft) => this.getLayerColor(ft)))
        .addTo(this.map);
    }
  }
}


