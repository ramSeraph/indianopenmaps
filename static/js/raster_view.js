// Default options we'll use for both maps. Se the center of the map,
// default zoom levels, and other Leaflet map options here.
// some of this html/js was copied from https://kokoalberti.com/articles/georeferencing-and-digitizing-old-maps-with-gdal/ and https://server.nikhilvj.co.in/pmgsy

import { extendLeaflet } from 'https://esm.sh/@india-boundary-corrector/leaflet-layer@latest';

const currUrl = window.location.href;

function getTileJSON(cb) {
  const tileJsonUrl = new URL('./tiles.json', currUrl).href;
  fetch(tileJsonUrl)
  .then(response => {
    if (!response.ok) {
      throw new Error('Network response was not ok');
    }
    return response.json();
  })
  .then(data => {
    if (data['name']) {
      document.title = data['name'];
    }
    cb(data);
  })
  .catch(error => {
    console.error('Title fetch error:', error);
  });

}
getTileJSON(addLayers);

function addLayers(tileJSON) {
  // Extend Leaflet with India boundary corrector
  extendLeaflet(L);
  
  var OSM =  L.tileLayer.indiaBoundaryCorrected('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap</a> contributors'
  });
  var OTM = L.tileLayer.indiaBoundaryCorrected('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
    maxZoom: 17,
    attribution: 'Map data: {attribution.OpenStreetMap}, <a href="http://viewfinderpanoramas.org">SRTM</a> | Map style: &copy; <a href="https://opentopomap.org">OpenTopoMap</a> (<a href="https://creativecommons.org/licenses/by-sa/3.0/">CC-BY-SA</a>)'
  });
  var gStreets = L.tileLayer('https://{s}.google.com/vt/lyrs=m&x={x}&y={y}&z={z}', {
    maxZoom: 20,
    subdomains: ['mt0','mt1','mt2','mt3'],
    attribution: 'Map data &copy; 2022 Google'
  });
  var gHybrid = L.tileLayer('https://{s}.google.com/vt/lyrs=s,h&x={x}&y={y}&z={z}', {
    maxZoom: 20,
    subdomains:['mt0','mt1','mt2','mt3'],
    attribution: 'Map data &copy; 2022 Google, Imagery &copy; 2022 TerraMetrics'
  });
  var esriWorld = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}', {
    attribution: 'Tiles &copy; Esri &mdash; Source: Esri, i-cubed, USDA, USGS, AEX, GeoEye, Getmapping, Aerogrid, IGN, IGP, UPR-EGP, and the GIS User Community',
    maxNativeZoom:18,
    maxZoom: 20
  });

  var layerOptions = {
    maxNativeZoom: tileJSON['maxzoom'],
    minZoom: tileJSON['minzoom'],
    attribution: tileJSON['attribution'],
  };

  var tileUrl = tileJSON['tiles'][0];


  var main1 = L.tileLayer(tileUrl, layerOptions);
  var main2 = L.tileLayer(tileUrl, layerOptions);

  var baseLayers = {
    "OpenStreetMap.org" : OSM,
    "OpenTopoMap" : OTM,
    "ESRI Satellite": esriWorld,
    "gStreets": gStreets,
    "gHybrid": gHybrid,
    "main": main2
  };

  const b = tileJSON['bounds'];
  var defaultCenter = tileJSON['center'] ? [tileJSON['center'][1], tileJSON['center'][0]] : [(b[1] + b[3])/2, (b[0] + b[2])/2];
  var defaultZoom = tileJSON['center'] && tileJSON['center'][2] ? tileJSON['center'][2] : 10;

  // Parse URL hash for initial position
  var center = defaultCenter;
  var zoom = defaultZoom;
  
  if (window.location.hash) {
    const hash = window.location.hash.substring(1);
    const parts = hash.split('/');
    if (parts.length >= 3) {
      const hashZoom = parseFloat(parts[0]);
      const hashLat = parseFloat(parts[1]);
      const hashLng = parseFloat(parts[2]);
      if (!isNaN(hashZoom) && !isNaN(hashLat) && !isNaN(hashLng)) {
        zoom = hashZoom;
        center = [hashLat, hashLng];
      }
    }
  }

  var options = {
    center: center,
    zoom: zoom,
    minZoom: 0,
    maxZoom: 20,
    attributionControl: false
  };
  options['zoomControl'] = false;

  // Create the left and the right map in their respective containers
  var map1 = L.map('map-left', options);
  L.control.attribution({prefix: '', position: 'bottomleft'}).addTo(map1)
  main1.addTo(map1);

  options['layers'] = [OSM];
  var map2 = L.map('map-right', options);
  L.control.layers(baseLayers, {}, {collapsed: true, autoZIndex:false}).addTo(map2);
  L.control.attribution({prefix: '', position: 'bottomright'}).addTo(map2)
  L.control.scale({metric:true, imperial:false, position: "bottomright"}).addTo(map2);
  L.control.zoom({ position: 'bottomright' }).addTo(map2);

  // Add Nominatim geocoder search control
  var geocoderMarker = null;
  var geocoder = L.Control.geocoder({
    defaultMarkGeocode: false,
    position: 'topleft',
    geocoder: new L.Control.Geocoder.Nominatim({
      geocodingQueryParams: {
        limit: 5
      }
    }),
    placeholder: 'Search using Nominatim..',
    errorMessage: 'No results found',
    showResultIcons: true,
    suggestMinLength: 3,
    suggestTimeout: 250
  })
    .on('markgeocode', function (e) {
      if (geocoderMarker) {
        map1.removeLayer(geocoderMarker);
      }
      var latlng = e.geocode.center;
      map1.setView(latlng, 14);
      geocoderMarker = L.marker(latlng, { icon: L.icon({
        iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
        iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
        shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
        iconSize: [25, 41],
        iconAnchor: [12, 41],
        popupAnchor: [1, -34],
        shadowSize: [41, 41]
      })})
        .addTo(map1)
        .bindPopup(e.geocode.name)
        .openPopup();
    })
    .addTo(map1);

  // Use the Leaflet Sync extension to sync the right bottom corner
  // of the left map to the left bottom corner of the right map, and
  // vice versa.
  map1.sync(map2, {offsetFn: L.Sync.offsetHelper([1, 1], [0, 1])});
  map2.sync(map1, {offsetFn: L.Sync.offsetHelper([0, 1], [1, 1])});

  // Update URL hash when map moves
  function updateHash() {
    const center = map1.getCenter();
    const zoom = map1.getZoom();
    const hash = `#${zoom.toFixed(1)}/${center.lat.toFixed(5)}/${center.lng.toFixed(5)}`;
    window.history.replaceState(null, null, hash);
  }

  // Update hash on move end
  map1.on('moveend', updateHash);
  
  // Initial hash update
  updateHash();
}

