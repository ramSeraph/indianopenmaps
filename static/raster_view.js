// Default options we'll use for both maps. Se the center of the map,
// default zoom levels, and other Leaflet map options here.
// some of this html/js was copied from https://kokoalberti.com/articles/georeferencing-and-digitizing-old-maps-with-gdal/ and https://server.nikhilvj.co.in/pmgsy

const currUrl = window.location.href;

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
    console.log(data);
    cb(data);
  })
  .catch(error => {
    console.error('Title fetch error:', error);
  });

}
setTitle();
getTileJSON(addLayers);

function addLayers(tileJSON) {
  var OSM =  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 19,
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap</a> contributors'
  });
  var OTM = L.tileLayer('https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png', {
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
  var center = [(b[0] + b[2])/2, (b[1] + b[3])/2];

  var options = {
    center: center,
    zoom: 10,
    minZoom: 5,
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

  // Use the Leaflet Sync extension to sync the right bottom corner
  // of the left map to the left bottom corner of the right map, and
  // vice versa.
  map1.sync(map2, {offsetFn: L.Sync.offsetHelper([1, 1], [0, 1])});
  map2.sync(map1, {offsetFn: L.Sync.offsetHelper([0, 1], [1, 1])});

}

