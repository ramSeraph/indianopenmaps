const pmtiles = require('pmtiles');

function getMimeType(t) {
  if (t == pmtiles.TileType.Png) {
    return "image/png";
  } else if (t == pmtiles.TileType.Jpeg) {
    return "image/jpeg";
  } else if (t == pmtiles.TileType.Webp) {
    return "image/webp";
  } else if (t == pmtiles.TileType.Avif) {
    return "image/avif";
  } else if (t == pmtiles.TileType.Mvt) {
    return "application/vnd.mapbox-vector-tile";
  }
  throw Error(`Unknown tiletype ${t}`);
}
function getExt(t) {
  if (t == pmtiles.TileType.Png) {
    return ".png";
  } else if (t == pmtiles.TileType.Jpeg) {
    return ".jpg";
  } else if (t == pmtiles.TileType.Webp) {
    return ".webp";
  } else if (t == pmtiles.TileType.Avif) {
    return ".avif";
  } else if (t == pmtiles.TileType.Mvt) {
    return ".pbf";
  }
  throw Error(`Unknown tiletype ${t}`);
}

module.exports = {
  'getMimeType': getMimeType,
  'getExt': getExt,
}
