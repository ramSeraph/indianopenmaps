import { TileType } from 'pmtiles';

export function getMimeType(t) {
  if (t == TileType.Png) {
    return "image/png";
  } else if (t == TileType.Jpeg) {
    return "image/jpeg";
  } else if (t == TileType.Webp) {
    return "image/webp";
  } else if (t == TileType.Avif) {
    return "image/avif";
  } else if (t == TileType.Mvt) {
    return "application/vnd.mapbox-vector-tile";
  }
  throw Error(`Unknown tiletype ${t}`);
}

export function getExt(t) {
  if (t == TileType.Png) {
    return ".png";
  } else if (t == TileType.Jpeg) {
    return ".jpg";
  } else if (t == TileType.Webp) {
    return ".webp";
  } else if (t == TileType.Avif) {
    return ".avif";
  } else if (t == TileType.Mvt) {
    return ".pbf";
  }
  throw Error(`Unknown tiletype ${t}`);
}

export function extendAttribution(attribution, datameetAttribution) {
  if (!datameetAttribution) {
    return attribution;
  }
  return attribution + ' - ' + 'Collected by <a href="https://datameet.org" target="_blank" rel="noopener noreferrer">Datameet Community</a>';
}
