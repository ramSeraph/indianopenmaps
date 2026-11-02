import * as maplibregl from './maplibre.js';
import { tileCacheUrl } from './tile_cache.js';

export class TerrainHandler {
  constructor(searchParams) {
    this.searchParams = searchParams;
    this.HILLSHADE_LAYER_ID = 'hills';
    this.TERRAIN_SOURCE_ID = 'terrain-source';
    this.HILLSHADE_SOURCE_ID = 'hillshade-source';
    this.TERRAIN_EXAGGERATION = 1.5;
  }

  getControl() {
    return new maplibregl.TerrainControl({
      source: this.TERRAIN_SOURCE_ID,
      exaggeration: this.TERRAIN_EXAGGERATION
    });
  }

  initTerrain(map) {
    let initialTerrainSetting = this.searchParams.getTerrain('false');

    if (initialTerrainSetting === 'false') {
      map.setTerrain(null);
    } else {
      map.setTerrain({ 'source': this.TERRAIN_SOURCE_ID, 'exaggeration': this.TERRAIN_EXAGGERATION });
    }
  }

  terrainChangeCallback(map) {
    const terrain = map.getTerrain();

    map.setLayoutProperty(this.HILLSHADE_LAYER_ID, 'visibility', terrain ? 'visible' : 'none');

    this.searchParams.updateTerrain(!!terrain);
  }

  getElevation(map, lngLat) {
    const terrain = map.getTerrain();
    if (!terrain) return undefined;
    const exaggeration = terrain.exaggeration ?? 1;
    // queryTerrainElevation returns 0 both at sea level and when DEM data is
    // missing. MapLibre's DEM state distinguishes these without another fetch.
    const { elevation, demState } = map.terrain.getElevationAndDEMStateForLngLat(lngLat, Math.floor(map.getZoom()));
    return demState === 'loaded' && Number.isFinite(elevation) && exaggeration > 0
      ? elevation / exaggeration : null;
  }

  getTerrainTileUrl() {
    const currUrl = window.location.href;
    const terrainTileUrl = decodeURI(new URL('/dem/terrain-rgb/cartodem-v3r1/bhuvan/{z}/{x}/{y}.webp', currUrl).href);
    return tileCacheUrl(terrainTileUrl);
  }

  getHillShadeLayer() {
    let initialTerrainSetting = this.searchParams.getTerrain('false');
    const visibility = initialTerrainSetting === 'false' ? 'none' : 'visible';
    return {
      'type': 'hillshade',
      'id': this.HILLSHADE_LAYER_ID,
      'source': this.HILLSHADE_SOURCE_ID,
      'maxzoom': 22,
      'layout': { 'visibility': visibility },
      'paint' : { 'hillshade-shadow-color': '#473B24' }
    };
  }

  getTerrainSource() {
    return {
      'type': 'raster-dem',
      'tiles': [this.getTerrainTileUrl()],
      'tileSize': 512,
      'maxzoom': 12,
      'minzoom': 0,
      'attribution': 'Terrain: <a href="https://bhuvan-app3.nrsc.gov.in/data/download/index.php" target="_blank">CartoDEM 30m v3r1</a>'
    };
  }

}

