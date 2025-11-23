import * as maplibregl from 'https://esm.sh/maplibre-gl@5.6.2';

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

  getTerrainTileUrl() {
    const currUrl = window.location.href;
    const terrainTileUrl = decodeURI(new URL('/dem/terrain-rgb/cartodem-v3r1/bhuvan/{z}/{x}/{y}.webp', currUrl).href);
    return terrainTileUrl;
  }

  getHillShadeLayer() {
    let initialTerrainSetting = this.searchParams.getTerrain('false');
    const visibility = initialTerrainSetting === 'false' ? 'none' : 'visible';
    return {
      'type': 'hillshade',
      'id': this.HILLSHADE_LAYER_ID,
      'source': this.HILLSHADE_SOURCE_ID,
      'maxzoom': 14,
      'layout': { 'visibility': visibility },
      'paint' : { 'hillshade-shadow-color': '#473B24' }
    };
  }

  getTerrainSource() {
    return {
      'type': 'raster-dem',
      'tiles': [this.getTerrainTileUrl()],
      'tileSize': 256,
      'maxzoom': 12,
      'minzoom': 5,
      'attribution': 'Terrain: <a href="https://bhuvan-app3.nrsc.gov.in/data/download/index.php" target="_blank">CartoDEM 30m v3r1</a>'
    };
  }

}


