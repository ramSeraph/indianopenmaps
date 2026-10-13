// Public browser key; allowed origins are managed in the Protomaps account.
const API_KEY = '693abf6cacee62be';
export const PROTOMAPS_DEFAULT_LAYER_NAME = 'Protomaps Black';
export const PROTOMAPS_GLYPHS = 'https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf';
const BOUNDARY_SOURCE_ID = 'basemap-soi-states';
const BOUNDARY_TILEJSON_URL = '/states/soi/tiles.json';
let boundaryPromise;

export function getProtomapsBaseLayers() {
  return ['black', 'white', 'grayscale'].map(flavor => ({
    name: `Protomaps ${flavor[0].toUpperCase()}${flavor.slice(1)}`,
    styleUrl: `https://api.protomaps.com/styles/v5/${flavor}/en.json?key=${API_KEY}`,
    sources: {},
  }));
}

export function resolveBaseLayerName(name) {
  // Keep existing shared links working after the provider change.
  return {
    'Carto OSM Dark': PROTOMAPS_DEFAULT_LAYER_NAME,
    'CartoDB Light': 'Protomaps White',
    'Protomaps Dark': PROTOMAPS_DEFAULT_LAYER_NAME,
    'Protomaps Light': 'Protomaps White',
  }[name] || name;
}

export async function fetchMapJSON(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Map resource request failed (${response.status})`);
  return response.json();
}

export async function loadProtomapsStyle(layerInfo) {
  // All themes share this dataset. Retry failed metadata loads on the next pick.
  boundaryPromise ||= fetchMapJSON(BOUNDARY_TILEJSON_URL).catch(error => {
    boundaryPromise = null;
    throw error;
  });
  const [style, boundary] = await Promise.all([
    fetchMapJSON(layerInfo.styleUrl), boundaryPromise,
  ]);
  if (style.version !== 8 || !style.sources || !Array.isArray(style.layers)) {
    throw new Error('Invalid Protomaps style');
  }
  if (!style.layers.some(layer => layer.id === 'boundaries')) {
    throw new Error('Protomaps style is missing its boundary layer');
  }
  const boundaryLayer = boundary.vector_layers?.find(layer => layer.id === 'SOI_States');
  if (!boundaryLayer || !boundary.tiles?.length) {
    throw new Error('SOI States TileJSON is missing its boundary layer or tiles');
  }

  // Namespace the basemap so it cannot collide with data overlays.
  const sourceId = id => `basemap-${id}`;
  return {
    sources: {
      ...Object.fromEntries(Object.entries(style.sources).map(([id, source]) => [sourceId(id), source])),
      [BOUNDARY_SOURCE_ID]: {
        type: 'vector',
        tiles: boundary.tiles,
        minzoom: boundary.minzoom ?? 0,
        maxzoom: boundary.maxzoom,
        bounds: boundary.bounds,
        attribution: boundary.attribution,
      },
    },
    layers: style.layers.filter(layer => layer.id !== 'boundaries_country').map(layer => {
      if (layer.id === 'boundaries') {
        // SOI state polygons supply both the country outline and state borders.
        // Retain the theme's color/dashes, with twice its boundary line width.
        // Drop the Protomaps-specific filter for the SOI polygon outlines.
        const { source, 'source-layer': sourceLayer, filter, ...appearance } = layer;
        return {
          ...appearance,
          id: 'basemap-soi-boundaries',
          source: BOUNDARY_SOURCE_ID,
          'source-layer': boundaryLayer.id,
          paint: {
            ...appearance.paint,
            'line-width': 2 * (appearance.paint['line-width'] ?? 1),
          },
        };
      }
      return {
        ...layer,
        id: `basemap-${layer.id}`,
        ...(layer.source ? { source: sourceId(layer.source) } : {}),
      };
    }),
    glyphs: style.glyphs,
    sprite: style.sprite,
  };
}
