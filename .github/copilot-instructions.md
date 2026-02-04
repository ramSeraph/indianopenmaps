# Copilot Instructions for Indian Open Maps

This repository contains a geospatial tile server and web viewer for Indian map data. It serves vector and raster tiles from PMTiles archives and Cloud-Optimized GeoTIFFs (COGs), and provides a STAC API for data discovery.

## Project Overview

- **Purpose**: Serve geospatial data (vector tiles, raster tiles, COG imagery) with a web-based map viewer
- **Deployment**: Runs on Fly.io, containerized with Docker
- **License**: Public domain (UNLICENSE)

## Technology Stack

### Server (`server/`)
- **Framework**: Fastify (Node.js)
- **Static files**: @fastify/static serving from `static/`
- **Tile formats**: PMTiles, Cloud-Optimized GeoTIFFs (COG)
- **Image processing**: Sharp for tile format conversion
- **Key dependencies**:
  - `pmtiles` - Reading PMTiles archives
  - `@cogeotiff/core`, `@cogeotiff/source-url` - COG parsing
  - `@basemaps/tiler`, `@basemaps/tiler-sharp` - COG tile rendering
  - `hyparquet` - Reading geoparquet files for STAC

### Frontend (`static/`)
- **Map library**: MapLibre GL JS (ESM imports from esm.sh)
- **STAC viewer**: Leaflet-based
- **No build step**: Browser-native ES modules, CSS files served directly
- **Key patterns**:
  - Modules loaded via `https://esm.sh/` CDN
  - Local modules in `/js/` use ES module syntax
  - India boundary correction via `@india-boundary-corrector/maplibre-protocol`

## Server Architecture

### Handlers (in `server/`)
- **`pmtiles_handler.js`**: Serves tiles from single PMTiles archives
- **`mosaic_handler.js`**: Serves tiles from multiple PMTiles files (mosaic format v1)
- **`cog_handler.js`**: Dynamic tile generation from COGs with mask support
- **`stac_handler.js`**: STAC API for COG collections; item indices stored in geoparquet files

### Key Files
- **`server.js`**: Main entry point, route definitions, Fastify setup
- **`routes.json`**: Configuration for tile sources (URLs, types, handlers)
- **`stac_catalog.json`**: STAC catalog configuration
- **`errors.js`**: Custom HTTP error classes
- **`common.js`**: Shared utilities (MIME types, attribution)
- **`cors_whitelist.json`**: Allowed domains for CORS proxy

### API Endpoints
- `GET /api/routes` - List available tile sources
- `GET /{source}/{z}/{x}/{y}.{ext}` - Get tile
- `GET /{source}/tiles.json` - TileJSON metadata
- `GET /cog-tiles/{z}/{x}/{y}?url=` - Dynamic COG tiles
- `GET /cog-info?url=` - COG metadata
- `GET /stac/*` - STAC API endpoints
- `GET /cors-proxy?url=` - CORS proxy (whitelisted URLs only)

## Frontend Architecture (`static/`)

### Main Viewer (`viewer.js`)
- Vector tile viewer with MapLibre GL
- Modular architecture with separate handlers:
  - `base_layer_picker.js` - Base map selection
  - `vector_source_handler.js` - Vector layer management
  - `color_handler.js` - Layer styling
  - `terrain_handler.js` - 3D terrain
  - `search_param_handler.js` - URL parameter state
  - `source_panel_control.js` - Layer panel UI
  - `inspect_control.js` - Feature inspection

### STAC Viewer (`stac_viewer.js`)
- Leaflet-based viewer for STAC collections
- Displays COG tiles on-demand

### Styling
- `css/main-dark.css` - Shared dark theme
- `css/view.css` - Map viewer styles
- `css/raster_view.css` - Raster viewer styles

## Code Style & Conventions

### JavaScript
- Use CommonJS `require()` in server code
- Use ES modules (`import/export`) in frontend code
- Prefer `async/await` over callbacks
- Use `const`/`let`, avoid `var`
- Error handling: throw custom error classes from `errors.js`

### Server Patterns
- All route handlers should set CORS headers: `Access-Control-Allow-Origin: *`
- Cache tiles with `Cache-Control: max-age=86400000` (long-lived)
- Use logger (`fastify.log`) for logging, not `console.log`
- Lazy initialization pattern: `initIfNeeded()` for handlers

### Frontend Patterns
- No bundler - use browser-native ES modules
- Import external libs from `https://esm.sh/`
- MapLibre controls as ES6 classes
- URL hash for map state (`#map=zoom/lat/lon`)

## Running Locally

```bash
npm install
npm start  # Runs on http://localhost:3000
```

## Testing

Currently no automated tests. Manual testing via the web interface.

## Adding New Tile Sources

1. Add entry to `server/routes.json` with:
   - `url`: PMTiles URL
   - `type`: "vector" or "raster"
   - `handlertype`: "pmtiles" or "mosaic"
   - `datameet_attribution`: boolean for attribution

2. The source will automatically be available at `/{key}/` routes

## Important Notes

- COG handler assumes EPSG:3857 (Web Mercator) projection
- COG mask handling requires consistent mask presence across all source TIFFs
- STAC items are cached in memory after first load from geoparquet
- PMTiles sources are lazily initialized on first request
