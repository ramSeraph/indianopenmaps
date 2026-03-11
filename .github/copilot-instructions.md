# Copilot Instructions for Indian Open Maps

This repository contains a geospatial tile server and web viewer for Indian map data. It serves vector and raster tiles from PMTiles archives and Cloud-Optimized GeoTIFFs (COGs), provides a STAC API for data discovery, and supports client-side data downloads in multiple formats.

## Project Overview

- **Purpose**: Serve geospatial data (vector tiles, raster tiles, COG imagery) with a web-based map viewer and download capabilities
- **Deployment**: Cloudflare Workers (with Fly.io redirect for legacy URLs)
- **License**: Public domain (UNLICENSE)

## Technology Stack

### Worker (`worker/`)
- **Framework**: Hono (lightweight web framework for Cloudflare Workers)
- **Static files**: Cloudflare Workers Assets from `static/`
- **Tile formats**: PMTiles, Cloud-Optimized GeoTIFFs (COG)
- **Image processing**: @cf-wasm/photon for tile format conversion
- **Key dependencies**:
  - `pmtiles` - Reading PMTiles archives
  - `@cogeotiff/core`, `@cogeotiff/source-url` - COG parsing
  - `@basemaps/tiler`, `@basemaps/geo` - COG tile rendering
  - `@cf-wasm/photon` - WebAssembly image processing for Workers
  - `@mapbox/tilebelt` - Tile math utilities
  - `hyparquet` - Reading geoparquet files for STAC
  - `flatbush` - Spatial indexing for mosaic tile lookup and STAC items
  - `pako` - Deflate decompression for COG masks

### Frontend (`worker/static/`)
- **Map library**: MapLibre GL JS (ESM imports from esm.sh)
- **Raster viewer**: Leaflet with side-by-side comparison
- **STAC viewer**: Leaflet-based
- **Data processing**: DuckDB WASM with spatial/httpfs extensions
- **File output**: GeoPackage via wa-sqlite Web Worker, GeoParquet via parquet-wasm
- **Temporary storage**: Origin Private File System (OPFS) for intermediate files
- **No build step**: Browser-native ES modules, CSS files served directly
- **Key patterns**:
  - Modules loaded via `https://esm.sh/` CDN
  - Local modules in `/js/` use ES module syntax
  - India boundary correction via `@india-boundary-corrector/maplibre-protocol`
  - Web Workers for heavy processing (GeoPackage writing)
  - Web Locks API for tab-aware orphan file cleanup

## Worker Architecture

### Handlers (in `worker/src/`)
- **`pmtiles_handler.js`**: Serves tiles from single PMTiles archives (shared 100-item promise cache)
- **`mosaic_handler.js`**: Serves tiles from multiple PMTiles files (mosaic format v1, Flatbush spatial index for efficient tile lookup when ≥10 sources)
- **`cog_handler.js`**: Dynamic tile generation from COGs with mask support (FilteredTiff separates mask/RGB, MaskAwareTiler for alpha blending)
- **`stac_handler.js`**: STAC API v1.0.0 for COG collections; item indices stored in geoparquet files
- **`tile_maker_photon.js`**: WebAssembly-based tile composition using @cf-wasm/photon (crop, resize, overlay, RGBA handling)

### Key Files
- **`index.js`**: Main entry point, Hono app setup with modular route registration
- **`routes/`**: Route handlers split by feature (tiles, stac, cog, proxy, static)
- **`routes/listing.json`**: Configuration for tile sources (~4,900 lines; URLs, types, handlers, categories)
- **`routes/stac_catalog.json`**: STAC catalog configuration
- **`routes/proxy_whitelist.json`**: Allowed domains for proxy (GitHub URLs, localhost)
- **`errors.js`**: Custom HTTP error classes (HttpError base → NotFoundError, BadRequestError, ForbiddenError, UnauthorizedError, UnknownError, ResourceUnavailableError)
- **`common.js`**: Shared utilities (getMimeType, getExt, extendAttribution)
- **`wrangler.toml`**: Cloudflare Workers configuration

### API Endpoints
- `GET /api/routes` - List available tile sources
- `GET /{source}/{z}/{x}/{y}.{ext}` - Get tile (.pbf, .webp, .png)
- `GET /{source}/tiles.json` - TileJSON 3.0.0 metadata
- `GET /cog-tiles/:z/:x/:y?url=&format=` - Dynamic COG tiles (png/webp)
- `GET /cog-info?url=` - COG metadata (bbox, resolution, size)
- `GET /stac` - STAC landing page
- `GET /stac/conformance` - STAC conformance
- `GET /stac/collections` - List STAC collections (limit/offset)
- `GET /stac/collections/:id` - Get collection
- `GET /stac/collections/:id/items` - List items (bbox filtering)
- `GET /stac/collections/:id/items/:itemId` - Get item
- `GET /stac/search` - Search items (GET)
- `POST /stac/search` - Search items (POST)
- `GET /proxy?url=` - Proxy (whitelisted URLs, supports Range requests)

### Static Pages
- `/` → `index.html` (landing page)
- `/vectors` → `vectors.html`
- `/rasters` → `rasters.html`
- `/viewer` → `viewer.html` (main vector viewer)
- `/raster-viewer` → `raster_view.html` (dual-map raster comparison)
- `/stac-viewer` → `stac_viewer.html`
- `/cog-viewer` → `cog-viewer.html`
- `/data-help` → `data-help.html`

## Frontend Architecture (`worker/static/`)

### Main Viewer (`js/viewer.js`)
- Vector tile viewer with MapLibre GL
- Modular architecture with separate handlers:
  - `base_layer_picker.js` - Base map selection (Carto/OSM/ESRI/Google + custom raster sources)
  - `vector_source_handler.js` - Vector layer management (MVT tiles, fill-extrusion for 3D buildings)
  - `color_handler.js` - Layer color palette (12 colors, session memory via sessionStorage)
  - `terrain_handler.js` - 3D terrain using CartoDEM v3r1
  - `search_param_handler.js` - URL parameter state (query & hash params)
  - `source_panel_control.js` - Vector source selector panel with category filtering
  - `sidebar_control.js` - Modular sidebar with icon buttons (search, layers, sources, downloads)
  - `inspect_control.js` - Feature inspection popups with HTML-escaped property display
  - `nominatim_geocoder.js` - Nominatim search adapter for MapLibre geocoder
  - `routes_handler.js` - Fetches `/api/routes`, separates raster vs vector sources
  - `size_getter.js` - File size estimation utilities
  - `extent_handler.js` - Visualizes data extents and row group extents as map rectangles

### Download System (`js/download_panel_control.js` + related)
- Multi-format download UI with spatial filtering (bbox or polygon)
- **Download pipeline**: Remote Parquet → DuckDB (httpfs) → spatial filter → OPFS intermediate → format handler → Blob download
- Format handlers (all extend `format_base.js`):
  - `format_csv.js` - CSV with geometry as WKT
  - `format_geojson.js` - GeoJSON / GeoJSONSeq
  - `format_geoparquet.js` - GeoParquet v1.1/v2.0 with Hilbert spatial sorting
  - `format_geopackage.js` - GeoPackage via Web Worker (wa-sqlite with R-tree index)
- Supporting modules:
  - `partial_download_handler.js` - Orchestrates DuckDB → OPFS → format handlers; orphan OPFS cleanup via Web Locks
  - `gpkg_worker.js` - Web Worker: reads intermediate parquet from OPFS, writes GeoPackage with spatial index
  - `parquet_metadata.js` - Parquet metadata caching; fetches `.parquet.meta.json`; partition/bbox lookups
  - `duckdb_client.js` - Singleton DuckDB WASM client (v1.33.0, spatial + httpfs extensions)

### STAC Viewer (`js/stac_viewer.js`)
- Leaflet-based viewer for STAC collections
- Displays COG tiles on-demand

### Raster Viewer (`js/raster_view.js`)
- Dual-map Leaflet viewer with side-by-side comparison and synchronized panning

### Styling
- `css/main-dark.css` - Shared dark theme (console-style)
- `css/viewer.css` - Map viewer styles (sidebar, panels, map controls)
- `css/raster_view.css` - Dual-map raster viewer styles
- **`viewer.html`** - Contains inline `<style>` block with source panel, download panel, and category filter styles (search for `.maplibregl-ctrl-source-panel`, `.category-filter`, `.download-panel`)

## Code Style & Conventions

### JavaScript
- Use ES modules (`import/export`) in both worker and frontend code
- Prefer `async/await` over callbacks
- Use `const`/`let`, avoid `var`
- Error handling: throw custom error classes from `errors.js`

### Worker Patterns
- All route handlers should set CORS headers: `Access-Control-Allow-Origin: *`
- Cache tiles with `Cache-Control: max-age=86400000` (long-lived)
- Use console logger for logging (`console.log`, `console.error`)
- Lazy initialization pattern: `initIfNeeded()` for handlers
- Shared promise caches (100 items) across handler instances

### Cloudflare Workers Restrictions
Even with `nodejs_compat` enabled, Workers have significant limitations:

**Cannot use:**
- Native Node.js modules (fs, path, child_process, etc.)
- NPM packages with native bindings (Sharp, better-sqlite3, etc.)
- Packages that rely on Node.js streams in incompatible ways
- Long-running processes (50ms CPU time limit on free, 30s on paid)
- More than 6 concurrent subrequests (1000 total per request)
- File system access - all data must come from fetch, KV, R2, or be bundled

**Alternatives used in this project:**
- `@cf-wasm/photon` instead of Sharp for image processing
- `fetch()` for all external data access
- Cloudflare Assets for static file serving
- In-memory caching with module-level variables (persists across requests in same isolate)

**Best practices:**
- Check package compatibility before adding dependencies
- Prefer WebAssembly-based packages for heavy computation
- Use streaming responses where possible
- Be aware of memory limits (~128MB)
- Test with `wrangler dev` which simulates the Workers environment

### Frontend Patterns
- No bundler - use browser-native ES modules
- Import external libs from `https://esm.sh/`
- MapLibre controls as ES6 classes
- URL hash for map state (`#map=zoom/lat/lon`)
- Web Workers for CPU-heavy tasks (GeoPackage writing)
- OPFS for temporary file storage during downloads
- Web Locks API for cross-tab resource coordination

## Running Locally

```bash
cd worker
npm install
npm run dev  # Runs on http://localhost:8787
```

## Deployment

```bash
cd worker
npm run deploy  # Deploys to Cloudflare Workers
```

## Testing

### Worker / Integration Tests (`tests/`)
- `test_tile.js`, `test_multiple_tiles.js` - Tile serving tests
- `test_tile_equivalence.js` - Tile comparison tests
- `test_complete_system.sh` - Full system integration test
- `check_cog.js` - COG validation
- `test_tile_output/` - Reference PNG/WebP images for comparison

### Python Tests (`python/tests/`)
- `test_cli_filter_7z.py` - 7z filtering with bounds & filter files
- `test_cli_filter_7z_advanced.py` - Advanced filtering scenarios
- `test_cli_infer_schema.py` - Schema inference tests
- `test_infer_schema.py` - Core schema inference logic tests
- Run with: `cd python && uv run pytest`

## Adding New Tile Sources

1. Add entry to `worker/src/routes/listing.json` with:
   - `url`: PMTiles URL
   - `type`: "vector" or "raster"
   - `handlertype`: "pmtiles" or "mosaic"
   - `datameet_attribution`: boolean for attribution
   - Optional: `category`, `promoteid`, `tilesuffix`

2. The source will automatically be available at `/{key}/` routes

## Important Notes

- COG handler assumes EPSG:3857 (Web Mercator) projection
- COG mask handling requires consistent mask presence across all source TIFFs
- STAC items are cached in memory after first load from geoparquet
- PMTiles sources are lazily initialized on first request
- Worker uses @cf-wasm/photon instead of Sharp for image processing (Workers-compatible)
- Proxy endpoint validates URLs against whitelist (GitHub URLs and localhost only)
- DuckDB WASM in frontend uses a custom build hosted at ramseraph.github.io

## Other Directories

- **`fly-redirect/`**: Nginx redirect service on Fly.io (indianopenmaps.fly.dev → indianopenmaps.com)
- **`data_processing/`**: Scripts for data processing (create_geoparquet.py, create_parquet_meta.py, update_partitioned_routes.py, upgrade_mosaic.sh, etc.)
- **`utils/`**: Utility scripts (check_endpoints.js, validate_bboxes.py, make_release.py, etc.)
- **`tests/`**: Integration tests for tile serving and COG processing
- **`plans/`**: Planning documents

---

## Python Package (`python/`)

The `iomaps` Python package provides CLI tools and a PyQt UI for processing geospatial data from 7z archives containing GeoJSONL files and remote geoparquet sources.

### Overview

- **Package name**: `iomaps` (available on PyPI)
- **Python version**: ≥3.12
- **Build system**: Hatchling with hatch-vcs for versioning
- **Entry point**: `iomaps` CLI command

### Key Dependencies

- `py7zr` - Reading 7z archives
- `multivolumefile` - Multi-volume 7z archive support
- `shapely` - Geometry operations
- `fiona` - Reading/writing geospatial file formats
- `PyQt5` - GUI framework
- `click` - CLI framework
- `geoparquet-io` - Reading remote geoparquet files with server-side filtering
- `requests` - HTTP client for API calls
- `rich` - CLI rich text formatting and tables

### Architecture

#### CLI Structure (`iomaps/cli.py`)
```
iomaps
├── cli
│   ├── filter-7z             # Filter/clip GeoJSONL from 7z archives
│   ├── infer-schema          # Infer schema from 7z archive
│   ├── extract               # Extract data from remote geoparquet sources (DuckDB)
│   ├── sources               # List available remote data sources
│   └── categories            # List source categories with counts
└── ui                         # Launch PyQt UI
```

#### Core Modules (`iomaps/core/`)

- **`streaming_7z.py`**: Custom py7zr IO classes for streaming extraction
  - `StreamingPy7zIO` - Line-by-line processing during decompression
  - `StreamingWriterFactory` - Factory for streaming IO instances
- **`spatial_filter.py`**: Spatial filtering utilities shared across commands
  - `ShapeFilter` - Spatial filtering with clip/intersection options
  - `create_filter()` - Factory for filter creation from bounds or filter file
  - `FilterFeaturePicker` - Select filter polygon by ID or key-value
- **`infer_schema.py`**: Schema inference for 7z archives
  - `SchemaFilter` - Pass-through filter for schema inference
  - `SchemaWriter` - Collects geometry types and property types
- **`helpers.py`**: Utility functions
  - `SUPPORTED_OUTPUT_DRIVERS` - CSV, Shapefile, FlatGeobuf, GeoJSON, GeoJSONSeq, GPKG, KML, GeoParquet-1.1, GeoParquet-2.0
  - `get_driver_from_filename()` - Infer driver from file extension
  - `fix_if_required()` - Geometry validation/fixing
  - `readable_size()` - Human-readable file sizes
- **`writers.py`**: Output writers
  - `FionaWriter` - Uses fiona library to write features to various formats
  - `create_writer()` - Factory for creating appropriate writer

#### Command Modules (`iomaps/commands/`)

- **`decorators.py`**: Shared CLI option decorators
  - `add_routes_options` - Adds --routes-file and --routes-url options
  - `add_log_level_option(default)` - Adds --log-level option (factory pattern)
  - `add_filter_options` - Adds bounds, filter-file, clip, geom-type options
  - `validate_routes_options()` - Validates mutual exclusivity
  - `validate_filter_options()` - Validates filter option combinations
  - `DEFAULT_ROUTES_URL` - Default API endpoint
- **`filter_7z.py`**: CLI wrapper for filter-7z command
- **`infer_schema.py`**: CLI wrapper for infer-schema command
- **`schema_common.py`**: Shared schema extraction logic for 7z archives
- **`extract.py`**: Extract from remote geoparquet sources using DuckDB
  - Uses DuckDB with spatial + httpfs extensions (2GB memory limit)
  - Spatial queries with ST_Intersects, ST_MakeEnvelope, ST_GeomFromText
  - `get_parquet_info_from_source()` - Get parquet URL info from source config
  - `fetch_partition_metadata()` - Fetch .parquet.meta.json for partitioned sources
  - `get_partitions_for_bbox()` - Filter partitions by bbox intersection
  - GeoParquet output via geoparquet-io's `write_parquet_with_metadata`
- **`sources.py`**: List available data sources from API
  - `get_vector_sources()` - Fetch and filter vector sources
  - `resolve_source()` - Resolve source by name, route, or index (fuzzy matching)
  - `categories` command - Lists categories sorted by count with rich table output
- **`pyqt_ui.py`**: PyQt5 GUI application (largest module, ~70KB)
  - Threaded workers for long operations
  - Progress tracking via signals

### Key Features

1. **Streaming Processing**: Processes 7z archives without full extraction
2. **Spatial Filtering**: Filter by bounding box or polygon file
3. **Clipping**: Optionally clip features to filter boundary
4. **Multi-volume Support**: Handles split 7z archives (`.7z.001`, etc.)
5. **Schema Inference**: Auto-detect geometry and property types with case-insensitive duplicate handling
6. **Multiple Output Formats**: CSV, Shapefile, FlatGeobuf, GeoJSON, GeoJSONSeq, GPKG, KML, GeoParquet-1.1, GeoParquet-2.0
7. **Remote Geoparquet**: Extract from partitioned remote sources via DuckDB with server-side bbox filtering
8. **Partition-aware**: Uses .parquet.meta.json to skip partitions outside filter bbox

### Schema Handling

- **Partitioned sources**: Schema (properties + geometry_types) from `.parquet.meta.json`
- **Single parquet**: Schema inferred via geoparquet-io metadata
- **Geometry type expansion**: Point → [Point, MultiPoint] (clipping can create multi geometries)
- **Shapefile limitations**: Single geometry type only; collapses to Multi variant if needed
- **Field widths**: Shapefiles use `int:19` and `float:32.15` to avoid GDAL warnings

### Code Style

- Use standard library `logging` module (output to stderr)
- Click decorators for CLI commands
- Type hints are optional but appreciated
- Classes for stateful operations (filters, writers)
- Use `ruff` for linting and formatting (line-length: 120)

### Running Tests

```bash
cd python
uv run pytest  # or: pytest (with venv activated)
```

### Running Locally

```bash
cd python
uv run iomaps cli filter-7z -i data.7z -o output.geojsonl -b "min_lon,min_lat,max_lon,max_lat"
uv run iomaps cli sources                    # List available remote sources
uv run iomaps cli sources -c cadastral       # Filter by category
uv run iomaps cli categories                 # List all categories with counts
uv run iomaps cli extract -s buildings -o buildings.gpkg -b "77.5,12.9,77.7,13.1"
uv run iomaps ui                             # Launch GUI
```

### Adding New CLI Commands

1. Create command module in `iomaps/commands/`
2. Define Click command with `@click.command()`
3. Register in `iomaps/cli.py` via `cli.add_command()` or `main_cli.add_command()`
4. Use shared decorators:
   - `@add_filter_options` - For spatial filtering
   - `@add_routes_options` - For API/routes configuration
   - `@add_log_level_option(default="INFO")` - For logging control
