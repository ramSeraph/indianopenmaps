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
- `css/view.css` - Map viewer styles (popups, inspect control, base layer picker)
- `css/raster_view.css` - Raster viewer styles
- **`viewer.html`** - Contains inline `<style>` block with source panel, download panel, and category filter styles (search for `.maplibregl-ctrl-source-panel`, `.category-filter`, `.download-panel`)

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
- `pyarrow` - Reading parquet files
- `PyQt5` - GUI framework
- `click` - CLI framework
- `aiohttp` - Async HTTP server for CORS proxy
- `geoparquet-io` - Reading remote geoparquet files with server-side filtering
- `requests` - HTTP client for API calls

### Architecture

#### CLI Structure (`iomaps/cli.py`)
```
iomaps                         # Default: launches PyQt UI
├── cli
│   ├── filter-7z             # Filter/clip GeoJSONL from 7z archives
│   ├── infer-schema          # Infer schema from 7z archive
│   ├── extract               # Extract data from remote geoparquet sources
│   └── sources               # List available remote data sources
├── download-ui-enabler       # Run CORS proxy and open viewer with download enabled
└── ui                         # Launch PyQt UI explicitly
```

#### Core Modules

- **`cli.py`**: Main entry point, Click command groups
- **`streaming.py`**: Custom py7zr IO classes for streaming extraction
  - `StreamingPy7zIO` - Line-by-line processing during decompression
  - `StreamingWriterFactory` - Factory for streaming IO instances
- **`filter_7z.py`**: Core filtering logic for 7z archives
  - `ShapeFilter` - Spatial filtering with clip/intersection options
  - `FionaWriter` / `GeoParquetWriter` - Output writers
  - `FilterFeaturePicker` - Select filter polygon by ID or key-value
  - `create_writer()` - Factory for creating appropriate writer
- **`spatial_filter.py`**: Spatial filtering utilities shared across commands
  - `ShapeFilter` - Spatial filtering (shared with extract command)
  - `create_filter()` - Factory for filter creation from bounds or filter file
- **`infer_schema.py`**: Schema inference for 7z archives
  - `SchemaFilter` - Pass-through filter for schema inference
  - `SchemaWriter` - Collects geometry types and property types
- **`helpers.py`**: Utility functions
  - `SUPPORTED_OUTPUT_DRIVERS` - Curated list (CSV, Shapefile, FlatGeobuf, GeoJSON, GeoJSONSeq, GPKG, Parquet)
  - `get_driver_from_filename()` - Infer driver from file extension
  - `fix_if_required()` - Geometry validation/fixing
  - `readable_size()` - Human-readable file sizes
- **`pyqt_ui.py`**: PyQt5 GUI application
  - Threaded workers for long operations
  - Progress tracking via signals

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
- **`extract.py`**: Extract from remote geoparquet sources
  - `get_parquet_info_from_source()` - Get parquet URL info from source config
  - `fetch_partition_metadata()` - Fetch .parquet.meta.json for partitioned sources
  - `get_partitions_for_bbox()` - Filter partitions by bbox intersection
  - `schema_from_meta()` - Build Fiona schema from partition metadata
  - `infer_schema_from_parquet()` - Infer schema from single parquet via geoparquet-io
  - `adapt_schema_for_driver()` - Adapt schema for output format (shapefile field widths, geometry types)
  - `process_parquet_source()` - Stream and filter parquet with pyarrow iter_batches
- **`sources.py`**: List available data sources from API
  - `get_vector_sources()` - Fetch and filter vector sources
  - `resolve_source()` - Resolve source by name, route, or index
  - Supports fuzzy matching and category filtering
- **`download_ui_enabler.py`**: CORS proxy server
  - aiohttp-based async server on localhost
  - Opens browser with `?cors=localhost:port` to enable downloads
  - `--no-browser` option to skip opening browser

### Key Features

1. **Streaming Processing**: Processes 7z archives without full extraction
2. **Spatial Filtering**: Filter by bounding box or polygon file
3. **Clipping**: Optionally clip features to filter boundary
4. **Multi-volume Support**: Handles split 7z archives (`.7z.001`, etc.)
5. **Schema Inference**: Auto-detect geometry and property types
6. **Multiple Output Formats**: CSV, Shapefile, FlatGeobuf, GeoJSON, GeoJSONSeq, GPKG, Parquet
7. **Remote Geoparquet**: Extract from partitioned remote sources with server-side bbox filtering
8. **Partition-aware**: Uses .parquet.meta.json to skip partitions outside filter bbox
9. **Download UI Enabler**: Local CORS proxy to enable downloads in web viewer

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
uv run iomaps cli extract -s buildings -o buildings.gpkg -b "77.5,12.9,77.7,13.1"
uv run iomaps download-ui-enabler            # Start CORS proxy and open viewer
uv run iomaps download-ui-enabler --no-browser  # Start proxy only
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
