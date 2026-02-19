A display server for the data hosted at the following repos:
* [indian_admin_boundaries](https://github.com/ramSeraph/indian_admin_boundaries)
* [indian_water_features](https://github.com/ramSeraph/indian_water_features)
* [indian_railways](https://github.com/ramSeraph/indian_railways)
* [indian_roads](https://github.com/ramSeraph/indian_roads)
* [indian_communications](https://github.com/ramSeraph/indian_communications)
* [indian_facilities](https://github.com/ramSeraph/indian_facilities)
* [indian_cadastrals](https://github.com/ramSeraph/indian_cadastrals)
* [indian_land_features](https://github.com/ramSeraph/indian_land_features)
* [india_natural_disasters](https://github.com/ramSeraph/india_natural_disasters)
* [indian_buildings](https://github.com/ramSeraph/indian_buildings)
* [indian_power_infra](https://github.com/ramSeraph/indian_power_infra)

Contains [code](https://github.com/ramSeraph/indianopenmaps/blob/main/worker/src/mosaic_handler.js) for getting tiles from a big pmtiles file which has been split into multiple shards (to overcome hosting size limits).

Tools for splitting a big pmtiles into smaller ones is at [pmtiles_mosaic](https://github.com/ramSeraph/pmtiles_mosaic).

See the list of data available at https://indianopenmaps.com/

A tool is available to filter the large .7z files in the repo based on another polygon shape or bounds and export them in other geospatial formats at [iomaps](https://github.com/ramSeraph/indianopenmaps/tree/main/python)

## Architecture

The project has two main components: a **Cloudflare Worker** that serves tiles and web viewers, and a **Python CLI tool** (`iomaps`) for data extraction and filtering.

### Worker (`worker/`)

A [Hono](https://hono.dev/)-based Cloudflare Worker that serves vector and raster tiles from [PMTiles](https://protomaps.com/docs/pmtiles) archives and [Cloud-Optimized GeoTIFFs](https://www.cogeo.org/) (COGs), along with web-based map viewers.

**Tile serving:**
- Serves vector tiles (protobuf) and raster tiles (webp/png) from PMTiles archives
- Supports "mosaic" mode for large PMTiles files that have been split into multiple shards
- Dynamically generates tiles from COGs with mask support, using [@cf-wasm/photon](https://github.com/nickreese/cf-wasm) for WebAssembly-based image processing
- Provides TileJSON metadata endpoints for each source
- Currently serves ~498 tile sources

**STAC API:**
- Implements a [STAC](https://stacspec.org/) API for browsing COG collections (e.g. India Topographic Maps 50k)
- Item indices are stored in geoparquet files and spatially indexed with [flatbush](https://github.com/mourner/flatbush)

**Web viewers (in `worker/static/`):**
- Vector tile viewer using [MapLibre GL JS](https://maplibre.org/) — layer selection, feature inspection, styling, 3D terrain, geocoding, download
- Raster tile viewer for raster/COG data
- STAC viewer using [Leaflet](https://leafletjs.com/) for browsing STAC collections
- COG viewer for on-demand COG visualization
- No build step — browser-native ES modules with dependencies loaded from [esm.sh](https://esm.sh/)

**Other endpoints:**
- `/api/routes` — lists all available tile sources

**Running locally:**
```bash
cd worker
npm install
npm run dev  # http://localhost:8787
```

### Python CLI — `iomaps` (`python/`)

A Python package ([available on PyPI](https://pypi.org/project/iomaps/)) providing CLI tools for working with the data.

**Commands:**
- `iomaps cli filter-7z` — filter/clip GeoJSONL features from .7z archives by bounding box or polygon, and export to various formats (GeoJSON, Shapefile, FlatGeobuf, GeoPackage, Parquet, etc.)
- `iomaps cli extract` — extract data from remote geoparquet sources with server-side bbox filtering (partition-aware for large datasets)
- `iomaps cli infer-schema` — infer geometry types and property schemas from .7z archives
- `iomaps cli sources` — list available remote data sources (with category filtering and fuzzy matching)
- `iomaps cli categories` — list available data categories
- `iomaps ui` — launch a PyQt5 GUI for data filtering

**Example usage:**
```bash
pip install iomaps
iomaps cli sources                    # list available sources
iomaps cli sources -c cadastral       # filter by category
iomaps cli extract -s buildings -o buildings.gpkg -b "77.5,12.9,77.7,13.1"
iomaps cli filter-7z -i data.7z -o output.geojsonl -b "min_lon,min_lat,max_lon,max_lat"
```

### Utilities (`utils/`)

Helper scripts for release management and endpoint validation.

