A display server for the data hosted at the following repos:
* [indian_admin_boundaries](https://github.com/ramSeraph/indian_admin_boundaries)
* [indian_water_features](https://github.com/ramSeraph/indian_water_features)
* [indian_transport](https://github.com/ramSeraph/indian_transport)
* [indian_communications](https://github.com/ramSeraph/indian_communications)
* [indian_facilities](https://github.com/ramSeraph/indian_facilities)
* [indian_cadastrals](https://github.com/ramSeraph/indian_cadastrals)
* [indian_land_features](https://github.com/ramSeraph/indian_land_features)
* [india_natural_disasters](https://github.com/ramSeraph/india_natural_disasters)
* [indian_buildings](https://github.com/ramSeraph/indian_buildings)
* [indian_power_infra](https://github.com/ramSeraph/indian_power_infra)
* [indian_industries](https://github.com/ramSeraph/indian_industries)

Contains [code](https://github.com/ramSeraph/indianopenmaps/blob/main/worker/src/mosaic_handler.js) for getting tiles from a big pmtiles file which has been split into multiple shards (to overcome hosting size limits).

Tools for splitting a big pmtiles into smaller ones is at [pmtiles_mosaic](https://github.com/ramSeraph/pmtiles_mosaic).

See the list of data available at https://indianopenmaps.com/

Use [iomaps](https://github.com/ramSeraph/indianopenmaps/tree/main/python) to extract remote GeoParquet features by polygon or bounding box and export them in other geospatial formats.

## Architecture

The project has two main components: a **Cloudflare Worker** that serves tiles and web viewers, and a **Python CLI tool** (`iomaps`) for data extraction and filtering.

### Worker (`worker/`)

A [Hono](https://hono.dev/)-based Cloudflare Worker that serves vector and raster tiles from [PMTiles](https://protomaps.com/docs/pmtiles) archives and [Cloud-Optimized GeoTIFFs](https://www.cogeo.org/) (COGs), along with web-based map viewers.

**Tile serving:**
- Serves vector tiles (protobuf) and raster tiles (webp/png) from PMTiles archives
- Supports "mosaic" mode for large PMTiles files that have been split into multiple shards
- Dynamically generates tiles from COGs using [@developmentseed/geotiff](https://github.com/developmentseed/deck.gl-raster) for reading, decoding, and pairing overview masks, and [@cf-wasm/photon](https://github.com/nickreese/cf-wasm) for WebAssembly-based image processing
- Provides TileJSON metadata endpoints for each source
- Currently serves ~498 tile sources

TileJSON and COG-info responses use `Cache-Control: no-cache`; tile responses use
`public, max-age=300, must-revalidate`. Both include content ETags and return
`304 Not Modified` for matching `If-None-Match` requests. Revalidation still reads
or renders the current response before comparing its ETag; it saves response
bandwidth, not all upstream reads or rendering work. Errors use `no-store`.

This behavior is enabled by `[vars] TILE_CACHE_REVALIDATION = "true"` in
`worker/wrangler.toml`. To roll it back, set the value to `"false"` and push.
The switch restores the previous long response lifetimes, disables conditional
responses and periodic metadata refresh, and disables the viewer URL migration
on the next page load. No preview mode or package release is needed.

On the next request after five minutes, each Worker instance revalidates its
`mosaic.json` with `If-None-Match` (or downloads it again if no ETag was supplied).
An unchanged manifest retains its resolver; a changed manifest is validated before
replacing it. Single-archive PMTiles metadata also refreshes after five minutes,
using the library's ETag mismatch recovery. Failed refreshes retain the last valid
metadata and retry after 30 seconds or the upstream `Retry-After`, whichever is
longer. These source and browser freshness windows are separate, so a manifest
change can take up to roughly ten minutes to appear on subsequent tile requests.
COG responses follow the same browser policy; their existing byte-range cache
does not yet revalidate replaced source COGs by ETag.

Our viewers append a fixed `iomaps-cache=2` query parameter to metadata and tile
requests to bypass responses previously cached for 1,000 days. This is a one-time
migration, not a per-publication version: release asset filenames and API paths
stay the same. Other clients holding old responses need a forced reload or their
own cache-busting query parameter once. Already displayed tiles are not pushed
updates; reload the page or map source to request them again.

A November 2026 public GitHub client review found ordinary browser tile loaders
in [Amche Atlas](https://github.com/publicmap/amche-atlas/blob/0ef70e319560f0008d53ced1923358956133c6d9/js/map-utils.js#L677),
[Bhoonaksha](https://github.com/saurabh4269/bhoonaksha/blob/4ba34ddf3ecdb4b5f18c9a8d4baee9d9318d40ce/js/app.js#L7),
[AeroForge](https://github.com/sharmapalak09082006-stack/AeroForge/blob/4bc67d41c7fa0c5fc3623848e2b99b3d591c9f35/client/src/components/MapViewer.tsx#L90),
[FSS Survey Calculator](https://github.com/moorthimms/FSS-survey-calculator/blob/01b3ff2d5c0878e63fc4adabbe14f041f79ffee3/map_assets/app.js#L1140),
[Goa AQI Logger](https://github.com/publicmap/goa-aqi-logger/blob/9930dfeb0f4f86a2085b7b17a609b4d4e4b5b981/index.html#L1035),
and [amnmalik's hiking map](https://github.com/amnmalik/mysite/blob/c8ba53b20446a1a5e4c3ea42038f258e32c48d43/static/hikes/index.html#L373).
No incompatible custom conditional-request handling was found in the reviewed
code. Cross-origin Chrome checks cover JSON fetch, binary fetch/XHR, and image
loading: automatic HTTP revalidation exposes the cached body as a successful
response to application code. This is a source and transport compatibility check,
not an end-to-end test of every external application or an inventory of private
and unindexed clients. Direct GitHub release downloads are unaffected.

**STAC API:**
- Implements a [STAC](https://stacspec.org/) API for browsing raster collections (e.g. SOI Open series maps 50k)
- Item indexes are stored in STAC GeoParquet files in GitHub releases; searches preserve source footprints and support bbox, intersects, datetime, IDs and complete GET/POST pagination.
- The HTTP API is optional for clients that read STAC GeoParquet: `iomaps` and Rustac can search the same release indexes directly on GitHub. See [direct-index examples and API differences](python/README.md#search-release-indexes-with-other-stac-clients).
- EPSG:3857 COG items expose standard Web Map Links `xyz` links for generic viewers. See [compatibility checks](worker/test/STAC_COMPATIBILITY.md).

**Web viewers (in `worker/static/`):**
- Vector tile viewer using [MapLibre GL JS](https://maplibre.org/) — layer selection, feature inspection, styling, 3D terrain, geocoding, and partial data download from remote parquet sources in multiple formats using [geoparquet-extractor](https://github.com/ramSeraph/geoparquet-extractor)
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

To override the Protomaps browser key locally, set `PROTOMAPS_API_KEY` in
`worker/.dev.vars`. This local configuration file is ignored by Git.

The COG and STAC viewers use the Worker's `/cog-info` and `/cog-tiles/{z}/{x}/{y}`
APIs. COG tiling supports north-up EPSG:3857 byte imagery. The reader uses a shared
32 MiB compressed-range cache containing only completed reads; readers and fetch
queues belong to individual requests. It uses Photon for JPEG/WebP decoding and
xmldom for GDAL XML metadata, with no browser canvas or decoder threads in the Worker.
Run `npm test` from `worker/` for offline workerd tests covering masks, overviews,
PNG/WebP output, edge crops, caching, concurrent/cancelled zoom requests, and error responses.

### Python CLI — `iomaps` (`python/`)

A Python package ([available on PyPI](https://pypi.org/project/iomaps/)) providing CLI tools for working with the data.

Agent usage guidance is split into two skills:

- [iomaps-client](.agents/skills/iomaps-client/SKILL.md) — discover sources and extract data.
- [iomaps-admin](.agents/skills/iomaps-admin/SKILL.md) — create coverage, prepare datasets, and publish releases.

**Commands:**
- `iomaps cli stac` — cached raster discovery and bbox imagery extraction directly from GitHub releases, without calling the Indian Open Maps API ([Python API and CLI examples](python/README.md#raster-discovery-and-bbox-extraction))
- `iomaps cli extract` — extract intersecting features from remote GeoParquet, pruning partitions and row groups by bounding box
- `iomaps cli sources` — list available remote data sources (with category filtering and fuzzy matching)
- `iomaps cli search --bounds "77.1,28.5,77.3,28.7"` — find candidate sources using cached coverage
- `iomaps cli source-categories` — list available data categories
- `iomaps ui` — launch a PyQt5 GUI for remote Parquet extraction
- `iomaps admin coverage` — create a dataset's 1-bit coverage PNG from local GeoParquet

**Example usage:**
```bash
pip install iomaps
iomaps cli sources                    # list available sources
iomaps cli sources -c cadastral       # filter by category
iomaps cli extract -s buildings -o buildings.gpkg -b "77.5,12.9,77.7,13.1"
```

### Utilities (`utils/`)

Helper scripts for endpoint validation. Dataset preparation and publication use
`iomaps admin prepare` and `iomaps admin make-release`; see the
[maintainer workflow](python/README.md#preparing-and-publishing-datasets).

