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

Configure the periods in `worker/wrangler.toml` and push:

```toml
[vars]
CACHE_REVALIDATION_SECONDS = "3600"
TILE_CACHE_MAX_AGE_SECONDS = "300"
```

The first setting controls source metadata and STAC index refresh (one hour by
default). The second controls browser tile freshness independently (five minutes
by default); `0` requires browser revalidation on every reuse. Missing or invalid
values use these defaults; durations must be non-negative whole seconds.

Set `CACHE_REVALIDATION_SECONDS` to `"0"` to disable periodic source/STAC refresh
and restore the previous tile/TileJSON/COG response policy: long response lifetimes,
no conditional responses, and no viewer URL migration on the next page load.
The old `TILE_CACHE_REVALIDATION` boolean is replaced by this duration setting.
No preview mode or package release is needed.

On the next request after the configured interval, each Worker instance revalidates its
`mosaic.json` with `If-None-Match` (or downloads it again if no ETag was supplied).
An unchanged manifest retains its resolver; a changed manifest is validated before
replacing it. Single-archive PMTiles metadata refreshes at the same interval,
using the library's ETag mismatch recovery. Failed refreshes retain the last valid
metadata and retry after 30 seconds or the upstream `Retry-After`, whichever is
longer. These source and browser freshness windows are separate, so a manifest
change can take up to roughly 65 minutes with the defaults to appear on subsequent
tile requests, provided upstream revalidation succeeds. These are maximum freshness
windows, not guaranteed cache retention: a new isolate or eviction requires a new read.

COG sources use the same refresh interval, with conditional HEAD requests.
Cached byte ranges are keyed by the source's strong ETag and size; network range
reads use `If-Match`. If the source changes during rendering, the Worker reopens
the entire reader and retries once, keeping TIFF metadata, masks and image blocks
from the same version. Sources without a strong ETag skip the shared byte cache.
The same rollback switch disables COG source revalidation too.

STAC's decoded search indexes and Parquet metadata use the source refresh interval
as well. Their memory budgets and the ETag/revision-keyed byte-range cache remain
independent of this interval.

Our viewers append a fixed `iomaps-cache=2` query parameter to metadata and tile
requests to bypass responses previously cached for 1,000 days. This is a one-time
migration, not a per-publication version: release asset filenames and API paths
stay the same. Other clients holding old responses need a forced reload or their
own cache-busting query parameter once. Already displayed tiles are not pushed
updates; reload the page or map source to request them again.

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

