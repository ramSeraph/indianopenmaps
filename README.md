A display server for geospatial data hosted as **GitHub Releases assets** in the
following repositories:

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

GitHub Releases downloads do not enable
[CORS](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS), so browser
JavaScript cannot read these files directly from another site. The Cloudflare
Worker exists to make that data usable in web maps: it reads the release assets
and serves tiles and metadata with CORS enabled. Its viewers use a same-origin
proxy for file reads. The data files remain hosted on GitHub Releases.
Command-line tools such as `iomaps` can read
them directly because they are not subject to browser CORS restrictions.

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
- Dynamically generates tiles from north-up EPSG:3857 COG byte imagery
- Provides TileJSON metadata endpoints for each source

See [Worker caching](worker/CACHING.md) for cache policy, refresh configuration,
and viewer cache migration.

**STAC API:**

- Implements a [STAC](https://stacspec.org/) API for browsing raster collections (e.g. SOI Open series maps 50k)
- Supports spatial, date and ID filters, with paginated search results.
- The HTTP API is optional for clients that read STAC GeoParquet: `iomaps` and Rustac can search the same release indexes directly on GitHub. See [direct-index examples and API differences](python/README.md#search-release-indexes-with-other-stac-clients).
- EPSG:3857 COG items expose standard Web Map Links `xyz` links for generic viewers. See [compatibility checks](worker/test/STAC_COMPATIBILITY.md).

**Web viewers (in `worker/static/`):**

- Vector tile viewer using [MapLibre GL JS](https://maplibre.org/) — layer selection, feature inspection, styling, 3D terrain with altitude shown on click, geocoding, and partial data download from remote parquet sources in multiple formats using [geoparquet-extractor](https://github.com/ramSeraph/geoparquet-extractor)
- Raster tile viewer for raster/COG data
- STAC viewer using [Leaflet](https://leafletjs.com/) for browsing STAC collections
- COG viewer for on-demand COG visualization
- No build step — browser-native ES modules with dependencies loaded from [esm.sh](https://esm.sh/)

**Other endpoints:**

- `/api/routes` — lists all available tile sources
- `/cog-info` and `/cog-tiles/{z}/{x}/{y}` — COG metadata and tiles

**Running locally:**
```bash
cd worker
npm install
npm run dev  # http://localhost:8787
```

To override the Protomaps browser key locally, set `PROTOMAPS_API_KEY` in
`worker/.dev.vars`. This local configuration file is ignored by Git.

Run `npm test` from `worker/` for the offline Worker tests.

**Worker logging:**

`LOG_LEVEL` in `worker/wrangler.toml` defaults to `error`. Missing or invalid
values also use `error`. Set it and redeploy to change production logging;
use `worker/.dev.vars` for local overrides.

| Level | Diagnostic logs retained |
| --- | --- |
| `off` | None |
| `error` | Handler failures and failed upstream HTTP/network attempts, including retries |
| `warn` | Errors plus slow upstream fetches and source refresh warnings |
| `info` | Warnings plus every successful upstream fetch |
| `debug` | Info plus incoming request header diagnostics |

Separately, `ACCESS_LOG_SAMPLE_RATE = "0.01"` samples 1% of requests handled by
the Worker, regardless of response status. Use `"0"` to disable access logs or
`"1"` to log every request. Missing, empty or invalid rates default to 0.01.
`LOG_LEVEL = "off"` disables both diagnostics and access logs. Other severity
levels do not change the access sample or sample out errors.

Access records have `event: "access"`, `requestId`, `method`, `path`, `status`,
`referer`, `refererHost`, `elapsedMs` (through response creation, not body transfer),
and `sampleRate`. Request paths omit queries; referrers omit credentials, queries
and fragments. Missing, malformed or non-HTTP referrers become null. Only sampled
requests produce access records; normal validation errors and missing tiles are
included in the same sample. Requests served directly by Cloudflare Assets
without invoking the Worker are outside this sample.

In Workers Logs, filter `event = access`, group by `refererHost` or `path`, and
count records. At the default rate, multiply counts by 100 for estimated requests;
if the rate changes, weight each record by `1 / sampleRate`. These are request
estimates, not visitor counts, and low-volume referrers may be missed. Ten million
Worker requests produce about 100,000 access records plus diagnostic error logs.

All application logs use the shared logger; levels are request-scoped so cached
handlers cannot retain another request's setting. Upstream records retain request
IDs, status, range and retry information, without signed queries or credentials.

Cloudflare's automatic per-request invocation logs are disabled with
`observability.logs.invocation_logs = false`. Log collection stays enabled at
`head_sampling_rate = 1` so application errors are not deliberately sampled out.
`LOG_LEVEL = "off"` suppresses our application logs; to stop Workers Logs
collection entirely (including runtime/dependency logs), set
`observability.logs.enabled = false` and redeploy. Raising verbosity or enabling
invocation logs should be temporary while diagnosing a problem.

From 1 December 2026, [Cloudflare Observability pricing](https://developers.cloudflare.com/observability/pricing/)
meters uncompressed ingestion (including enriched metadata) and stored GB-month,
with shared account allowances. Monitor Billable Usage after deploying; filtering
the log viewer does not reduce ingestion. Keep the default seven-day retention
and avoid enabling extra trace/log datasets unless needed.

### Python CLI — `iomaps` (`python/`)

A Python package ([available on PyPI](https://pypi.org/project/iomaps/)) providing CLI tools for working with the data.

**Commands:**

- `iomaps cli stac` — discover raster collections and extract imagery by bounding box
- `iomaps cli extract` — extract features from remote GeoParquet by polygon or bounding box
- `iomaps cli sources` — list available remote data sources (with category filtering and fuzzy matching)
- `iomaps cli search --bounds "77.1,28.5,77.3,28.7"` — find sources covering an area
- `iomaps cli source-categories` — list available data categories
- `iomaps ui` — launch a PyQt5 GUI for remote Parquet extraction

**Example usage:**
```bash
pip install iomaps
iomaps cli sources                    # list available sources
iomaps cli sources -c cadastral       # filter by category
iomaps cli extract -s buildings -o buildings.gpkg -b "77.5,12.9,77.7,13.1"
```

See the [Python documentation](python/README.md) for installation options,
Python API examples, and detailed command usage.

