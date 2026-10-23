# Repository instructions

This repository contains a geospatial tile server and web viewer for Indian map data. It serves vector and raster tiles from PMTiles archives and Cloud-Optimized GeoTIFFs (COGs), provides a STAC API for data discovery, and supports client-side data downloads in multiple formats.

## Dataset preparation and publication

Use the repository's existing tools when adding datasets. The user specifically
prefers this workflow over manual route edits, individual uploads, or new publishing
wrappers.

### Existing tools

Run from the repository root using the checkout's Python environment:

```bash
python/.venv/bin/iomaps admin prepare --help
python/.venv/bin/iomaps admin make-release --help
```

The `admin` group is hidden from the top-level help. Its implementations are
`python/iomaps/commands/admin_prepare.py` and `admin_release.py`; these replace the
older `utils/prepare.py` and `utils/make_release.py` scripts. Setup and command
examples are in [python/README.md](python/README.md#preparing-and-publishing-datasets).

`prepare` creates the compressed original GeoJSONL, PMTiles, GeoParquet, and ID
analysis. It also handles files that need splitting. `make-release` uploads assets,
updates the dataset's release notes section, then commits and pushes the route
through the local checkout. It verifies GitHub content, the Cloudflare build, and
live endpoints, and saves a receipt. Let the command handle the whole publication.
The implementation is split between `admin_release.py` (CLI and plans),
`admin_publish.py` (Git and releases), and `admin_verify.py` (live checks).

### Sync before local work

The route publisher handles synchronization itself: `make-release` fetches
`origin`, checks local history, and fast-forwards when possible during route
preflight and again immediately before the route commit. Do not add a separate
manual `git fetch origin` step just to publish a route.

For separate tooling commits or work that needs the current listing before the
publisher runs, fetch and compare the local branch with upstream. The old
publisher committed routes through GitHub's API, bypassing the checkout and
leaving both local `main` and cached `origin/main` behind. Other contributors can
still advance the remote, so do not assume cached history is current.

```bash
git status --short --branch
git fetch origin
git log --oneline HEAD..origin/main
git diff --name-only HEAD origin/main
git merge --ff-only origin/main
```

Inspect incoming changes and preserve existing work. A fast-forward can retain
unrelated local edits, as it did when syncing the Delhi route. If histories have
diverged or upstream overlaps local changes, resolve that deliberately without
discarding edits. Sync first, then commit local tooling or documentation changes.
Push authorized local commits before publishing a dataset. The publisher refuses
unrelated unpushed commits or divergent history, rather than pushing them implicitly.
It allows unrelated staged and unstaged edits, and commits only the listing.
Existing edits to the listing must be resolved before publication.

### Workflow

1. Let the publisher sync route changes as described above; sync manually for
   separate local commits or when the current listing is needed earlier. Read similar entries in
   `worker/src/routes/listing.json`, their Git history,
   and the existing data release. Follow their repository, release tag, filename,
   category, and metadata conventions. Confirm an existing release before
   proposing a new one. Check GitHub too if the local checkout is behind.
2. Choose the final basename first. Rename the working GeoJSONL copy before
   running `prepare`, so the archive member, tile layer, and derived filenames
   agree. Keep working data in ignored `scratch/<dataset>/` and preserve the
   supplied original. If the basename changes, regenerate from the renamed input.
3. Run `prepare` with the source URL and source name. Validate feature counts,
   coordinates, properties, and the ID field; compare with the source service
   where possible. Review any fixes or dropped features reported by the tool.
   Record source geometry problems rather than silently changing them. Check that
   the chosen ID exists in the PMTiles too: GeoParquet conversion may introduce
   `OGC_FID`, which is absent from the tiles. Use `--promoteid` for the source ID.
4. Use `make-release --dry-run --owner <owner>` with all intended publication
   arguments. This is an offline JSON preview containing the exact filenames,
   destinations, route entry, license Markdown, release-note Markdown, and asset
   SHA-256 hashes. Save it under `scratch/` for review. Show the actual license
   text, not a paraphrase. Old previews without a schema version/hash manifest
   must be regenerated; they cannot be executed.
5. When the user requests review, finish preparation and show the concrete preview
   before publishing. Once publication is authorized, execute that exact preview:
   `python/.venv/bin/iomaps admin make-release --plan scratch/<dataset>/release-preview.json`.
   Do not ask for the same approval again. Dataset flags cannot override a saved
   plan. Use `--checkout /path/to/indianopenmaps` if running outside the checkout.
   Do not manually add the listing entry or implement another publishing workflow.
6. Check the generated `release-preview.receipt.json`. The command verifies asset
   hashes, notes, the remote route, the Cloudflare build, live `/api/routes`, route
   `tiles.json`, and a sample tile against the local archive. `--verify-timeout`
   controls the deployment wait. `--no-verify` explicitly skips live checks, so do
   not call the dataset live based on that run. It still checks GitHub content.

**`--no-uploads` is not a dry run.** It requires all planned assets to exist with
matching hashes, then still updates notes and commits/pushes routes.

Publication is not atomic. After interruption, rerun the same `--plan` with the
same receipt path. Matching assets are verified and skipped, notes are replaced
within stable dataset boundaries, and a recorded unpushed route commit can be
retried. Differing assets are never overwritten. An incomplete remote asset or
conflicting local work needs inspection. If intentionally changing a reviewed
plan after an attempt, review it again and use a new `--receipt` path.

### Remote preparation with local route commits

For datasets that do not fit locally, use the same tools on the data server.
Keep the working data, `TMPDIR`, and caches on its data volume, and run long
preparation jobs durably with logs. Check permissions and download completeness
first. Stabilize the input before processing; label incomplete snapshots when
the user has authorized publishing them. Preserve the source and choose the
published basename before preparation. A hard link saves space only when the
source cannot be modified for the duration of the job.

Run `make-release --plan ... --phase assets` on the server, with verification
enabled. This needs data-repository credentials but no Git checkout or route
push credentials. Copy only the exact plan and its `.receipt.json` to this
checkout, then run `make-release --plan ... --phase routes` here. Do not rewrite
the asset paths or copy large assets locally. The local phase validates remote
hashes/notes again, commits and pushes the listing through local Git, and verifies
the live tile using the checksum in the server receipt. See the Python README
for headless module invocations and examples. Keep credentials out of logs and
handoff files; do not copy local SSH keys to the server.

When disk space is tight or PMTiles is still running, upload ready files first
with `make-release --phase uploads --asset <file> ... --dry-run`, save the batch
plan, then execute it with `--phase uploads --plan <batch.json>`. This phase only
uploads/verifies files: it does not change release notes or routes. Keep its plan
and receipt. Remove local data only after the receipt reaches `uploads_complete`
and GitHub sizes and SHA-256 hashes match. Keep source/filtered input needed by
unfinished preparation stages. Generate the final publication preview with
`--uploaded-plan <batch.json>` for each batch; the publisher includes their
manifests, requires matching remote assets, and does not need their local copies.
Keep PMTiles locally through the final asset phase so it can capture a sample.

On small-memory servers, use `prepare --bounded-parquet --parquet-memory-limit
256MB` for large 2D WGS84 GeoJSONL inputs. The pinned `geoparquet-io==0.9.0`
converter repeatedly reads GeoJSONL and eventually fetches the full result into
an Arrow table; it was OOM-killed on the 30-million-feature Rajasthan input with
1.9 GiB RAM. The bounded path uses a disk-backed DuckDB table, an external spatial
sort and direct Parquet writes, including splitting and ID analysis with the
memory limit applied. The DuckDB limit is not a process-wide RAM cap: the first
bounded retry also OOM-killed with a 768 MB limit, ZSTD 22 and 100,000-row groups.
The bounded writer now uses ZSTD 9 and 8,192-row groups. Test the writing stage
with a representative volume (the 1,000-feature smoke test missed the large-write
failure). A completed import can be resumed with `--parquet-resume-db` and its
source identity/count checkpoint; keep the database after a failed write instead
of rescanning GeoJSONL. This requires `--no-filter --no-zip --no-pmtiles`.
Set `TMPDIR` on the data volume; leave room for the spool,
sort spill and output in addition to the source. Watch timestamped stage logs and
check `dmesg` after unexplained exit 137; sustained CPU/read activity alone does
not establish that the next stage can fit in memory.

For a Tippecanoe tile-size failure, test the failed tile with `--one-tile=z/x/y`
before restarting the complete dataset. `prepare --tile-reduction drop-densest`
or `drop-smallest` overrides coalescing, and repeated `--tippecanoe-option=...`
arguments permit zoom limits and progress logging. Rajasthan's coalescing failed
at 10/719/425; dropping densest features passed a 329,483-feature pilot, preserving
8,280 tile features in 481,093 bytes. Its retry starts at zoom 13 with at most
three additional zooms. Check decoded tile IDs: `--use-attribute-for-id` removes
that property. Provide both GeoJSON `id` and `properties.OGC_FID` when both are
needed. For Rajasthan, a FIFO streams the same GDAL reader's FIDs into prepare,
avoiding an extra input copy. A 1,000-feature comparison with published Parquet
and an end-to-end FIFO/PMTiles smoke test both passed. Current run details live in
ignored `scratch/rajasthan-cadastrals/session.json`; keep the filtered source until
all final outputs are verified.

Use the preparation module's `TWO_GB` (2 GiB = 2,147,483,648 bytes) for release
asset size checks. The `7z -v2000m` volume size is smaller and is not GitHub's
limit. Rajasthan's completed PMTiles passed standard verification; its scratch
wrapper initially rejected valid partitions by applying that smaller threshold.

### Authentication

The release utility reads `GH_TOKEN`, then `GITHUB_TOKEN`, then
`.secrets/gh_token.txt` or the legacy `utils/token.txt` in the working directory or
enclosing checkout. The repo token and `gh` CLI login can differ. Never print or
commit credentials. Use `--owner ramSeraph` explicitly for this project's releases.
The token needs Contents read/write on the data repository and read access to the
route repository for verification. Route pushes use the checkout's existing Git
credentials and need push access to `origin`. Successful authentication alone does
not prove upload/push permissions. A sandbox DNS failure is not an invalid token.

### City land-use precedent

The Tamil Nadu DTCP Master Plan is the template used for Delhi:

| Field | Delhi precedent |
| --- | --- |
| Data repository | `ramSeraph/indian_land_features` |
| Existing release | `urban-land-use` |
| Template basename | `TNGIS_DTCP_Master_Plan` |
| Delhi basename | `DDA_Delhi_Master_Plan` |
| Name | `DDA Delhi Land Use Plan` |
| Route | `/not-so-open/urban/regulation/landuse/delhi/dda/` |
| Categories | `landuse`, `delhi` |
| Promote ID | `fid` (`objectid` repeats in the Delhi source) |

Use `regulation` for new routes. The Tamil Nadu route's historical `regultion`
spelling should not be copied; changing an existing published URL is a separate task.
Delhi's 4,722 features were published successfully; do not republish that dataset
just to test tooling. Its source is
https://gis.dda.org.in/server/rest/services/Hosted/MAP_1_LANDUSE_PLAN_FOR_DELHI_UPDATED/FeatureServer/9.

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
- All dependencies injected at construction (no late `setMap()` calls)
- Constructors with 3+ params use destructured config objects for readability
- Modular architecture with separate handlers:
  - `base_layer_picker.js` - Base map selection (Carto/OSM/ESRI/Google + custom raster sources)
  - `vector_source_handler.js` - Vector layer management (MVT tiles, fill-extrusion for 3D buildings); `#selectedSources` is private with accessor methods (`selectedSourceCount`, `hasSource()`, `getSourceData()`, `getSelectedEntries()`, `getSelectedPaths()`, `getPathBySourceName()`, `getColorForPath()`)
  - `color_handler.js` - Layer color palette (12 colors, session memory via sessionStorage)
  - `terrain_handler.js` - 3D terrain using CartoDEM v3r1
  - `search_param_handler.js` - URL parameter state (query & hash params)
  - `source_panel_control.js` - Vector source selector panel with category filtering; extends `EventTarget`, dispatches `'sourcechange'` events
  - `sidebar_control.js` - Modular sidebar with icon buttons (search, layers, sources, downloads)
  - `inspect_control.js` - Feature inspection popups with HTML-escaped property display
  - `nominatim_geocoder.js` - Nominatim search adapter for MapLibre geocoder
  - `routes_handler.js` - Fetches `/api/routes`, separates raster vs vector sources
  - `size_getter.js` - File size estimation utilities
  - `extent_handler.js` - Visualizes data extents and row group extents as map rectangles; extends `EventTarget`, dispatches `'loadingchange'` CustomEvents

### Download System (`js/download_panel_control.js` + related)
- Multi-format download UI with spatial filtering (bbox or polygon)
- Split into two focused classes:
  - `download_panel_control.js` - Source dropdown, source info display, full download links
  - `partial_download_ui.js` - Partial download UI: extents checkbox, bbox display, format picker, memory slider, download execution, and progress tracking
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
- `css/viewer.css` - All viewer styles (sidebar, panels, map controls, source panel, download panel, category filters)
- `css/raster_view.css` - Dual-map raster viewer styles
- `viewer.html` is minimal (~29 lines) — just head with stylesheet links and body with map div

## Code Style & Conventions

### JavaScript
- Use ES modules (`import/export`) in both worker and frontend code
- Prefer `async/await` over callbacks
- Use `const`/`let`, avoid `var`
- Error handling: throw custom error classes from `errors.js`
- **Consistency**: Before introducing a new pattern, check how similar problems are already solved in the codebase. Follow existing conventions.
- **Single source of truth**: When splitting code, ensure constants and domain knowledge live in the owning module and are imported by consumers. Don't duplicate definitions across files.
- **Post-split review**: After extracting or splitting code, review the result for: duplicated state or constants across files, callbacks that could be replaced by pushing state down, and data/logic that ended up in the wrong module based on responsibility. Fix these before committing.

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
- Constructors with 3+ params use destructured config objects: `new Foo({ map, handler })` not `new Foo(map, handler)`
- Dependencies injected at construction time — no late `setMap()` pattern
- Inter-component communication via `EventTarget` (`addEventListener`/`dispatchEvent`), not callbacks
- Use JS `#private` class fields for internal state (e.g., `#selectedSources` in VectorSourceHandler)
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
- `test_cli_extract.py` - Remote Parquet queries, partitions, and export
- `test_coverage.py` - Coverage geometry, PNG output, and preparation integration
- `test_coverage_index.py` - Cached bbox lookup, ETags, bounded downloads, and offline reads
- `test_cli_commands.py` - CLI discovery and remote extraction UI
- `test_admin_*.py` - Preparation, publication plans, receipts, and verification
- Run with: `python/.venv/bin/pytest python/tests`

## Tile source configuration

The publisher manages entries in `worker/src/routes/listing.json`; use the
preparation and publication workflow above for dataset additions. Entries contain
`url`, `type` (`vector` or `raster`), `handlertype` (`pmtiles` or `mosaic`),
`datameet_attribution`, and optional `category`, `promoteid`, and `tilesuffix`.
The source is automatically available at its `/{key}/` route.

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
- **`utils/`**: Utility scripts (check_endpoints.js, validate_bboxes.py, etc.)
- **`tests/`**: Integration tests for tile serving and COG processing
- **`plans/`**: Planning documents

---

## Python Package (`python/`)

The `iomaps` package provides CLI and PyQt tools for remote GeoParquet extraction,
plus a hidden admin group for preparation, coverage creation, and publication.
Python 3.12 or newer is required; Hatchling and hatch-vcs provide packaging.

### CLI structure

```text
iomaps
├── cli
│   ├── extract               # Remote GeoParquet extraction
│   ├── sources               # Source listing and search
│   ├── search                # Find sources covering a bounding box
│   └── source-categories     # Categories with source counts
├── admin                     # Hidden from top-level help
│   ├── coverage              # Local GeoParquet to 1-bit coverage PNG
│   ├── prepare               # Prepare dataset assets
│   └── make-release          # Plan, publish, and verify a dataset
└── ui                        # Remote extraction GUI
```

### Modules and dependencies

- `commands/sources.py`: catalog fetching, category/search filters, and stable route lookup.
- `commands/search.py` and `core/coverage_index.py`: bbox discovery with cached PNGs,
  explicit ETag refresh, and a combined memory-mapped bitmask index. SQLite stores
  original downloads and ETags only; warm lookups access neither SQLite nor HTTP.
- `commands/extract.py`: DuckDB spatial and HTTP range reads, partition/row-group
  pruning, exact intersections, and format exports via GDAL or geoparquet-io.
  Matching features retain their full geometry.
- `commands/decorators.py`: shared bounds, polygon-file, output, and catalog options.
- `commands/pyqt_ui.py`: PyQt5 remote source selection and extraction with worker threads.
- `core/spatial_filter.py`: bounds parsing and polygon selection from filter files.
- `core/helpers.py`: supported output drivers and filename-to-driver mapping.
- `core/coverage.py` and `commands/admin_coverage.py`: batched WKB coverage on a fixed
  India grid; 1-bit PNG, north at the top, no coverage manifest.
- `commands/admin_prepare.py` and `admin_parquet.py`: asset generation and bounded
  GeoParquet preparation; coverage is on by default, using final Parquet partitions
  (`--no-coverage` opts out). Publication automatically collects the coverage PNG.
- `commands/admin_release.py`, `admin_publish.py`, and `admin_verify.py`: saved
  publication plans, uploads, route commits, receipts, and live checks.

Use the dataset preparation workflow above and the examples in `python/README.md`.
The legacy 7z consumer commands/UI and Python archive-reading dependencies are
removed. Admin preparation still invokes external `7z`, and publication supports
those archives. Keep this distinction when changing dependencies.

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
uv run iomaps cli sources                    # List available remote sources
uv run iomaps cli sources -c cadastral       # Filter by category
uv run iomaps cli source-categories          # List all categories with counts
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
