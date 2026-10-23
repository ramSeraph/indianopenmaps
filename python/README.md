# Indianopenmaps [![PyPI - Latest Version](https://img.shields.io/pypi/v/iomaps)](https://pypi.org/project/iomaps/)

This project provides command-line tools and a PyQt UI for extracting geographical data from Indianopenmaps remote GeoParquet sources, plus admin tools for dataset preparation, coverage, and publication.

Maintainers adding a dataset should use the existing
[preparation and publication commands](#preparing-and-publishing-datasets).

## Installation

The `iomaps` package is available on PyPI. You can install it using `pip` or `uv`.

1.  **Using `pip`:**
    ```bash
    pip install iomaps
    ```

2.  **Using `uv` (Recommended):**
    If you have `uv` installed, you can use it to install `iomaps` and manage its dependencies.

    a.  **Install `uv`:**
        https://docs.astral.sh/uv/getting-started/installation/

    b.  **Install `iomaps`:**
        ```bash
        uv pip install iomaps
        ```
        *Note: This step is not needed if you are using `uvx` to run commands, as `uvx` handles dependency management automatically.*

## Usage

The `iomaps` package provides a command-line interface (CLI) with several subcommands.

### Running with `uvx` (Recommended)

`uvx` is the easiest and recommended way to run the `indianopenmaps` CLI. It automatically manages virtual environments for you, ensuring that the correct dependencies are installed and activated without manual intervention. This means you don't need to explicitly create or activate a virtual environment.

To run commands using `uvx`:
```bash
uvx iomaps <command>
```
For example:
```bash
uvx iomaps cli extract -s "SOI States" -o states.gpkg -b "70,10,90,30"
```

### Running with `pip`

If you prefer to use `pip` and manage your virtual environments manually, follow these steps:

1.  **Create and activate a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

2.  **Install dependencies:**
    ```bash
    pip install -e .
    ```

3.  **Run commands:**
    Once the virtual environment is activated and dependencies are installed, you can run the `iomaps` CLI directly:
    ```bash
    iomaps cli extract -s "SOI States" -o states.gpkg -b "70,10,90,30"
    ```
    Remember to activate the virtual environment (`source .venv/bin/activate`) each time you open a new terminal session before running `iomaps` commands.

---

## Extracting from Remote Geoparquet Files

These commands allow you to extract and filter data directly from remote geoparquet sources hosted on the indianopenmaps server without downloading the entire files.

### 1. `iomaps cli extract`

The checkout uses DuckDB 2.0 or newer for asynchronous remote Parquet reads and
connection pooling. HTTPS certificate verification is enabled. The default
uses DuckDB's selected transport; `--http-client curl` and `--http-client httplib`
are available for comparison. Spatial filtering uses the GeoParquet covering
columns to prune row groups, followed by the exact geometry intersection.
Intersecting features from every matching partition are retained in full.
GDAL exports explicitly use longitude/easting before latitude/northing.

On two SOI sheets, DuckDB 2.0 returned the same features and coordinates as the
earlier extraction. It needs no community HTTP extension. At the time of testing,
the community `cache_httpfs` and `curl_httpfs` builds were not yet available for
DuckDB 2.0 on macOS ARM64. The complete source Parquet files are not downloaded
as a preprocessing step; the query reads remote ranges and can still transfer
large row groups when the source layout requires it.

Extract and filter data from a remote geoparquet source.

Sources are resolved by fetching the list from the indianopenmaps API. The command fetches geoparquet files and applies spatial filters.

Sources can be specified by:
- Name (e.g., "SOI States")
- Route starting with / (e.g., "/states/soi/")
- Number from 'iomaps cli sources' list (e.g., "1", "2")

**Options:**

*   `-s, --source <source>` (Required): Source to extract from. Can be a name, route (starting with /), or number from 'iomaps cli sources'.
*   `-o, --output-file <path>` (Required): Path for the output file where processed data will be saved.
*   `-d, --output-driver <driver>`: Specify output driver (choices: `csv`, `esri shapefile`, `flatgeobuf`, `geojson`, `geojsonseq`, `gpkg`, `parquet`). If not specified, it will be inferred from the output file extension.
*   `-l, --log-level <level>`: Set the logging level (choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; default: `INFO`).
*   `-f, --filter-file <path>`: Path to an input filter file (e.g., a shapefile) for spatial filtering.
*   `--filter-file-driver <driver>`: Specify the OGR driver for the filter file. If not specified, it will be inferred from the filter file extension.
*   `-b, --bounds <min_lon,min_lat,max_lon,max_lat>`: Rectangular bounds for spatial filtering. You can get bounding box coordinates from [http://bboxfinder.com/](http://bboxfinder.com/).
*   `--pick-filter-feature-id <id>`: Select a specific polygon feature by its 0-based index.
*   `--pick-filter-feature-kv <key=value>`: Select a specific polygon feature by a key-value pair (e.g., 'key=value') from its properties.
*   `--routes-file <path>`: Path to local JSON file with routes. Cannot be used with --routes-url.
*   `--routes-url <url>`: URL to fetch routes from. Cannot be used with --routes-file. Default: https://indianopenmaps.com/api/routes
*   `--http-client <default|curl|httplib>`: Override DuckDB's HTTP client (default: `default`).

**Example:**

```bash
uvx iomaps cli extract -s "SOI States" -o states.gpkg -b "70,10,90,30"
```

### 2. `iomaps cli sources`

List available data sources for the extract command.

Sources can be referenced by number, route, or name when using the extract command's --source flag.

**Options:**

*   `-c, --category <category>`: Filter by category (can be specified multiple times).
*   `-s, --search <text>`: Search sources by name or route (fuzzy matching with typo tolerance).
*   `--json`: Output as JSON.
*   `-l, --log-level <level>`: Set the logging level.
*   `--routes-file <path>`: Path to local JSON file with routes.
*   `--routes-url <url>`: URL to fetch routes from.

**Examples:**

```bash
uvx iomaps cli sources                      # List all sources
uvx iomaps cli sources -c buildings         # Filter by category
uvx iomaps cli sources -s districts         # Search for "districts"
uvx iomaps cli sources -c states -s soi     # Combine filters
```

### Bbox source discovery: `iomaps cli search`

```bash
python/.venv/bin/iomaps cli search --bounds '77.1,28.5,77.3,28.7' --json
python/.venv/bin/iomaps cli search --bounds '77.1,28.5,77.3,28.7' --category landuse
python/.venv/bin/iomaps cli search --bounds '77.1,28.5,77.3,28.7' --force-update
```

Search returns sources occupying any grid cell touched by the bbox. These are
approximate candidates; use each returned `route` with `cli extract --source` to
retrieve actual features. No exact geometry check runs during discovery.
`--category` can be repeated, and `--search` filters names/routes with fuzzy matching.
Numeric catalog IDs are not returned because they can change between catalogs.

First use caches the catalog and downloads coverage PNGs with eight workers
(`--workers 1..32`), reusing completed downloads after interruption. Later searches
read only a local bitmask index and its embedded source registry: **no network
requests, update checks, or SQLite access**. Refresh explicitly with
`--force-update` (alias `--refresh`); this fetches the catalog and PNGs using
`If-None-Match` where ETags are available. A 304 reuses the original bytes.
Transient errors or invalid PNGs leave the previous index intact.

The platform cache directory contains a separate cache for each catalog. Override
it with `--cache-dir` or `IOMAPS_CACHE_DIR`. `--routes-file` selects a local catalog;
`--routes-url` selects another endpoint. A warm local-catalog search also works
without the original catalog file. Only cache creation/refresh opens SQLite,
which stores original compressed PNG bytes and ETags, not spatial cells. The
combined index uses packed source bits and memory-mapped reads; 556 sources at
1,000 × 1,000 cells take about 70 MB plus their source registry.

Coverage names use the common Parquet basename: `Plots.parquet` or
`Plots.parquet.meta.json` maps to `Plots.coverage.png`. For the normal catalog,
the basename is derived from the corresponding `Plots.pmtiles` or
`Plots.mosaic.json` URL, beside the same release assets. A custom catalog can set
`coverage_url`, including a relative local filename. All PNGs in one catalog
must have the same dimensions and use the fixed India bounds. No remote coverage
manifest is required.

JSON stdout is an object with `matches`, `unknown`, `bounds`, `effective_bounds`,
`grid`, `cached_at`, and `coverage`. Status messages go to stderr. The `coverage`
counts describe the whole cached catalog; category/text filters apply to both
result lists. Missing PNGs (HTTP 404/410) are **unknown coverage**, not empty
coverage, and remain visible until an explicit refresh. Existing datasets need
their PNGs backfilled before they can participate in spatial matching.
Queries partly outside **67,5,98,38** search their overlap and report that scope;
queries wholly outside fail. An empty result does not establish absence in sources
with unknown coverage.

### 3. `iomaps cli source-categories`

List available source categories.

Shows all categories with the number of sources in each. Use with 'iomaps cli sources -c <category>' to filter sources.

**Options:**

*   `--json`: Output as JSON.
*   `-l, --log-level <level>`: Set the logging level.
*   `--routes-file <path>`: Path to local JSON file with routes.
*   `--routes-url <url>`: URL to fetch routes from.

**Example:**

```bash
uvx iomaps cli source-categories
```

---

## Remote extraction UI

`iomaps ui` opens a PyQt interface to select a remote source, optionally filter by
bounding box, and export the matching features.

```bash
uvx iomaps ui
```

The legacy `filter-7z` and `infer-schema` commands and their UI views have been
removed. Dataset preparation and publication still support original GeoJSONL
archives, including split `.7z` volumes.

## Preparing and publishing datasets

Use the checkout's hidden `admin` command group to prepare data and publish it.
These commands replace the older `utils/prepare.py` and `utils/make_release.py`
scripts. Run the following commands from the repository root.

Install the admin dependencies if the local environment does not already have them:

```bash
uv sync --project python --extra admin
```

Preparation also needs `7z`, `tippecanoe`, `pmtiles`, and `uvx` on `PATH`.
Check the existing installation before downloading another environment.
The regular converter runs `geoparquet-io==0.9.0` with `duckdb==1.5.6` in an
isolated `uvx` environment: this GPIO version uses Arrow APIs removed in DuckDB 2.
The checkout keeps its own DuckDB version. Output remains GeoParquet 1.1; the
converter package version and output format version are independent.

### Prepare a dataset

Route publication synchronizes the checkout automatically: the publisher fetches
upstream, checks history, and fast-forwards when possible. A separate manual
`git fetch origin` is not needed for route publication. For separate tooling
commits or work that needs the latest listing before publication, fetch and inspect
incoming changes first, preserving local edits and resolving divergence. Older
versions committed routes directly through GitHub's API and left the checkout
behind; see [AGENTS.md](../AGENTS.md#sync-before-local-work).

Find a similar existing listing and GitHub release first. Choose the final basename
and rename a working copy of the input GeoJSONL before processing. For example,
Delhi follows `TNGIS_DTCP_Master_Plan` with `DDA_Delhi_Master_Plan`. Preparation uses
this basename for the archive member, tile layer, and output files.

```bash
python/.venv/bin/iomaps admin prepare \
  /absolute/path/Authority_City_Master_Plan.geojsonl \
  'https://source.example/layer' \
  'Source authority'
```

The command writes the original `.geojsonl.7z`, `.pmtiles`, `.parquet`, and
`.analysis.json` beside the input, with partitions and metadata for large datasets.
The `.mbtiles` file is an intermediate. The archive preserves the original input;
tiles and GeoParquet use the preparation tool's cleaned data. Review the reported
fixes/drops and validate feature counts and ID uniqueness before publishing.

Preparation validates coordinates against worldwide WGS84 limits: longitude
`[-180, 180]`, latitude `[-90, 90]`. Valid features outside India are retained
unchanged. Cleaning reports and drops malformed or non-finite coordinates;
India-based decimal/swap repairs are attempted only for out-of-range values.
`--no-india` disables those repairs, not coordinate validation.

`--no-filter` skips cleaning but still validates coordinates before accepting
GeoParquet. The regular path checks the source and stages/validates converter
output before replacing the final file. The bounded path validates every XY
vertex in batches from its imported table, including resumed imports, without
rescanning the source GeoJSONL. A failed validation preserves the source and any
previous final Parquet. These checks also run with `--no-coverage`.

For a tile-size failure, first test the offending tile with Tippecanoe's
`--one-tile=z/x/y`. `prepare --tile-reduction drop-densest` (or `drop-smallest`)
replaces the default polygon coalescing strategy. Explicit dropping also removes
adaptive gamma; existing defaults are preserved when this option is omitted.
Use repeated `--tippecanoe-option=--option=value` arguments for advanced settings,
for example `--tippecanoe-option=--extend-zooms-if-still-dropping-maximum=3`.
Dropping affects displayed tile features, so retain the complete downloadable
formats and document the chosen settings. Bound zoom extensions deliberately.

Check IDs in decoded tiles, not just Parquet analysis. Tippecanoe's
`--use-attribute-for-id` consumes the property. To retain a property for the route's
`promoteid` as well as a native tile ID, provide both GeoJSON `id` and the property
in the input instead. Generated GDAL `OGC_FID` values can be reproduced by streaming
the same unchanged input through the same GDAL reader used for Parquet; verify
sample IDs, properties and geometries against the published Parquet first.
The chosen ID must also exist in the tiles; conversion can add an `OGC_FID` field
only to GeoParquet. Use `--promoteid` to select a suitable source field when needed.

### Create source coverage

Create a dataset's coverage image from its final local GeoParquet files:

```bash
python/.venv/bin/iomaps admin coverage \
  scratch/dataset/Dataset.parquet \
  --output scratch/dataset/Dataset.coverage.png
```

For a split dataset, pass every Parquet partition, or its existing
`Dataset.parquet.meta.json`; the command resolves all listed partitions beside
that metadata file and combines their coverage. A missing partition fails the
command. `admin prepare` writes `<basename>.coverage.png` by default after Parquet
creation and partitioning. Use `--no-coverage` to skip it. `--no-parquet` also skips
coverage; explicitly requesting both `--coverage` and `--no-parquet` is an error.

The output is a standalone **1-bit PNG**, with white for occupied cells and black
for empty cells. It contains no manifest, provenance, or embedded metadata.
`iomaps` defines the bounds as **67,5,98,38** in longitude/latitude. Image columns
run west to east, and row zero is north. The default image is 1,000 by 1,000 pixels,
roughly 3.2 by 3.7 km per cell near central India. `--grid-size N` changes the image
to N by N pixels with the same bounds; preparation uses `--coverage-grid-size N`.

Coverage uses actual feature geometry, including polygon interiors, narrow lines,
and cells touched at shared edges or corners. It is approximate at cell resolution:
a source touching a query's cells may have no features inside the exact query bbox.
`make-release` automatically includes `<basename>.coverage.png` when present,
including one common image for all partitions. The PNG appears in the saved plan,
is uploaded beside the Parquet, and receives the same hash verification as other
assets. Earlier datasets without PNGs remain publishable and appear as unknown in
`cli search` until coverage is uploaded.

Inputs must be GeoParquet 1.0 or 1.1 with WKB and planar WGS84 coordinates
(`OGC:CRS84` or `EPSG:4326`, with longitude first). Omitted CRS metadata defaults
to CRS84 per GeoParquet; explicitly unknown CRS, projected data and spherical edges
are unsupported. Malformed WKB, unusable coordinates and geometry-engine failures
produce warnings and skip only the affected features. Warnings identify the source
file, count and first affected row; `skipped_geometries` records their total.
Invalid shapes with usable coordinates mark their own bounding-box cells and
produce a warning with the source file and first affected row in each batch.
This can add false positives; it does not modify or repair the Parquet geometry.
Reproject other inputs before use. Null/empty geometries occupy no cells. Features
entirely outside the India bounds contribute nothing; crossing features contribute
only within the bounds. This is the bounding rectangle, not India's country boundary.
Z/M values do not affect the 2D coverage.

Only the geometry column is read, in batches of 8,192 rows. Use `--batch-size` to
adjust this for existing Parquet; a batch limit does not bound the size of an
individual geometry or Parquet decoder buffers. The grid uses approximately one
byte per cell while building. PNG packing uses one bit per pixel, with each row
padded to a byte and then compressed losslessly. Existing output is preserved on
failure; `admin coverage --overwrite` replaces it only after successful creation.
The standalone command can also run headlessly as
`python -m iomaps.commands.admin_coverage`.

### Preview the exact publication

Substitute the new dataset's filenames and metadata in this example. The
`urban-land-use` release already exists in `ramSeraph/indian_land_features`.

```bash
mkdir -p scratch/new-city
python/.venv/bin/iomaps admin make-release \
  --owner ramSeraph \
  --repo indian_land_features \
  --release urban-land-use \
  --base-file /absolute/path/Authority_City_Master_Plan.geojsonl.7z \
  --description 'Authority City Master Plan' \
  --source 'Source authority' \
  --source-url 'https://source.example/layer' \
  --route /not-so-open/urban/regulation/landuse/city/authority/ \
  --category landuse --category city \
  --promoteid fid \
  --dry-run > scratch/new-city/release-preview.json
```

`--dry-run` needs an explicit `--owner` and `--release`. It reads local assets and
prints JSON without reading credentials or accessing GitHub. The JSON contains
the asset paths, sizes and SHA-256 hashes, both repository destinations, exact
route entry, license Markdown, and release-note Markdown. Invisible HTML comments
bound the dataset's notes section so retries and updates preserve other datasets.
This offline preview does not check whether the remote release exists or the
token has write permission. Previews without `schema_version` and hashes must be
regenerated before execution.

Use `--notes`/`--notes-file` and `--license`/`--license-file` for optional Markdown.
Links become HTML in the route entry and remain Markdown in release notes. Without
a license override, the release uses the existing default:
[CC0 1.0 but attribute datameet and the original government source where possible](https://github.com/ramSeraph/indianopenmaps/blob/main/DATA_LICENSE.md).
Without `--promoteid`, the command reads the field chosen in `.analysis.json`.
For split datasets, use the original `.geojsonl` or a `.geojsonl.7z.001` volume
as `--base-file` to discover all related parts. The original need not remain
uncompressed after the archive has been verified.

### Publish and verify

After reviewing the preview and confirming publication is authorized, execute it:

```bash
python/.venv/bin/iomaps admin make-release \
  --plan scratch/new-city/release-preview.json
```

The saved plan is the input: dataset flags cannot override it, and changed local
assets fail their hash check before publication. The original argument-based
invocation is still supported, but a saved plan avoids repeating metadata.

Before uploading, the publisher checks the local `main` branch and `origin`'s
fetch/push destinations, fetches upstream, and fast-forwards when possible. It
rejects a dirty listing, divergent history, and unrelated unpushed commits. Push
your authorized tooling commits first. Unrelated staged and unstaged edits are
preserved; the publisher never stashes them. `--checkout /path/to/indianopenmaps`
selects a checkout explicitly; the default is the current directory's checkout.

It uploads assets to the existing release, updates this dataset's notes section,
then writes `worker/src/routes/listing.json` locally, commits only that file, and
pushes `main`. It never force-pushes. There is no need to upload each asset or edit
the listing manually, or fetch an API-created route commit afterward.

Verification checks GitHub asset hashes, notes, and the remote listing, waits for
the Cloudflare Workers build, and compares live route metadata and a tile with
the local PMTiles. `pmtiles` must be on `PATH`. The default deployment wait is 300
seconds; change it with `--verify-timeout`. `--no-verify` skips deployment/live
checks explicitly while retaining GitHub verification.

The default receipt is `release-preview.receipt.json` beside the plan. Use
`--receipt /path/to/receipt.json` to choose another location. It records progress,
the route commit, verification results and any error. A timeout after publication
returns a nonzero exit code with the published stage recorded in the receipt.

### Prepare/upload remotely, publish the route locally

When the dataset lives on a server, run `admin prepare` and generate the dry-run
plan there. Keep the original basename consistent with similar releases. Put the
working directory, `TMPDIR`, and tool caches on the volume with enough space;
large runs should have a durable background process and a log. Check whether a
scraper is still writing the input. An incomplete snapshot needs an explicit
coverage note and a stable input before preparation.

For large 2D WGS84 GeoJSONL on a small-memory server, use
`admin prepare ... --bounded-parquet --parquet-memory-limit 256MB`. This optional
path stages data in DuckDB on `TMPDIR`, sorts spatially with disk spill, and writes
GeoParquet 1.1 with bounding-box covering directly, without fetching the entire
dataset into Arrow. Oversized output is split along contiguous spatially sorted
row ranges and checked for size and total feature count. Keep enough space for
the source, disk-backed table, sort spill, and output simultaneously. The default
pinned GPIO converter remains available for smaller inputs; version 0.9.0's
whole-table Arrow allocation can exhaust RAM on large datasets.

DuckDB's buffer limit does not cover all process allocations. The bounded writer
uses ZSTD 9 and 8,192-row groups to reduce compression/writer memory. If a killed
process leaves a completed `features.duckdb` import, preserve it and resume with
`--parquet-resume-db /path/to/features.duckdb --bounded-parquet --no-filter
--no-zip --no-pmtiles`. The import checkpoint checks the input path, size, mtime
and table count before reuse. Keep the resumed database until output has been
verified; resuming does not remove it automatically.

On the asset server, use the existing publisher's asset phase:

```bash
python/.venv/bin/iomaps admin make-release --plan release-preview.json --phase assets
```

This validates local hashes, captures a sample tile checksum, uploads the files,
and updates/verifies release notes. It does not use Git or need route push access.
Keep verification enabled so the receipt includes the sample for the next phase.
On a headless host, the same commands can be invoked directly as
`python -m iomaps.commands.admin_prepare` and
`python -m iomaps.commands.admin_release` from the package's Python environment.

Copy only `release-preview.json` and `release-preview.receipt.json` to the local
checkout, keeping their contents unchanged; the asset paths still point to the
server. Then run:

```bash
python/.venv/bin/iomaps admin make-release \
  --plan scratch/dataset/release-preview.json --phase routes
```

This phase needs no local dataset files or PMTiles executable. It checks the
receipt belongs to the exact plan, independently verifies GitHub asset hashes
and notes, and uses local Git for the listing commit/push. It then checks the
deployment against the server's recorded tile checksum. It never uploads assets
or changes release notes. Both phases can be retried with their respective
receipts. The default `--phase all` retains single-machine publication.

If formats finish at different times or disk space is limited, stage ready assets
without waiting for PMTiles:

```bash
python/.venv/bin/iomaps admin make-release --phase uploads \
  --owner ramSeraph --repo indian_cadastrals --release rajasthan \
  --asset Rajdharaa_RJ_Cadastrals.geojsonl.7z.001 \
  --asset Rajdharaa_RJ_Cadastrals.geojsonl.7z.002 \
  --dry-run > archive-upload.json
python/.venv/bin/iomaps admin make-release --phase uploads --plan archive-upload.json
```

This phase verifies GitHub sizes/hashes and saves an `uploads_complete` receipt.
It needs neither PMTiles nor Git and leaves notes/routes untouched. Once verified,
local copies of those assets can be removed; retain the batch plan and receipt,
plus any input needed for unfinished formats. Use a separate batch plan for
GeoParquet when it finishes. Add `--uploaded-plan archive-upload.json` and
`--uploaded-plan parquet-upload.json` to the final ordinary dry-run command, using
the finished PMTiles as `--base-file`. Its manifest includes the earlier assets
as `remote_only`; final publication rechecks every remote hash before any writes
and refuses missing or changed uploads. Keep PMTiles locally until the final
`--phase assets` run captures the sample used by local route verification.

### Resume an interrupted publication

Rerun the same `--plan` command with the same receipt path. Publication is not
atomic, but retries verify and skip matching assets, avoid duplicate notes, and
reuse a recorded route commit if its push failed. An interrupted listing edit
can be resumed only if it still exactly matches this plan. Conflicting assets or
user edits are not overwritten. If intentionally changing the plan, review the
new preview and choose a new receipt path.

**`--no-uploads` still writes release notes and routes. It is not a preview.**
The plan still lists all required assets, and publication requires every one to
already exist with a matching hash. Prefer the ordinary retry path: it skips
matching assets automatically and uploads only the missing ones.

Authentication uses `GH_TOKEN`, then `GITHUB_TOKEN`, then `.secrets/gh_token.txt`
or legacy `utils/token.txt` in the current directory or enclosing checkout.
Never print or commit the token. Grant it Contents read/write on the data repo
and read access to `indianopenmaps` for verification. Route pushes use your existing
Git credentials, which need push access to `origin`. `--owner` defaults to the
authenticated user for argument-based publication; a saved preview fixes the owner.

Keep unrelated local changes out of deployment. Delhi and IILB have already been
published; use only `--dry-run` when using their files to exercise the tooling.
