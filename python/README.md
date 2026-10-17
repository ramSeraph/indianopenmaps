# Indianopenmaps [![PyPI - Latest Version](https://img.shields.io/pypi/v/iomaps)](https://pypi.org/project/iomaps/)

This project provides command-line tools and a PyQt UI for processing geographical data from Indianopenmaps. It supports extracting data from remote geoparquet files as well as filtering local 7z archives containing geojsonl files.

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

## Filtering/Extracting from Local geojsonl.7z Files

These commands work with 7z archives containing geojsonl files that have been downloaded to your local machine.

> **Note:** These commands require the 7z files to be fully downloaded to the local machine first. For multi-part 7z files (e.g., `data.7z.001`, `data.7z.002`, etc.), all parts must be downloaded locally. When running the command, provide the path to the first part (e.g., `data.7z.001`) and the remaining parts will be located automatically if they are present in the same directory.

### 1. `iomaps cli filter-7z`

Processes a 7z archive containing a single geojsonl file by streaming.

**Options:**

*   `-i, --input-7z <path>` (Required): Path to the input 7z archive file.
*   `-o, --output-file <path>` (Required): Path for the output file where processed data will be saved.
*   `-d, --output-driver <driver>`: Specify output driver (choices: `csv`, `esri shapefile`, `flatgeobuf`, `geojson`, `geojsonseq`, `gpkg`). If not specified, it will be inferred from the output file extension.
*   `-s, --schema <path>`: Path to a schema file (JSON format). If not provided, the schema will be inferred from the input archive. This requires an extra pass over the data. So, if required you can use the `infer-schema` command first to generate the schema file and then provide it here.
*   `-l, --log-level <level>`: Set the logging level (choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; default: `INFO`).
*   `-f, --filter-file <path>`: Path to an input filter file (e.g., a shapefile) for spatial filtering.
*   `--filter-file-driver <driver>`: Specify the OGR driver for the filter file. If not specified, it will be inferred from the filter file extension.
*   `-b, --bounds <min_lon,min_lat,max_lon,max_lat>`: Rectangular bounds for spatial filtering. You can get bounding box coordinates from [http://bboxfinder.com/](http://bboxfinder.com/).
*   `--pick-filter-feature-id <id>`: Select a specific polygon feature by its 0-based index.
*   `--pick-filter-feature-kv <key=value>`: Select a specific polygon feature by a key-value pair (e.g., 'key=value') from its properties.

**Example:**

```bash
uvx iomaps cli filter-7z -i data.7z -o filtered_data.geojsonl -b "70,10,90,30"
```

### 2. `iomaps cli infer-schema`

Infers the schema from a 7z archive containing a single geojsonl file and saves it to a JSON file.

**Options:**

*   `-i, --input-7z <path>` (Required): Path to the input 7z archive file.
*   `-o, --output-file <path>`: Path for the output file where the inferred schema will be saved.
*   `-l, --log-level <level>`: Set the logging level (choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; default: `INFO`).

**Example:**

```bash
uvx iomaps cli infer-schema -i data.7z -o data_schema.json
```

### 3. `iomaps ui`

Launches a PyQt-based graphical user interface for processing local 7z archives.

**Example:**

```bash
uvx iomaps ui
```

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

### Prepare a dataset

Sync the checkout with upstream before the next dataset or local commit. Older
versions committed routes directly through GitHub's API, leaving the checkout
behind. Publication now uses local Git and checks upstream automatically. Run
`git fetch origin`, inspect incoming changes, then use `git merge --ff-only
origin/main` when possible. Preserve local edits and resolve any divergence before
continuing; see [AGENTS.md](../AGENTS.md#sync-before-local-work).

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
The chosen ID must also exist in the tiles; conversion can add an `OGC_FID` field
only to GeoParquet. Use `--promoteid` to select a suitable source field when needed.

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
For split datasets, the original `.geojsonl` can be used as `--base-file` to discover
all related parts.

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
