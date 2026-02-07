# Indianopenmaps [![PyPI - Latest Version](https://img.shields.io/pypi/v/iomaps)](https://pypi.org/project/iomaps/)

This project provides command-line tools and a PyQt UI for processing geographical data from Indianopenmaps. It supports extracting data from remote geoparquet files as well as filtering local 7z archives containing geojsonl files.

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
*   `--routes-url <url>`: URL to fetch routes from. Cannot be used with --routes-file. Default: https://indianopenmaps.fly.dev/api/routes

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
