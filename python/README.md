# Indianopenmaps [![PyPI - Latest Version](https://img.shields.io/pypi/v/iomaps)](https://pypi.org/project/iomaps/)

This project provides command-line tools and a PyQt UI for processing geographical data, specifically focusing on Indianopenmaps data. It leverages 7z archives containing geojsonl files for efficient data handling.

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

The `indianopenmaps` project provides a command-line interface (CLI) with several subcommands. If no subcommand is provided, the PyQt UI will be launched by default.

### Running with `uvx` (Recommended)

`uvx` is the easiest and recommended way to run the `indianopenmaps` CLI. It automatically manages virtual environments for you, ensuring that the correct dependencies are installed and activated without manual intervention. This means you don't need to explicitly create or activate a virtual environment.

To run commands using `uvx`:
```bash
uvx iomaps <command>
```
For example:
```bash
uvx iomaps cli filter-7z -i data.7z -o filtered_data.geojsonl -b "70,10,90,30"
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
    iomaps cli filter-7z -i data.7z -o filtered_data.geojsonl -b "70,10,90,30"
    ```
    Remember to activate the virtual environment (`source .venv/bin/activate`) each time you open a new terminal session before running `iomaps` commands.

### 1. `iomaps cli filter-7z`

Processes a 7z archive containing a single geojsonl file by streaming, allowing for spatial filtering and schema management.

**Options:**

*   `-i, --input-7z <path>` (Required): Path to the input 7z archive file.
*   `-o, --output-file <path>` (Required): Path for the output file where processed data will be saved.
*   `-d, --output-driver <driver>`: Specify output driver (e.g., `ESRI Shapefile`, `GeoJSON`). If not specified, it will be inferred from the output file extension.
*   `-s, --schema <path>`: Path to a schema file (JSON format). If not provided, the schema will be inferred from the input archive. This requires an extra pass over the data. So, if required you can ing `infer-schema` command first to generate the schema file and then provide it here.
*   `-l, --log-level <level>`: Set the logging level (choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; default: `INFO`).
*   `-f, --filter-file <path>`: Path to an input filter file (e.g., a shapefile) for spatial filtering.
*   `--filter-file-driver <driver>`: Specify the OGR driver for the filter file. If not specified, it will be inferred from the filter file extension.
*   `-b, --bounds <min_lon,min_lat,max_lon,max_lat>`: Rectangular bounds for spatial filtering. You can get bounding box coordinates from [http://bboxfinder.com/](http://bboxfinder.com/).
    *   **Note:** Only one of `--filter-file` or `--bounds` can be used, and one is required for filtering.
*   `--no-clip`: Do not clip features by the filter shape or bounding box. Only filter by intersection.
*   `-g, --limit-to-geom-type <type>`: Limit processing to a specific geometry type (choices: `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`, `GeometryCollection`). `MultiPolygon` matches `Polygon` unless `--strict-geom-type-check` is used.
*   `--strict-geom-type-check`: If enabled, geometry types must match exactly. Otherwise, `MultiPolygon` matches `Polygon`, etc.
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
*   `-o, --output-file <path>`: Path for the output file where the inferred schema will be saved. If not provided, the schema will be saved to `<input_7z_basename>.schema.json` in the same directory as the input 7z file.
*   `-l, --log-level <level>`: Set the logging level (choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`; default: `INFO`).
*   `-g, --limit-to-geom-type <type>`: Limit schema inference to a specific geometry type (choices: `Point`, `LineString`, `Polygon`, `MultiPoint`, `MultiLineString`, `MultiPolygon`, `GeometryCollection`). `MultiPolygon` matches `Polygon` unless `--strict-geom-type-check` is used.
*   `--strict-geom-type-check`: If enabled, geometry types must match exactly. Otherwise, `MultiPolygon` matches `Polygon`, etc.

**Example:**

```bash
uvx iomaps cli infer-schema -i data.7z -o data_schema.json
```

### 3. `iomaps ui`

Launches a PyQt-based graphical user interface for interacting with the Indianopenmaps tools. This is now the default behavior when `iomaps` is run without any arguments.

**Example:**

```bash
uvx iomaps ui
```

or simply

```bash
uvx iomaps
```
