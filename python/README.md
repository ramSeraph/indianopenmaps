# Indianopenmaps

This project provides command-line tools and a Streamlit UI for processing geographical data, specifically focusing on Indianopenmaps data. It leverages 7z archives containing geojsonl files for efficient data handling.

## Installation

This project uses `uv` for dependency management.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/ramseraph/indianopenmaps.git
    cd indianopenmaps/python
    ```

2.  **Install dependencies using `uv`:**
    ```bash
    uv sync
    ```

## Usage

The `indianopenmaps` project provides a command-line interface (CLI) with several subcommands. You can run these commands using `uvx iomaps <command>`. This invoction automaticlly manages the virtual environments for you.

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

Launches a Streamlit-based graphical user interface for interacting with the Indianopenmaps tools.

**Example:**

```bash
uvx iomaps ui
```
