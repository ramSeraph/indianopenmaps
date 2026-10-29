"""
Extract command for filtering data from remote geoparquet sources.

Uses DuckDB to read remote parquet files directly and write to various formats.

Some of this code was adapted from geoparquet-io (https://github.com/pka/geoparquet-io).
"""

import json
import logging
import sys
import threading
import time

import click
import duckdb
import requests
from geoparquet_io.core.common import write_geoparquet_table
from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info
from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text
from shapely.geometry import box

from iomaps.commands.decorators import (
    add_filter_options,
    add_log_level_option,
    add_output_options,
    add_routes_options,
    validate_filter_options,
    validate_output_driver,
    validate_routes_options,
)
from iomaps.commands.sources import (
    get_vector_sources,
    resolve_source,
)
from iomaps.core.helpers import GEOPARQUET_VERSIONS
from iomaps.core.local_parquet import local_parquet_paths
from iomaps.core.source_assets import get_parquet_info_from_source as get_parquet_info_from_source
from iomaps.core.spatial_filter import (
    get_shape_from_bounds,
    get_shape_from_filter_file,
)

# GDAL format configuration for DuckDB COPY
GDAL_FORMATS = {
    "GPKG": {"driver": "GPKG", "layer_option": "LAYER_NAME"},
    "FlatGeobuf": {"driver": "FlatGeobuf"},
    "ESRI Shapefile": {"driver": "ESRI Shapefile", "encoding_option": "ENCODING"},
    "GeoJSON": {"driver": "GeoJSON"},
    "GeoJSONSeq": {"driver": "GeoJSONSeq"},
    "KML": {"driver": "KML"},
}


def get_duckdb_connection(local_only=False):
    """Create a connection; local inputs need only the spatial extension."""
    con = duckdb.connect()
    try:
        con.execute("LOAD spatial")
    except duckdb.IOException:
        con.execute("INSTALL spatial; LOAD spatial;")
    if not local_only:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    settings = {row[0] for row in con.execute("SELECT name FROM duckdb_settings()").fetchall()}
    if "geometry_always_xy" in settings:
        # DuckDB 2.0 requires an explicit axis-order choice for GDAL export.
        # GeoParquet and our filter geometries use longitude/easting first.
        con.execute("SET geometry_always_xy = true")
    if not local_only:
        con.execute("SET enable_server_cert_verification = true")
    # Limit memory usage for large operations
    con.execute("SET memory_limit = '2GB';")
    # Concurrent Parquet readers need resident decode buffers; those cannot
    # always spill. Two threads keep large geometry row groups within this cap.
    con.execute("SET threads = 2;")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET arrow_large_buffer_size = true;")
    return con


def fetch_partition_metadata(meta_url):
    """
    Fetches the partition metadata from a .parquet.meta.json file.

    Args:
        meta_url: URL to the meta.json file

    Returns:
        dict: Metadata containing 'schema' and 'extents', or None on error
    """
    try:
        response = requests.get(meta_url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch partition metadata from {meta_url}: {e}")
        return None


def get_partitions_for_filter(meta_url, filter_shape, metadata):
    """
    Gets list of partition URLs that intersect with the given filter shape.

    Args:
        meta_url: URL to the meta.json file
        filter_shape: Shapely geometry to filter by, or None to get all partitions
        metadata: Pre-fetched metadata dict

    Returns:
        list: List of partition URLs that intersect the filter shape
    """
    extents = metadata.get("extents", {})
    base_url = meta_url.rsplit("/", 1)[0]

    matching_partition_urls = []
    for partition_name, extent in extents.items():
        if filter_shape is None:
            matching_partition_urls.append(f"{base_url}/{partition_name}")
        else:
            p_minx = extent.get("minx")
            p_miny = extent.get("miny")
            p_maxx = extent.get("maxx")
            p_maxy = extent.get("maxy")

            # Create a box for the partition extent and check intersection with filter shape
            partition_box = box(p_minx, p_miny, p_maxx, p_maxy)
            if filter_shape.intersects(partition_box):
                matching_partition_urls.append(f"{base_url}/{partition_name}")
                logging.debug(f"Partition {partition_name} intersects with filter shape")
            else:
                logging.debug(f"Partition {partition_name} skipped (no intersection)")

    return matching_partition_urls


def get_schema_from_parquet(parquet_url):
    """
    Gets schema info from a remote geoparquet file by reading only metadata.

    Args:
        parquet_url: URL to the geoparquet file

    Returns:
        tuple: (geometry_types, properties_schema) where geometry_types is a list
               and properties_schema is a dict mapping column names to {"type": ...}.
               Returns (None, None) on error.
    """
    try:
        logging.info(f"Reading parquet metadata from {parquet_url}")

        # Get geo metadata for geometry types
        geo_metadata = get_geo_metadata(parquet_url)

        geometry_types = []
        if geo_metadata and "columns" in geo_metadata:
            primary_col = geo_metadata.get("primary_column", "geometry")
            if primary_col in geo_metadata["columns"]:
                col_info = geo_metadata["columns"][primary_col]
                geometry_types = col_info.get("geometry_types", [])

        # Get schema info from DuckDB and convert to dict format
        schema_list = get_schema_info(parquet_url)
        properties_schema = {}
        for col in schema_list:
            col_name = col.get("name", "")
            if col_name:
                properties_schema[col_name] = {"type": col.get("type")}

        return geometry_types, properties_schema

    except Exception as e:
        logging.error(f"Failed to get schema from parquet metadata: {e}")
        return None, None


def get_covering_bbox(con, parquet_url):
    """Read GeoParquet 1.1 covering paths from the footer, without scanning rows."""
    rows = con.execute("SELECT value FROM parquet_kv_metadata(?) WHERE key = 'geo'", [parquet_url]).fetchall()
    if not rows:
        return None
    try:
        metadata = json.loads(rows[0][0])
        column = metadata["columns"][metadata["primary_column"]]
        bbox = column.get("covering", {}).get("bbox", {})
        axes = ("xmin", "ymin", "xmax", "ymax")
        if all(
            isinstance(bbox.get(axis), list)
            and bbox[axis]
            and all(isinstance(name, str) and name for name in bbox[axis])
            for axis in axes
        ):
            return {axis: bbox[axis] for axis in axes}
    except (KeyError, TypeError, ValueError, AttributeError):
        logging.warning("Invalid GeoParquet covering metadata in %s; using geometry filter only", parquet_url)
    return None


def build_source_query(parquet_urls, filter_shape=None, covering_bboxes=None):
    """
    Builds a DuckDB SQL query to read and filter parquet files.

    Args:
        parquet_urls: List of parquet URLs to read
        filter_shape: Optional shapely geometry for spatial filtering
        covering_bboxes: Optional mapping of URL to GeoParquet covering column paths.

    Returns:
        str: SQL query string
    """
    if not parquet_urls:
        raise ValueError("At least one Parquet source is required")
    queries = []
    for url in parquet_urls:
        safe_url = url.replace("'", "''")
        query = f"SELECT * FROM read_parquet('{safe_url}')"
        if filter_shape is not None:
            predicates = []
            bbox = (covering_bboxes or {}).get(url)
            if bbox:
                minx, miny, maxx, maxy = filter_shape.bounds
                for axis, operator, bound in (
                    ("xmin", "<=", maxx),
                    ("ymin", "<=", maxy),
                    ("xmax", ">=", minx),
                    ("ymax", ">=", miny),
                ):
                    # Coverings may use nested structs or separately named columns.
                    path = ".".join('"' + name.replace('"', '""') + '"' for name in bbox[axis])
                    # A decimal literal can promote integer coverings to DECIMAL
                    # and prevent scan pushdown in DuckDB 2.0. Bounds are doubles.
                    predicates.append(f"({path} {operator} {bound}::DOUBLE OR {path} IS NULL)")
            wkt = filter_shape.wkt.replace("'", "''")
            # Keep the exact predicate: a bbox match can lie in a polygon's hole.
            predicates.append(f"ST_Intersects(geometry, ST_GeomFromText('{wkt}'))")
            query += "\nWHERE " + "\n  AND ".join(predicates)
        queries.append(query)
    # Per-file predicates support partitions with different covering layouts.
    return "\nUNION ALL BY NAME\n".join(queries)


def execute_with_spinner(con, query, description="Processing"):
    """Execute a DuckDB query while showing a spinner with elapsed time.

    Note: DuckDB 1.5.0 (expected Feb 2026) adds a working query_progress() API that can be
    polled from a separate thread to get actual progress percentage. See PR #16927:
    https://github.com/duckdb/duckdb/pull/16927
    Once upgraded, this can be replaced with a proper progress bar.
    """
    console = Console(stderr=True)
    result = None
    error = None
    start_time = time.time()

    def run_query():
        nonlocal result, error
        try:
            result = con.execute(query)
        except Exception as e:
            error = e

    thread = threading.Thread(target=run_query)
    thread.start()

    # Show spinner while query runs (no real progress available in DuckDB < 1.5.0)
    with Live(console=console, refresh_per_second=4, transient=True) as live:
        while thread.is_alive():
            elapsed = time.time() - start_time
            spinner = Spinner("dots", text=Text(f" {description}... ({elapsed:.1f}s)", style="cyan"))
            live.update(spinner)
            time.sleep(0.1)

    thread.join()

    elapsed = time.time() - start_time
    if error:
        raise error

    logging.info(f"{description} completed in {elapsed:.1f}s")
    return result


def write_gdal_output(con, query, output_path, driver, layer_name="features"):
    """Write query results to GDAL format via DuckDB COPY."""
    config = GDAL_FORMATS.get(driver)
    if not config:
        raise ValueError(f"Unsupported GDAL driver: {driver}")

    # GeoParquet coverings are usually STRUCT columns. DuckDB's GDAL writer
    # cannot export these (or maps/lists) directly. Preserve their values as
    # JSON text instead of silently dropping source attributes.
    columns = con.execute(f"DESCRIBE SELECT * FROM ({query})").fetchall()
    expressions = []
    nested_columns = []
    for name, column_type, *_ in columns:
        identifier = '"' + name.replace('"', '""') + '"'
        if column_type.startswith(("STRUCT(", "MAP(", "UNION(")) or column_type.endswith("]"):
            expressions.append(f"CAST(to_json({identifier}) AS VARCHAR) AS {identifier}")
            nested_columns.append(name)
        else:
            expressions.append(identifier)
    if nested_columns:
        logging.info("Encoding nested columns as JSON text for %s: %s", driver, ", ".join(nested_columns))
        query = f"SELECT {', '.join(expressions)} FROM ({query})"

    # Build layer creation options
    lco_parts = []
    if config.get("layer_option"):
        safe_layer_name = layer_name.replace("'", "''")
        lco_parts.append(f"{config['layer_option']}={safe_layer_name}")
    if config.get("encoding_option"):
        lco_parts.append(f"{config['encoding_option']}=UTF-8")

    lco_clause = f", LAYER_CREATION_OPTIONS '{' '.join(lco_parts)}'" if lco_parts else ""
    safe_output_path = output_path.replace("'", "''")

    copy_query = f"""
        COPY ({query})
        TO '{safe_output_path}'
        WITH (FORMAT GDAL, DRIVER '{config["driver"]}'{lco_clause})
    """

    logging.debug(f"Executing: {copy_query}")
    execute_with_spinner(con, copy_query, f"Downloading and writing {driver}")


def write_csv_output(con, query, output_path):
    """Write query results to CSV with WKT geometry."""
    # Wrap query to convert geometry to WKT
    csv_query = f"""
        SELECT * EXCLUDE (geometry), ST_AsText(geometry) as wkt
        FROM ({query})
    """

    safe_output_path = output_path.replace("'", "''")
    copy_query = f"""
        COPY ({csv_query})
        TO '{safe_output_path}'
        WITH (HEADER TRUE, DELIMITER ',')
    """

    logging.debug(f"Executing: {copy_query}")
    execute_with_spinner(con, copy_query, "Downloading and writing CSV")


def write_parquet_output(con, query, output_path, geoparquet_version="1.1"):
    """Write query results to GeoParquet."""
    console = Console(stderr=True)
    error = None
    start_time = time.time()

    def run_write():
        nonlocal error
        try:
            # Own the DuckDB -> Arrow boundary: geoparquet-io 0.9's SQL writer
            # calls fetch_arrow_table(), removed from DuckDB 2.0.
            table = con.sql(f"SELECT * REPLACE (ST_AsWKB(geometry)::BLOB AS geometry) FROM ({query})").to_arrow_table()
            write_geoparquet_table(
                table=table,
                output_file=output_path,
                geometry_column="geometry",
                compression="ZSTD",
                compression_level=15,
                row_group_rows=100000,
                geoparquet_version=geoparquet_version,
            )
        except Exception as e:
            error = e

    thread = threading.Thread(target=run_write)
    thread.start()

    description = "Downloading and writing Parquet"
    with Live(console=console, refresh_per_second=4, transient=True) as live:
        while thread.is_alive():
            elapsed = time.time() - start_time
            spinner = Spinner("dots", text=Text(f" {description}... ({elapsed:.1f}s)", style="cyan"))
            live.update(spinner)
            time.sleep(0.1)

    thread.join()

    elapsed = time.time() - start_time
    if error:
        raise error

    logging.info(f"{description} completed in {elapsed:.1f}s")


@click.command("extract")
@click.option(
    "-s",
    "--source",
    help="Remote source name, route or number. Mutually exclusive with --parquet.",
)
@click.option(
    "--parquet",
    "parquets",
    multiple=True,
    type=click.Path(exists=True, dir_okay=False, resolve_path=True),
    help="Local GeoParquet or .parquet.meta.json; repeat for multiple inputs. Skips the remote catalog.",
)
@click.option(
    "--http-client",
    type=click.Choice(["default", "curl", "httplib"]),
    default="default",
    show_default=True,
    help="DuckDB HTTP client override; default uses its native connection pooling and range reads.",
)
@add_output_options
@add_routes_options
@add_log_level_option(default="INFO")
@add_filter_options
def extract(
    source,
    parquets,
    output_file,
    log_level,
    filter_file,
    filter_file_driver,
    bounds,
    output_driver,
    pick_filter_feature_id,
    pick_filter_feature_kv,
    routes_file,
    routes_url,
    http_client,
):
    """
    Extract and filter data from a remote geoparquet source.

    Uses DuckDB to read remote parquet files and apply spatial filters efficiently.

    \b
    Sources can be specified by:
      - Name (e.g., "SOI States")
      - Route starting with / (e.g., "/states/soi/")
      - Number from 'iomaps cli sources' list (e.g., "1", "2")
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stderr,
    )

    # Early check: validate output file format before doing any network calls
    effective_driver = validate_output_driver(output_file, output_driver)

    if bool(source) == bool(parquets):
        raise click.UsageError("Specify exactly one of --source or --parquet")
    if parquets and (routes_file or routes_url or http_client != "default"):
        raise click.UsageError("Local --parquet input cannot use catalog or HTTP options")

    # Validate mutually exclusive options
    if not validate_routes_options(routes_file, routes_url):
        logging.error("--routes-file and --routes-url are mutually exclusive.")
        return

    # Validate filter options
    validate_filter_options(filter_file, bounds)

    if not parquets:
        # Fetch available vector sources
        logging.info("Fetching available sources...")
        vector_sources = get_vector_sources(routes_url=routes_url, routes_file=routes_file)

        if vector_sources is None:
            logging.error("Failed to fetch sources. Aborting.")
            return

        logging.info(f"Found {len(vector_sources)} available sources")

        # Resolve requested source and get parquet info
        source_config = resolve_source(source, vector_sources)

        if source_config is None:
            logging.info("Run 'iomaps cli sources' to see available sources.")
            return

        parquet_info = get_parquet_info_from_source(source_config)

        if parquet_info is None:
            source_name = source_config.get("name", source)
            logging.error(f"Could not determine parquet URL for source '{source_name}'")
            return

        source_name = source_config.get("name", source)
        logging.info(f"Resolved source '{source}' ({source_name}) to {parquet_info['base_url']}")
        if parquet_info["is_partitioned"]:
            logging.info(f"  Source is partitioned, meta: {parquet_info['meta_url']}")

    # Get filter shape (needed for partition filtering and spatial query)
    filter_shape = None
    if filter_file or bounds:
        if filter_file:
            filter_shape = get_shape_from_filter_file(
                filter_file,
                driver=filter_file_driver,
                pick_filter_feature_id=pick_filter_feature_id,
                pick_filter_feature_kv=pick_filter_feature_kv,
            )
        else:
            filter_shape = get_shape_from_bounds(bounds)

        if filter_shape is None:
            raise click.Abort()

    # Collect parquet URLs
    parquet_urls = []

    if parquets:
        try:
            parquet_urls = local_parquet_paths(parquets, filter_shape)
        except (ValueError, OSError) as exc:
            raise click.ClickException(str(exc)) from exc
        if not parquet_urls:
            raise click.ClickException("No local partitions intersect the filter")
        logging.info("Reading %d local Parquet files", len(parquet_urls))
    elif parquet_info["is_partitioned"]:
        # Fetch metadata for partitions
        partition_metadata = fetch_partition_metadata(parquet_info["meta_url"])
        if partition_metadata is None:
            logging.error(f"Failed to fetch metadata for {parquet_info['base_url']}")
            return

        # Get partitions that intersect with filter shape
        partitions = get_partitions_for_filter(parquet_info["meta_url"], filter_shape, partition_metadata)
        if not partitions:
            logging.info("No partitions intersect with the filter bounds. Nothing to extract.")
            return
        logging.info(f"Found {len(partitions)} partitions to process")
        parquet_urls.extend(partitions)
    else:
        parquet_urls.append(parquet_info["base_url"])

    # Create DuckDB connection and execute
    con = get_duckdb_connection(local_only=True) if parquets else get_duckdb_connection()

    try:
        if http_client != "default":
            con.execute("SET httpfs_client_implementation = ?", [http_client])
            if http_client == "curl":
                con.execute("SET enable_curl_server_cert_verification = true")
            logging.info("Using %s HTTP client with keep-alive connections", http_client)
        # Scalar predicates on the declared covering let DuckDB prune Parquet row
        # groups and remote byte ranges. ST_Intersects alone does not do this for
        # these GeoParquet 1.1 files, even when given a rectangular geometry.
        coverings = {}
        if filter_shape is not None:
            coverings = {url: get_covering_bbox(con, url) for url in parquet_urls}
            logging.info(
                "Using bbox filter pushdown for %d/%d files",
                sum(bool(v) for v in coverings.values()),
                len(parquet_urls),
            )
        query = build_source_query(parquet_urls, filter_shape, coverings)
        logging.debug(f"Query: {query}")

        # Write output based on format
        logging.info(f"Writing to {output_file} (driver: {effective_driver})")

        if effective_driver in GDAL_FORMATS:
            write_gdal_output(con, query, output_file, effective_driver)
        elif effective_driver == "CSV":
            write_csv_output(con, query, output_file)
        elif effective_driver in GEOPARQUET_VERSIONS:
            version = GEOPARQUET_VERSIONS[effective_driver]
            write_parquet_output(con, query, output_file, geoparquet_version=version)
        else:
            logging.error(f"Unsupported output driver: {effective_driver}")
            return

        logging.info(f"Output written to {output_file}")

    except Exception as e:
        logging.error(f"Failed to write output: {e}")
        raise
    finally:
        con.close()
