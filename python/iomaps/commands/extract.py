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
from geoparquet_io.core.common import write_parquet_with_metadata
from geoparquet_io.core.duckdb_metadata import get_geo_metadata, get_schema_info
from rich.console import Console
from rich.live import Live
from rich.spinner import Spinner
from rich.text import Text

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
}


def get_duckdb_connection():
    """Create a DuckDB connection with spatial and httpfs extensions."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # Limit memory usage for large operations
    con.execute("SET memory_limit = '2GB';")
    con.execute("SET preserve_insertion_order = false;")
    con.execute("SET arrow_large_buffer_size = true;")
    return con


def get_parquet_info_from_source(source_config):
    """
    Extracts the geoparquet URL(s) from a source configuration.

    Args:
        source_config: Source configuration dict

    Returns:
        dict: Contains 'base_url' (str), 'is_partitioned' (bool), and 'meta_url' (str or None)
              Returns None if source is raster or has no URL
    """
    # Skip raster sources - they don't have vector parquet files
    source_type = source_config.get("type")
    if source_type == "raster":
        return None

    # Get the URL from the source config
    url = source_config.get("url")

    if not url:
        return None

    is_partitioned = source_config.get("partitioned_parquet", False)

    # Replace .pmtiles suffix with .parquet
    if url.endswith(".pmtiles"):
        base_url = url.replace(".pmtiles", ".parquet")
    # Replace .mosaic.json suffix with .parquet
    elif url.endswith(".mosaic.json"):
        base_url = url.replace(".mosaic.json", ".parquet")
    else:
        return None

    if is_partitioned:
        meta_url = base_url + ".meta.json"
    else:
        meta_url = None

    return {
        "base_url": base_url,
        "is_partitioned": is_partitioned,
        "meta_url": meta_url,
    }


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
    from shapely.geometry import box

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


def build_source_query(parquet_urls, filter_shape=None):
    """
    Builds a DuckDB SQL query to read and filter parquet files.

    Args:
        parquet_urls: List of parquet URLs to read
        filter_shape: Optional shapely geometry for spatial filtering

    Returns:
        str: SQL query string
    """
    # Build parquet source expression
    if len(parquet_urls) == 1:
        source_expr = f"read_parquet('{parquet_urls[0]}')"
    else:
        urls_json = json.dumps(parquet_urls)
        source_expr = f"read_parquet({urls_json})"

    # Build WHERE clause for spatial filter
    where_clause = ""
    if filter_shape is not None:
        wkt = filter_shape.wkt
        minx, miny, maxx, maxy = filter_shape.bounds
        # First filter by bbox (fast), then by actual intersection (slower but precise)
        where_clause = f"""
WHERE ST_Intersects(geometry, ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy}))
  AND ST_Intersects(geometry, ST_GeomFromText('{wkt}'))"""

    return f"SELECT * FROM {source_expr}{where_clause}"


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


def write_parquet_output(con, query, output_path):
    """Write query results to GeoParquet."""
    console = Console(stderr=True)
    error = None
    start_time = time.time()

    def run_write():
        nonlocal error
        try:
            write_parquet_with_metadata(
                con=con,
                query=query,
                output_file=output_path,
                compression="ZSTD",
                compression_level=15,
                row_group_rows=100000,
                geoparquet_version="2.0",
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
    required=True,
    help="Source to extract from. Can be a name, route (starting with /), or number from 'iomaps cli sources'.",
)
@add_output_options
@add_routes_options
@add_log_level_option(default="INFO")
@add_filter_options
def extract(
    source,
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

    # Validate mutually exclusive options
    if not validate_routes_options(routes_file, routes_url):
        logging.error("--routes-file and --routes-url are mutually exclusive.")
        return

    # Validate filter options
    validate_filter_options(filter_file, bounds)

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

    if parquet_info["is_partitioned"]:
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

    # Build query with spatial filter
    query = build_source_query(parquet_urls, filter_shape)
    logging.debug(f"Query: {query}")

    # Create DuckDB connection and execute
    con = get_duckdb_connection()

    try:
        # Write output based on format
        logging.info(f"Writing to {output_file} (driver: {effective_driver})")

        if effective_driver in GDAL_FORMATS:
            write_gdal_output(con, query, output_file, effective_driver)
        elif effective_driver == "CSV":
            write_csv_output(con, query, output_file)
        elif effective_driver == "Parquet":
            write_parquet_output(con, query, output_file)
        else:
            logging.error(f"Unsupported output driver: {effective_driver}")
            return

        logging.info(f"Output written to {output_file}")

    except Exception as e:
        logging.error(f"Failed to write output: {e}")
        raise
    finally:
        con.close()
