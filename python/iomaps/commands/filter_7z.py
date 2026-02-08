import json
import logging
from pathlib import Path

import click
import multivolumefile
import py7zr
from fiona.crs import CRS
from rich.progress import BarColumn, DownloadColumn, Progress, TextColumn, TimeElapsedColumn, TransferSpeedColumn

from iomaps.commands.decorators import (
    add_filter_options,
    add_output_options,
    validate_filter_options,
    validate_output_driver,
)
from iomaps.commands.schema_common import get_schema_from_archive
from iomaps.core.helpers import (
    get_geojsonl_file_info,
    readable_size,
)
from iomaps.core.spatial_filter import (
    PassThroughFilter,
    ShapeFilter,
    get_shape_from_bounds,
    get_shape_from_filter_file,
)
from iomaps.core.streaming_7z import StreamingWriterFactory
from iomaps.core.writers import create_writer


class RichUpdater:
    def __init__(self, progress, task_id):
        self.progress = progress
        self.task_id = task_id

    def update_size(self, sz):
        self.progress.update(self.task_id, advance=sz)

    def update_other_info(self, **kwargs):
        output_size = kwargs.get("output_size", None)
        output_size_str = readable_size(output_size)
        processed = kwargs.get("processed", 0)
        passed = kwargs.get("passed", 0)
        self.progress.update(
            self.task_id, description=f"[cyan]Filtering ({processed:,} processed, {passed:,} passed, {output_size_str})"
        )


class CombinedUpdater:
    def __init__(self, *updaters):
        self.updaters = updaters

    def update_size(self, sz):
        for updater in self.updaters:
            updater.update_size(sz)

    def update_other_info(self, **kwargs):
        for updater in self.updaters:
            updater.update_other_info(**kwargs)


def process_archive(archive, filter, writer, external_updater=None):
    target_file_info = get_geojsonl_file_info(archive)
    if target_file_info is None:
        return

    target_file = target_file_info.filename
    file_size = target_file_info.uncompressed

    logging.info(f"Found geojsonl file: {target_file} (size: {file_size} bytes)")

    try:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=None,
        ) as progress:
            task_id = progress.add_task(f"[cyan]Filtering {target_file}", total=file_size)
            updater = RichUpdater(progress, task_id)
            if external_updater:
                updater = CombinedUpdater(updater, external_updater)
            factory = StreamingWriterFactory(filter, writer, updater)
            archive.extract(targets=[target_file], factory=factory)
            factory.streaming_io.flush_last_line()
    finally:
        writer.close()


@click.command("filter-7z")
@click.option(
    "-i",
    "--input-7z",
    required=True,
    type=click.Path(exists=True),
    help="Path to the input 7z archive file.",
)
@add_output_options(exclude_drivers=["parquet"])
@click.option(
    "-s",
    "--schema",
    "schema_file",
    default=None,
    type=click.Path(exists=True),
    help="Path to schema file. If not provided, schema will be inferred from the input archive.",
)
@click.option(
    "-l",
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Set the logging level.",
)
@add_filter_options
def filter_7z(
    input_7z,
    output_file,
    log_level,
    filter_file,
    filter_file_driver,
    bounds,
    schema_file,
    output_driver,
    pick_filter_feature_id,
    pick_filter_feature_kv,
):
    """
    Processes a 7z archive containing a single geojsonl file by streaming.
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info(f"Processing {input_7z}")

    # Validate output driver
    effective_driver = validate_output_driver(output_file, output_driver)

    # Validate filter options
    validate_filter_options(filter_file, bounds)

    # Get filter shape (may be None if no filter specified)
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

    # Get schema
    if schema_file:
        schema_path = Path(schema_file)
        logging.info(f"Reading schema from {schema_path}")
        with schema_path.open("r") as f:
            schema = json.load(f)
        property_renames = schema.pop("property_renames", {})
    else:
        logging.info(f"Inferring schema from {input_7z}")
        schema, property_renames = get_schema_from_archive(input_7z)
        if schema is None:
            logging.error("Could not infer schema from the archive.")
            return

    # Create filter with allowed geometry types from schema
    schema_properties = schema.get("properties", {})
    if filter_shape is not None:
        filter_obj = ShapeFilter(filter_shape, property_renames=property_renames, schema_properties=schema_properties)
    else:
        filter_obj = PassThroughFilter(property_renames=property_renames, schema_properties=schema_properties)

    crs = CRS.from_epsg(4326)
    writer = create_writer(output_file, schema, crs, effective_driver)

    archive_path = input_7z
    if archive_path.endswith(".001"):
        base_path = archive_path.rsplit(".", 1)[0]
        with multivolumefile.open(base_path, "rb") as multivolume_file:
            with py7zr.SevenZipFile(multivolume_file, "r") as archive:
                process_archive(archive, filter_obj, writer)
    else:
        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            process_archive(archive, filter_obj, writer)
