
import logging
import json
from pathlib import Path

import click
import py7zr
import multivolumefile
from fiona.crs import CRS
from tqdm import tqdm

from iomaps.helpers import (
    get_geojsonl_file_info,
    readable_size,
    get_supported_output_drivers,
)

from iomaps.streaming import StreamingWriterFactory

from iomaps.filter_7z import (
    create_filter,
    create_writer,
)

from iomaps.commands.schema_common import get_schema_from_archive

class Updater:
    def __init__(self, pb):
        self.pb = pb

    def update_size(self, sz):
        self.pb.update(sz)

    def update_other_info(self, **kwargs):
        output_size = kwargs.get('output_size', None)

        output_size_str = readable_size(output_size)

        self.pb.set_postfix(processed=kwargs.get('processed', 0),
                            passed=kwargs.get('passed', 0),
                            output_size=output_size_str)

def process_archive(archive, filter, writer):
    target_file_info = get_geojsonl_file_info(archive)
    if target_file_info is None:
        return
    
    target_file = target_file_info.filename
    file_size = target_file_info.uncompressed

    logging.info(f"Found geojsonl file: {target_file} (size: {file_size} bytes)")

    try:
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Filtering {target_file}") as pbar:
            updater = Updater(pbar)
            factory = StreamingWriterFactory(filter, writer, updater)
            archive.extract(targets=[target_file], factory=factory)
            factory.streaming_io.flush_last_line()
    finally:
        writer.close()


@click.command("filter-7z")
@click.option("-i", "--input-7z", required=True, type=click.Path(exists=True), help="Path to the input 7z archive file.")
@click.option("-o", "--output-file", required=True, type=click.Path(), help="Path for the output file where processed data will be saved.")
@click.option("-d", "--output-driver", type=click.Choice(get_supported_output_drivers(), case_sensitive=False), help="Specify output driver. If not specified, it will be inferred from the output file extension.")
@click.option("-s", "--schema", "schema_file", default=None, type=click.Path(exists=True), help="Path to schema file. If not provided, schema will be inferred from the input archive.")
@click.option("-l", "--log-level", default="INFO", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False), help="Set the logging level.")
@click.option("-f", "--filter-file", type=click.Path(exists=True), default=None, help="Path to an input filter file (e.g., shapefile) for spatial filtering.")
@click.option("--filter-file-driver", default=None, help="Specify the OGR driver for the filter file. If not specified, it will be inferred from the filter file extension.")
@click.option("-b", "--bounds", help="Rectangular bounds for spatial filtering, e.g., 'min_lon,min_lat,max_lon,max_lat'. You can get bounding box coordinates from http://bboxfinder.com/.")
@click.option("--no-clip", is_flag=True, help="Do not clip features by the filter shape or bounding box. Only filter by intersection.")
@click.option("-g", "--limit-to-geom-type", type=click.Choice(["Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"], case_sensitive=False), help="Limit processing to a specific geometry type. MultiPolygon matches Polygon unless --strict-geom-type-check is used.")
@click.option("--strict-geom-type-check", is_flag=True, help="If enabled, geometry types must match exactly. Otherwise, MultiPolygon matches Polygon, etc.")
@click.option("--pick-filter-feature-id", type=int, default=None, help="Select a specific polygon feature by its 0-based index.")
@click.option("--pick-filter-feature-kv", type=str, default=None, multiple=True, help="Select a specific polygon feature by a key-value pair (e.g., 'key=value') from its properties.")
def filter_7z(input_7z, output_file, log_level, filter_file, filter_file_driver, bounds, no_clip, schema_file, limit_to_geom_type, strict_geom_type_check, output_driver, pick_filter_feature_id, pick_filter_feature_kv):
    """
    Processes a 7z archive containing a single geojsonl file by streaming.
    """
    logging.basicConfig(level=getattr(logging, log_level.upper()),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Processing {input_7z}")

    if filter_file and bounds:
        raise click.UsageError("Only one of --filter-file or --bounds can be used, not both.")
    if not (filter_file or bounds):
        raise click.UsageError("Either --filter-file or --bounds is required.")

    filter = create_filter(filter_file=filter_file, filter_file_driver=filter_file_driver, bounds=bounds, no_clip=no_clip, limit_to_geom_type=limit_to_geom_type, strict_geom_type_check=strict_geom_type_check, pick_filter_feature_id=pick_filter_feature_id, pick_filter_feature_kv=pick_filter_feature_kv)
    if filter is None:
        raise click.Abort() 

    if schema_file:
        schema_path = Path(schema_file)
        logging.info(f"Reading schema from {schema_path}")
        with schema_path.open('r') as f:
            schema = json.load(f)
    else:
        logging.info(f"Inferring schema from {input_7z}")
        schema = get_schema_from_archive(input_7z, limit_to_geom_type=limit_to_geom_type, strict_geom_type_check=strict_geom_type_check)
        if schema is None:
            logging.error("Could not infer schema from the archive.")
            return
        
    crs = CRS.from_epsg(4326)
    writer = create_writer(output_file, schema, crs, output_driver)
    if writer is None:
        return

    archive_path = input_7z
    if archive_path.endswith('.001'):
        base_path = archive_path.rsplit('.', 1)[0]
        with multivolumefile.open(base_path, 'rb') as multivolume_file:
            with py7zr.SevenZipFile(multivolume_file, 'r') as archive:
                process_archive(archive, filter, writer)
    else:
        with py7zr.SevenZipFile(archive_path, mode='r') as archive:
            process_archive(archive, filter, writer)
