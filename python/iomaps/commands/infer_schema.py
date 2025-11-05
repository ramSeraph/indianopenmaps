
import logging
from pathlib import Path
import json

import click

from iomaps.helpers import get_base_name
from iomaps.commands.schema_common import get_schema_from_archive

@click.command("infer-schema")
@click.option("-i", "--input-7z", required=True, type=click.Path(exists=True), help="Path to the input 7z archive file.")
@click.option("-o", "--output-file", type=click.Path(), help="Path for the output file where schema will be saved.")
@click.option("-l", "--log-level", default="INFO", type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False), help="Set the logging level.")
@click.option("-g", "--limit-to-geom-type", type=click.Choice(["Point", "LineString", "Polygon", "MultiPoint", "MultiLineString", "MultiPolygon", "GeometryCollection"], case_sensitive=False), help="Limit schema inference to a specific geometry type. MultiPolygon matches Polygon unless --strict-geom-type-check is used.")
@click.option("--strict-geom-type-check", is_flag=True, help="If enabled, geometry types must match exactly. Otherwise, MultiPolygon matches Polygon, etc.")
def infer_schema(input_7z, output_file, log_level, limit_to_geom_type, strict_geom_type_check):
    """
    Infers schema from a 7z archive containing a single geojsonl file.
    """
    logging.basicConfig(level=getattr(logging, log_level.upper()),
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info(f"Processing {input_7z} for schema inference.")

    input_path = Path(input_7z)
    if not input_path.exists():
        logging.error(f"The file {input_7z} does not exist.")
        return

    schema = get_schema_from_archive(input_path, limit_to_geom_type=limit_to_geom_type, strict_geom_type_check=strict_geom_type_check)
    if schema is None:
        logging.error("Failed to infer schema.")
        return

    base_name = get_base_name(input_path)

    if output_file:
        output_filename = output_file
    else:
        output_filename = input_path.with_name(f"{base_name}.schema.json")

    with open(output_filename, 'w') as f:
        json.dump(schema, f, indent=4)
    
    logging.info(f"Schema inferred and saved to {output_filename}")
