import json
import logging
from pathlib import Path

import click

from iomaps.commands.schema_common import get_schema_from_archive
from iomaps.core.helpers import get_base_name


@click.command("infer-schema")
@click.option(
    "-i",
    "--input-7z",
    required=True,
    type=click.Path(exists=True),
    help="Path to the input 7z archive file.",
)
@click.option(
    "-o",
    "--output-file",
    type=click.Path(),
    help="Path for the output file where schema will be saved.",
)
@click.option(
    "-l",
    "--log-level",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
    help="Set the logging level.",
)
def infer_schema(input_7z, output_file, log_level):
    """
    Infers schema from a 7z archive containing a single geojsonl file.
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info(f"Processing {input_7z} for schema inference.")

    input_path = Path(input_7z)
    if not input_path.exists():
        logging.error(f"The file {input_7z} does not exist.")
        return

    schema, renames = get_schema_from_archive(input_path)
    if schema is None:
        logging.error("Failed to infer schema.")
        raise click.Abort()

    # Include renames in schema file if there are any
    if renames:
        schema["property_renames"] = renames

    base_name = get_base_name(input_path)

    if output_file:
        output_filename = output_file
    else:
        output_filename = input_path.with_name(f"{base_name}.schema.json")

    with open(output_filename, "w") as f:
        json.dump(schema, f, indent=4)

    logging.info(f"Schema inferred and saved to {output_filename}")
