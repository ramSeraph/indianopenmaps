"""Create a dataset's geographic occupancy image from final GeoParquet assets."""

from pathlib import Path

import click

from iomaps.core.coverage import CoverageError, create_coverage


@click.command("coverage")
@click.argument("inputs", nargs=-1, required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option(
    "-o", "--output", required=True, type=click.Path(dir_okay=False, path_type=Path), help="Output coverage PNG."
)
@click.option(
    "--grid-size", type=click.IntRange(1, 10000), default=1000, show_default=True, help="Cells along each axis."
)
@click.option(
    "--batch-size", type=click.IntRange(1), default=8192, show_default=True, help="Geometry rows read per batch."
)
@click.option("--overwrite", is_flag=True, help="Replace an existing PNG after successful creation.")
def coverage(inputs, output, grid_size, batch_size, overwrite):
    """Create a 1-bit PNG of cells touched by a dataset's geometries.

    INPUTS are local GeoParquet files, or a .parquet.meta.json from preparation
    listing all partitions. Include every partition of the dataset. Bounds are
    fixed at 67,5,98,38 (west,south,east,north). White pixels mean occupied;
    image row zero is north. Shared edges and corners count as intersections.
    Features outside these bounds contribute nothing; crossing features
    contribute only inside the bounds.
    Invalid shapes use their feature bboxes and produce a warning.
    Unusable coordinates, malformed WKB and geometry-engine failures are warned about and skipped.
    """
    try:
        result = create_coverage(
            inputs,
            output,
            grid_size=grid_size,
            batch_size=batch_size,
            overwrite=overwrite,
            progress=lambda message: click.echo(message, err=True),
        )
    except (CoverageError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Created {output}: {grid_size} x {grid_size}, {int(result.cells.sum()):,} occupied cells")


if __name__ == "__main__":
    coverage()
