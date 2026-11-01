"""Public STAC catalog access and raster extraction."""

import json
from functools import wraps
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from iomaps.core.stac_client import DEFAULT_CATALOG_URL, RasterCatalog


def client_command(function):
    @click.pass_obj
    @wraps(function)
    def wrapped(options, *args, **kwargs):
        try:
            return function(RasterCatalog(**options), *args, **kwargs)
        except (click.ClickException, KeyboardInterrupt):
            raise
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc

    return wrapped


@click.group()
@click.option(
    "--catalog-url",
    default=DEFAULT_CATALOG_URL,
    envvar="IOMAPS_STAC_CATALOG",
    show_default=True,
    help="GitHub catalog file URL or a local JSON file; never a STAC API",
)
@click.option("--index", help="Use a release GeoParquet URL directly, without fetching any catalog")
@click.option("--collection-id", help="Collection ID for --index")
@click.option("--cache-dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--refresh", is_flag=True, help="Refresh cached catalog, collection metadata and release indexes")
@click.option("--offline", is_flag=True, help="Discover/search using cached metadata only; cannot extract imagery")
@click.pass_context
def stac(ctx, catalog_url, index, collection_id, cache_dir, refresh, offline):
    """Search release indexes locally and extract imagery directly from COGs."""
    if bool(index) != bool(collection_id):
        raise click.UsageError("--index and --collection-id must be used together")
    ctx.obj = {
        "url": catalog_url,
        "indexes": {collection_id: index} if index else None,
        "cache_dir": cache_dir,
        "refresh": refresh,
        "offline": offline,
    }


@stac.command("collections")
@click.option("--json", "output_json", is_flag=True)
@client_command
def collections(catalog, output_json):
    """List available raster collections."""
    collections = catalog.collections()
    if output_json:
        click.echo(json.dumps({"collections": collections}, indent=2))
        return
    table = Table(title="Raster collections")
    table.add_column("ID")
    table.add_column("Title")
    for collection in collections:
        table.add_row(collection["id"], collection.get("title", collection["id"]))
    Console().print(table)


@stac.command("search")
@click.option("-c", "--collection", multiple=True, help="Collection ID; repeat to search multiple collections")
@click.option("-b", "--bounds", help="WGS84 bbox: west,south,east,north")
@click.option("--datetime", help="ISO date, timestamp or interval (open endpoints use ..)")
@click.option("--max-items", type=click.IntRange(1), help="Optional total result limit; otherwise return all matches")
@click.option("--json", "output_json", is_flag=True, help="Print a GeoJSON/STAC ItemCollection")
@client_command
def search(catalog, collection, bounds, datetime, max_items, output_json):
    """Find raster sheets intersecting an area."""
    items = catalog.search(collections=list(collection) or None, bounds=bounds, datetime=datetime, max_items=max_items)
    if output_json:
        click.echo(json.dumps({"type": "FeatureCollection", "features": list(items)}, indent=2))
        return
    table = Table(title="Matching raster sheets")
    for name in ("Collection", "Sheet", "Datetime", "Assets"):
        table.add_column(name)
    for item in items:
        table.add_row(
            item["collection"], item["id"], str(item["properties"].get("datetime", "")), ", ".join(item["assets"])
        )
    Console().print(table)


@stac.command("item")
@click.argument("collection")
@click.argument("item_id")
@client_command
def item(catalog, collection, item_id):
    """Print a sheet's STAC metadata and download URLs."""
    click.echo(json.dumps(catalog.item(collection, item_id), indent=2))


@stac.command("extract")
@click.option("-c", "--collection", required=True)
@click.option("-b", "--bounds", required=True, help="WGS84 bbox: west,south,east,north")
@click.option("-o", "--output", required=True, type=click.Path(dir_okay=False, path_type=Path))
@click.option("--asset", help="Asset key; defaults to cog, then data")
@click.option("--datetime", help="Restrict source sheets by their catalog datetime")
@click.option("--crs", help="Output CRS; defaults to the first sheet's CRS")
@click.option("--resolution", type=click.FloatRange(min=0, min_open=True), help="Pixel size in output CRS units")
@click.option("--resampling", type=click.Choice(["near", "bilinear", "cubic", "average", "mode"]), default="near")
@click.option("--max-sheets", type=click.IntRange(1), default=1000, show_default=True)
@click.option("--overwrite", is_flag=True)
@client_command
def extract(catalog, **options):
    """Read COG windows, mosaic and crop into a GeoTIFF. Requires GDAL.

    Elevation values retain their numeric type. Overlapping sheets are processed
    in item-ID order, with later valid pixels winning. Uses HTTP range reads.
    """
    result = catalog.extract(**options)
    click.echo(json.dumps(result, indent=2))
