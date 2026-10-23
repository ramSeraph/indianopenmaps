"""Discover candidate sources for an area using cached coverage PNGs."""

import json
import sqlite3
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from iomaps.commands.decorators import DEFAULT_ROUTES_URL
from iomaps.commands.sources import filter_sources
from iomaps.core.coverage_index import CoverageIndex, CoverageIndexError, query_bounds


@click.command("search")
@click.option("--bounds", required=True, help="WGS84 bbox: west,south,east,north (within 67,5,98,38).")
@click.option("-c", "--category", multiple=True, help="Filter by category; repeat to match any category.")
@click.option("-s", "--search", "text_filter", help="Fuzzy filter on source name or route.")
@click.option("--json", "output_json", is_flag=True, help="Print matches, unknown coverage and cache status as JSON.")
@click.option("--force-update", "--refresh", is_flag=True, help="Refresh catalog and PNGs, using ETags when available.")
@click.option("--workers", type=click.IntRange(1, 32), default=8, show_default=True, help="Concurrent PNG downloads.")
@click.option(
    "--cache-dir",
    type=click.Path(file_okay=False, path_type=Path),
    envvar="IOMAPS_CACHE_DIR",
    help="Cache directory (default: platform cache/iomaps; or IOMAPS_CACHE_DIR).",
)
@click.option("--routes-file", type=click.Path(dir_okay=False, path_type=Path), help="Local source catalog JSON.")
@click.option("--routes-url", help="Alternative source catalog URL.")
def search(bounds, category, text_filter, output_json, force_update, workers, cache_dir, routes_file, routes_url):
    """Find sources whose coverage cells overlap a bbox; warm searches work offline.

    Cell matches are approximate. Missing coverage is reported as unknown.
    Use returned routes with 'cli extract --source' for actual features.
    """
    if routes_file and routes_url:
        raise click.UsageError("Only one of --routes-file or --routes-url can be used")
    try:
        requested, _ = query_bounds(bounds.split(","))
        catalog = routes_file.absolute().as_uri() if routes_file else (routes_url or DEFAULT_ROUTES_URL)
        index = CoverageIndex(catalog, cache_dir)
        index.ensure(force_update=force_update, workers=workers, progress=lambda message: click.echo(message, err=True))
        result = index.lookup(requested)
    except (CoverageIndexError, OSError, sqlite3.Error) as exc:
        raise click.ClickException(str(exc)) from exc
    result["matches"] = filter_sources(result["matches"], category, text_filter)
    result["unknown"] = filter_sources(result["unknown"], category, text_filter)
    if result["bounds"] != result["effective_bounds"]:
        click.echo("Bbox clipped to the supported India bounds (67,5,98,38).", err=True)
    if result["unknown"]:
        click.echo(f"Coverage unknown for {len(result['unknown'])} source(s) matching the filters.", err=True)
    if output_json:
        click.echo(json.dumps(result, indent=2))
        return
    table = Table(title=f"{len(result['matches'])} candidate sources (approximate coverage)")
    table.add_column("Name")
    table.add_column("Route")
    table.add_column("Categories")
    for source in result["matches"]:
        table.add_row(source["name"], source["route"], ", ".join(source["category"]))
    console = Console()
    console.print(table)
    if result["unknown"]:
        table = Table(title="Unknown coverage")
        table.add_column("Route")
        table.add_column("Reason")
        for source in result["unknown"]:
            table.add_row(source["route"], source["reason"])
        console.print(table)
    click.echo(f"Cached at {result['cached_at']}. Use --force-update to refresh.", err=True)


if __name__ == "__main__":
    search()
