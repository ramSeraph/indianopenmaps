"""
Sources command for listing available data sources.
"""

import json
import logging
from pathlib import Path

import click
import requests
from rich.console import Console
from rich.table import Table

from iomaps.commands.decorators import (
    DEFAULT_ROUTES_URL,
    add_log_level_option,
    add_routes_options,
    validate_routes_options,
)


def _parse_routes_dict(routes_dict):
    """Converts routes dictionary to list format with route key added."""
    sources = []
    for route, config in routes_dict.items():
        config["route"] = route
        sources.append(config)
    return sources


def _load_routes_from_file(routes_file):
    """Loads routes from a local JSON file."""
    try:
        routes_path = Path(routes_file)
        logging.info(f"Loading routes from file: {routes_path}")
        with routes_path.open("r") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load routes from file: {e}")
        return None


def _fetch_routes_from_url(routes_url):
    """Fetches routes from a URL."""
    try:
        logging.info(f"Fetching routes from: {routes_url}")
        response = requests.get(routes_url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to fetch sources from API: {e}")
        return None


def fetch_sources_list(routes_url=None, routes_file=None):
    """
    Fetches the list of sources from URL or file.

    Args:
        routes_url: URL to fetch routes from (defaults to indianopenmaps API)
        routes_file: Path to local JSON file with routes

    Returns:
        list: List of source configurations with route added, or None on error
    """
    if routes_file:
        routes_dict = _load_routes_from_file(routes_file)
    else:
        routes_dict = _fetch_routes_from_url(routes_url or DEFAULT_ROUTES_URL)

    if routes_dict is None:
        return None

    return _parse_routes_dict(routes_dict)


def get_vector_sources(routes_url=None, routes_file=None):
    """
    Fetches and filters sources to only include non-raster sources.

    Args:
        routes_url: URL to fetch routes from (defaults to indianopenmaps API)
        routes_file: Path to local JSON file with routes

    Returns:
        list: List of vector (non-raster) source configurations, or None on error
    """
    sources_list = fetch_sources_list(routes_url=routes_url, routes_file=routes_file)
    if sources_list is None:
        return None
    return [s for s in sources_list if s.get("type") != "raster"]


def resolve_source(source_input, vector_sources):
    """
    Resolves a source input to a source configuration.

    Args:
        source_input: Can be:
            - A number (1-indexed position in list)
            - A route starting with "/"
            - A source name
        vector_sources: List of vector source configurations

    Returns:
        dict: Source configuration, or None if not found
    """
    # Check if it's a number (1-indexed)
    try:
        index = int(source_input)
        if 1 <= index <= len(vector_sources):
            return vector_sources[index - 1]
        else:
            logging.error(f"Source index {index} out of range (1-{len(vector_sources)})")
            return None
    except ValueError:
        pass

    # Check if it's a route (starts with "/")
    if source_input.startswith("/"):
        for source in vector_sources:
            if source.get("route") == source_input:
                return source
        logging.error(f"No source found with route '{source_input}'")
        return None

    # Otherwise, treat as a name
    for source in vector_sources:
        if source.get("name") == source_input:
            return source

    logging.error(f"No source found with name '{source_input}'")
    return None


def _get_source_categories(source):
    """Gets categories from a source as a list."""
    category = source.get("category")
    if category is None:
        return []
    if isinstance(category, list):
        return [c.lower() for c in category]
    return [category.lower()]


def _levenshtein_distance(s1, s2):
    """Calculate the Levenshtein edit distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # insertions, deletions, substitutions
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def _fuzzy_match(text, query, max_distance=2):
    """
    Check if query fuzzy matches text using edit distance.

    Args:
        text: Text to search in
        query: Search query
        max_distance: Maximum allowed edit distance (default 2)

    Returns:
        bool: True if match found
    """
    text_lower = text.lower()
    query_lower = query.lower()

    # Exact substring match
    if query_lower in text_lower:
        return True

    # Check each word in text against query using edit distance
    words = text_lower.replace("-", " ").replace("_", " ").split()
    for word in words:
        # Scale max distance based on word length
        allowed_distance = min(max_distance, len(word) // 3 + 1)
        if _levenshtein_distance(word, query_lower) <= allowed_distance:
            return True

    return False


def filter_sources(sources, categories=None, search=None):
    """
    Filter sources by category and/or search query.

    Args:
        sources: List of source configurations
        categories: List of category names to filter by (case-insensitive)
        search: Search query for fuzzy matching against name and route

    Returns:
        list: Filtered sources
    """
    filtered = sources

    # Filter by categories
    if categories:
        categories_lower = [c.lower() for c in categories]
        filtered = [
            s for s in filtered
            if any(cat in _get_source_categories(s) for cat in categories_lower)
        ]

    # Filter by search query
    if search:
        search_results = []
        for source in filtered:
            name = source.get("name", "")
            route = source.get("route", "")
            # Check name and route for fuzzy match
            if _fuzzy_match(name, search) or _fuzzy_match(route, search):
                search_results.append(source)
        filtered = search_results

    return filtered


def get_category_counts(sources):
    """Get category counts from sources."""
    counts = {}
    for source in sources:
        for cat in _get_source_categories(source):
            counts[cat] = counts.get(cat, 0) + 1
    return counts


@click.command("source-categories")
@add_routes_options
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output as JSON.",
)
@add_log_level_option()
def categories(routes_file, routes_url, output_json, log_level):
    """
    List available source categories.

    Shows all categories with the number of sources in each.
    Use with 'iomaps cli sources -c <category>' to filter sources.
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if not validate_routes_options(routes_file, routes_url):
        click.echo("Error: --routes-file and --routes-url are mutually exclusive.", err=True)
        return

    vector_sources = get_vector_sources(routes_url=routes_url, routes_file=routes_file)

    if vector_sources is None:
        click.echo("Failed to fetch sources.", err=True)
        return

    if not vector_sources:
        click.echo("No sources available.", err=True)
        return

    category_counts = get_category_counts(vector_sources)

    if not category_counts:
        click.echo("No categories found.", err=True)
        return

    # Sort by count descending, then by name
    sorted_cats = sorted(category_counts.items(), key=lambda x: (-x[1], x[0]))

    # JSON output
    if output_json:
        click.echo(f"Found {len(category_counts)} categories from {len(vector_sources)} sources", err=True)
        output = [{"category": cat, "count": count} for cat, count in sorted_cats]
        click.echo(json.dumps(output, indent=2))
        return

    # Create rich table (output to stderr)
    console = Console(stderr=True)
    table = Table(show_header=True, header_style="bold cyan", title="Source Categories")
    table.add_column("Category", style="magenta")
    table.add_column("Sources", justify="right", style="green")

    for cat, count in sorted_cats:
        table.add_row(cat, str(count))

    console.print(table)
    console.print(f"\nTotal: [bold]{len(category_counts)}[/bold] categories")
    console.print("Use with: [dim]iomaps cli sources -c <category>[/dim]")


@click.command("sources")
@add_routes_options
@click.option(
    "-c",
    "--category",
    "categories",
    multiple=True,
    help="Filter by category (can be specified multiple times).",
)
@click.option(
    "-s",
    "--search",
    type=str,
    help="Search sources by name or route (fuzzy matching with typo tolerance).",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    help="Output as JSON.",
)
@add_log_level_option()
def sources(routes_file, routes_url, categories, search, output_json, log_level):
    """
    List available data sources for the extract command.

    Sources can be referenced by number, route, or name when using
    the extract command's --source flag.

    Examples:

      iomaps cli sources                      # List all sources

      iomaps cli sources -c buildings         # Filter by category

      iomaps cli sources -s districts         # Search for "districts"

      iomaps cli sources -c states -s soi     # Combine filters
    """
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if not validate_routes_options(routes_file, routes_url):
        click.echo("Error: --routes-file and --routes-url are mutually exclusive.", err=True)
        return

    vector_sources = get_vector_sources(routes_url=routes_url, routes_file=routes_file)

    if vector_sources is None:
        click.echo("Failed to fetch sources.", err=True)
        return

    if not vector_sources:
        click.echo("No sources available.", err=True)
        return

    # Apply filters
    filtered_sources = filter_sources(vector_sources, categories=categories, search=search)

    if not filtered_sources:
        click.echo("No sources match the filter criteria.", err=True)
        if categories:
            click.echo("Run 'iomaps cli source-categories' to see available categories.", err=True)
        return

    # JSON output
    if output_json:
        click.echo(f"Found {len(filtered_sources)} of {len(vector_sources)} sources", err=True)
        output = []
        for source in filtered_sources:
            original_index = vector_sources.index(source) + 1
            output.append({
                "index": original_index,
                "name": source.get("name"),
                "route": source.get("route"),
                "category": source.get("category"),
            })
        click.echo(json.dumps(output, indent=2))
        return

    # Create rich table (output to stderr)
    console = Console(stderr=True)
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("#", style="dim", justify="right")
    table.add_column("Name", style="green")
    table.add_column("Route", style="blue")
    table.add_column("Category", style="magenta")

    # Add rows (numbering based on original list for extract command compatibility)
    for source in filtered_sources:
        original_index = vector_sources.index(source) + 1
        name = source.get("name", "N/A")
        route = source.get("route", "N/A")
        category = source.get("category")
        if isinstance(category, list):
            category = ", ".join(category)
        category = category or "N/A"
        table.add_row(str(original_index), name, route, category)

    console.print(table)
    console.print(f"\nShowing: [bold]{len(filtered_sources)}[/bold] of {len(vector_sources)} sources")
    console.print("Use with: [dim]iomaps cli extract --source <number|route|name>[/dim]")
