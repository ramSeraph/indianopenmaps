"""
Common decorators for CLI commands.
"""

import functools

import click

from iomaps.core.helpers import get_driver_from_filename, get_supported_output_drivers

DEFAULT_ROUTES_URL = "https://indianopenmaps.fly.dev/api/routes"


def get_effective_driver(output_file, output_driver=None):
    """
    Get the effective output driver, inferring from filename if not specified.

    Args:
        output_file: Path to the output file
        output_driver: Optional explicit driver name

    Returns:
        str: The driver name, or None if it couldn't be determined
    """
    if output_driver:
        return output_driver
    return get_driver_from_filename(output_file)


def validate_output_driver(output_file, output_driver=None):
    """
    Validate and return the effective output driver.

    Args:
        output_file: Path to the output file
        output_driver: Optional explicit driver name

    Returns:
        str: The effective driver name

    Raises:
        click.UsageError: If the driver cannot be determined
    """
    from pathlib import Path

    driver = get_effective_driver(output_file, output_driver)
    if not driver:
        ext = Path(output_file).suffix.lower().lstrip(".")
        raise click.UsageError(
            f"Could not determine output file format from extension: {ext}, "
            "please specify driver explicitly using -d/--output-driver."
        )
    return driver


def add_output_options(func=None, *, exclude_drivers=None):
    """Decorator that adds common output file options to a command.

    Args:
        exclude_drivers: Optional list of driver names to exclude from choices.
    """
    if exclude_drivers is None:
        exclude_drivers = []

    drivers = [d for d in get_supported_output_drivers() if d.lower() not in [e.lower() for e in exclude_drivers]]

    def decorator(f):
        @click.option(
            "-o",
            "--output-file",
            required=True,
            type=click.Path(),
            help="Path for the output file where processed data will be saved.",
        )
        @click.option(
            "-d",
            "--output-driver",
            type=click.Choice(drivers, case_sensitive=False),
            help="Specify output driver. If not specified, it will be inferred from the output file extension.",
        )
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapper

    if func is not None:
        # Called without arguments: @add_output_options
        return decorator(func)
    # Called with arguments: @add_output_options(exclude_drivers=["parquet"])
    return decorator


def add_routes_options(func):
    """Decorator that adds common routes-related options to a command."""

    @click.option(
        "--routes-file",
        type=click.Path(exists=True),
        help="Path to local JSON file with routes. Cannot be used with --routes-url.",
    )
    @click.option(
        "--routes-url",
        type=str,
        help=f"URL to fetch routes from. Cannot be used with --routes-file. Default: {DEFAULT_ROUTES_URL}",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def add_log_level_option(default="WARNING"):
    """Decorator factory that adds a log-level option to a command."""

    def decorator(func):
        @click.option(
            "-l",
            "--log-level",
            default=default,
            type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], case_sensitive=False),
            help="Set the logging level.",
        )
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    return decorator


def add_filter_options(func):
    """Decorator to add common filtering options to Click commands."""

    @click.option(
        "-f",
        "--filter-file",
        type=click.Path(exists=True),
        default=None,
        help="Path to an input filter file (e.g., shapefile) for spatial filtering.",
    )
    @click.option(
        "--filter-file-driver",
        default=None,
        help="Specify the OGR driver for the filter file. If not specified, "
        "it will be inferred from the filter file extension.",
    )
    @click.option(
        "-b",
        "--bounds",
        help="Rectangular bounds for spatial filtering, e.g., 'min_lon,min_lat,max_lon,max_lat'. "
        "You can get bounding box coordinates from http://bboxfinder.com/.",
    )
    @click.option(
        "--pick-filter-feature-id",
        type=int,
        default=None,
        help="Select a specific polygon feature by its 0-based index.",
    )
    @click.option(
        "--pick-filter-feature-kv",
        type=str,
        default=None,
        multiple=True,
        help="Select a specific polygon feature by a key-value pair (e.g., 'key=value') from its properties.",
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper


def validate_routes_options(routes_file, routes_url):
    """Validates that routes-file and routes-url are not both specified."""
    if routes_file and routes_url:
        return False
    return True


def validate_filter_options(filter_file, bounds):
    """
    Validates that filter options are properly specified.

    Args:
        filter_file: Path to filter file or None
        bounds: Bounds string or None

    Raises:
        click.UsageError: If validation fails
    """
    if filter_file and bounds:
        raise click.UsageError("Only one of --filter-file or --bounds can be used, not both.")
