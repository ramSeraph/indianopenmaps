"""
Prepare a geojsonl file for distribution: compress and convert to mbtiles/pmtiles.
"""

import copy
import json
import math
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import click
import duckdb

from iomaps.core.geometry_validation import (
    CoordinateError,
    validate_geojsonl,
    validate_geometry_coordinates,
    validate_parquet_coordinates,
)

TWO_GB = 2 * 1024 * 1024 * 1024

GPIO_VERSION = "0.9.0"
# GPIO 0.9.0 uses Arrow fetch methods removed in DuckDB 2. Keep this
# dependency local to its uvx environment; iomaps itself uses DuckDB 2.
GPIO_DUCKDB_VERSION = "1.5.6"


def gpio_command(*args: str) -> list[str]:
    return [
        "uvx",
        "--from",
        f"geoparquet-io=={GPIO_VERSION}",
        "--with",
        f"duckdb=={GPIO_DUCKDB_VERSION}",
        "gpio",
        *args,
    ]


def run_cmd(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    """Run a command, printing it first."""
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, **kwargs)


def calculate_partition_count(file_size_bytes: int) -> int:
    """Calculate the number of partitions needed (must be power of 2)."""
    if file_size_bytes <= TWO_GB:
        return 1
    min_partitions = math.ceil(file_size_bytes / TWO_GB)
    power = math.ceil(math.log2(min_partitions))
    return 2**power


def filter_empty_geometries(geojsonl_file: Path, check_india: bool = True) -> tuple[int, int, Path]:
    """Filter out features with empty or null geometries from a geojsonl file.
    Also renames properties containing 'geom' to avoid DuckDB geometry detection issues.
    Valid WGS84 coordinates are retained anywhere in the world. If enabled,
    India-based repairs are attempted only for out-of-range coordinate values.
    Writes a separate cleaned file; the original is left intact.

    Returns: (original_count, filtered_count, cleaned_file_path)
    """
    cleaned_file = geojsonl_file.parent / f"{geojsonl_file.stem}.cleaned.geojsonl"
    original_count = 0
    filtered_count = 0
    fixed_count = 0
    dropped_count = 0
    renamed_props: dict[str, str] = {}
    rename_counter = 0
    bbox = INDIA_BBOX if check_india else None

    print("cleaning features (fixing coordinates, filtering empty geometries)...")

    with open(geojsonl_file) as infile, open(cleaned_file, "w") as outfile:
        for line in infile:
            original_count += 1
            line = line.strip()
            if not line:
                continue

            try:
                feature = json.loads(line)
                if not isinstance(feature, dict) or feature.get("type") != "Feature" or "geometry" not in feature:
                    raise CoordinateError("expected a GeoJSON Feature with a geometry member")
                geometry = feature["geometry"]
                try:
                    coordinate_count = validate_geometry_coordinates(geometry)
                except CoordinateError as exc:
                    if not bbox or not exc.repairable:
                        raise
                    try:
                        fixed_geom, fix_info = _fix_geometry_coords(geometry, bbox)
                        if fixed_geom is None:
                            raise exc
                        coordinate_count = validate_geometry_coordinates(fixed_geom)
                    except (TypeError, ValueError, OverflowError, IndexError) as fix_exc:
                        raise CoordinateError(f"unfixable coordinates: {fix_exc}") from fix_exc
                    feature["geometry"] = fixed_geom
                    fixed_count += 1
                    for desc in fix_info:
                        print(f"  fixed {_feature_label(feature)} at line {original_count}: {desc}")
                if not coordinate_count:
                    raise CoordinateError("empty or null geometry")

                # Rename properties containing 'geom' to avoid GDAL confusion.
                props = feature.get("properties", {})
                if props:
                    new_props = {}
                    for key, value in props.items():
                        if "geom" in key.lower():
                            if key not in renamed_props:
                                renamed_props[key] = f"geom_renamed_{rename_counter}"
                                rename_counter += 1
                            new_props[renamed_props[key]] = value
                        else:
                            new_props[key] = value
                    feature["properties"] = new_props

                # Valid geometry, write it
                outfile.write(json.dumps(feature) + "\n")
                filtered_count += 1

            except (json.JSONDecodeError, CoordinateError, RecursionError) as exc:
                dropped_count += 1
                print(f"  dropped {geojsonl_file}, line {original_count}: {exc}")
                continue

    print(f"  original features: {original_count}")
    print(f"  after cleaning: {filtered_count}")
    print(f"  fixed coordinates: {fixed_count}")
    print(f"  dropped: {dropped_count}")
    if renamed_props:
        print(f"  renamed properties: {renamed_props}")

    return original_count, filtered_count, cleaned_file


# --- Coordinate validation & fixing (India-specific) ---

# Rough bounding box for India (with padding for all claimed territories)
INDIA_BBOX = {"minx": 67, "miny": 5, "maxx": 98, "maxy": 38}


def _feature_label(feature):
    """Return a short label for identifying a feature."""
    props = feature.get("properties") or {}
    for key in ("HAB_NAME", "NAME", "name", "id", "ID"):
        if key in props and props[key] is not None:
            return str(props[key])
    for v in props.values():
        if isinstance(v, str) and v:
            return v
    return "(no name)"


def _extract_coords(geometry):
    """Recursively extract all [lon, lat] pairs from a GeoJSON geometry."""
    gtype = geometry.get("type", "")
    coords = geometry.get("coordinates")

    if gtype == "Point":
        yield coords
    elif gtype in ("MultiPoint", "LineString"):
        yield from coords
    elif gtype in ("MultiLineString", "Polygon"):
        for ring in coords:
            yield from ring
    elif gtype == "MultiPolygon":
        for polygon in coords:
            for ring in polygon:
                yield from ring
    elif gtype == "GeometryCollection":
        for geom in geometry.get("geometries", []):
            yield from _extract_coords(geom)


def _in_bbox(lon, lat, bb):
    return bb["minx"] <= lon <= bb["maxx"] and bb["miny"] <= lat <= bb["maxy"]


def _coords_in_bbox(geometry, bb):
    """Check if all coordinate pairs in a geometry fall within the bounding box."""
    for pair in _extract_coords(geometry):
        if not isinstance(pair, (list, tuple)) or len(pair) < 2:
            return False
        lon, lat = pair[0], pair[1]
        if not isinstance(lon, (int, float)) or not isinstance(lat, (int, float)):
            return False
        if not _in_bbox(lon, lat, bb):
            return False
    return True


def _try_fix_decimal(value, min_val, max_val):
    """Try adjusting decimal placement so value falls within [min_val, max_val]."""
    if min_val <= value <= max_val:
        return value
    if abs(value) > 1e15:
        return None

    s = f"{abs(value):.10f}".rstrip("0").rstrip(".")
    digits = s.replace(".", "")
    digits = digits.lstrip("0") or "0"
    negative = value < 0

    for i in range(1, min(len(digits), 5)):
        try:
            candidate = float(digits[:i] + "." + digits[i:])
        except ValueError:
            continue
        if negative:
            candidate = -candidate
        if min_val <= candidate <= max_val:
            return candidate
    return None


def _try_fix_coord_pair(lon, lat, bb):
    """Attempt to repair a (lon, lat) pair so it falls inside bb.

    Returns (fixed_lon, fixed_lat, description) or None when unfixable.
    """
    if _in_bbox(lon, lat, bb):
        return lon, lat, None

    # Identical values — unfixable
    if lon == lat:
        return None

    # Strategy 1: swap lat/lon
    if _in_bbox(lat, lon, bb):
        return lat, lon, f"swapped lat/lon: ({lon}, {lat}) → ({lat}, {lon})"

    # Strategy 2: fix decimal placement
    f_lon = _try_fix_decimal(lon, bb["minx"], bb["maxx"])
    f_lat = _try_fix_decimal(lat, bb["miny"], bb["maxy"])
    if f_lon is not None and f_lat is not None:
        parts = []
        if f_lon != lon:
            parts.append(f"lon {lon} → {f_lon}")
        if f_lat != lat:
            parts.append(f"lat {lat} → {f_lat}")
        if parts:
            return f_lon, f_lat, "decimal fix: " + ", ".join(parts)

    # Strategy 3: swap first, then fix decimals
    f_lon_s = _try_fix_decimal(lat, bb["minx"], bb["maxx"])
    f_lat_s = _try_fix_decimal(lon, bb["miny"], bb["maxy"])
    if f_lon_s is not None and f_lat_s is not None:
        parts = ["swapped"]
        if f_lon_s != lat:
            parts.append(f"lon {lat} → {f_lon_s}")
        if f_lat_s != lon:
            parts.append(f"lat {lon} → {f_lat_s}")
        return (
            f_lon_s,
            f_lat_s,
            " + decimal fix: ".join([parts[0], ", ".join(parts[1:])]) if len(parts) > 1 else parts[0],
        )

    return None


def _fix_geometry_coords(geometry, bb):
    """Walk a geometry, attempt to fix every coordinate pair.

    Returns (fixed_geometry, list_of_descriptions) or (None, list_of_failure_reasons).
    """
    gtype = geometry.get("type", "")
    fixes = []
    failures = []

    def fix_pair(pair, idx):
        try:
            validate_geometry_coordinates({"type": "Point", "coordinates": pair})
            # A valid position may be outside India even when another position
            # in this feature needs repairing. Keep it exactly as supplied.
            return list(pair)
        except CoordinateError as exc:
            if not exc.repairable:
                failures.append(str(exc))
                return None
        lon, lat = pair[0], pair[1]
        result = _try_fix_coord_pair(lon, lat, bb)
        if result is None:
            failures.append(f"unfixable ({lon}, {lat})")
            return None
        f_lon, f_lat, desc = result
        if desc:
            fixes.append(desc)
        extra = pair[2:] if len(pair) > 2 else []
        return [f_lon, f_lat] + extra

    def fix_ring(ring, base_idx):
        out = []
        for j, pair in enumerate(ring):
            fixed = fix_pair(pair, base_idx + j)
            if fixed is None:
                return None, base_idx + len(ring)
            out.append(fixed)
        return out, base_idx + len(ring)

    geom = copy.deepcopy(geometry)

    if gtype == "Point":
        fixed = fix_pair(geom["coordinates"], 0)
        if fixed is None:
            return None, failures
        geom["coordinates"] = fixed

    elif gtype in ("MultiPoint", "LineString"):
        new_coords = []
        for i, pair in enumerate(geom["coordinates"]):
            fixed = fix_pair(pair, i)
            if fixed is None:
                return None, failures
            new_coords.append(fixed)
        geom["coordinates"] = new_coords

    elif gtype in ("MultiLineString", "Polygon"):
        idx = 0
        new_rings = []
        for ring in geom["coordinates"]:
            fixed_ring, idx = fix_ring(ring, idx)
            if fixed_ring is None:
                return None, failures
            new_rings.append(fixed_ring)
        geom["coordinates"] = new_rings

    elif gtype == "MultiPolygon":
        idx = 0
        new_polys = []
        for polygon in geom["coordinates"]:
            new_rings = []
            for ring in polygon:
                fixed_ring, idx = fix_ring(ring, idx)
                if fixed_ring is None:
                    return None, failures
                new_rings.append(fixed_ring)
            new_polys.append(new_rings)
        geom["coordinates"] = new_polys

    elif gtype == "GeometryCollection":
        new_geoms = []
        for sub in geom.get("geometries", []):
            fixed_sub, sub_info = _fix_geometry_coords(sub, bb)
            if fixed_sub is None:
                failures.extend(sub_info)
                return None, failures
            fixes.extend(sub_info)
            new_geoms.append(fixed_sub)
        geom["geometries"] = new_geoms
        return geom, fixes

    if failures:
        return None, failures
    return geom, fixes


def get_parquet_metadata(parquet_path: Path) -> tuple[dict | None, dict | None]:
    """Get parquet metadata from a file using gpio inspect meta."""
    geo_meta = None
    parquet_meta = None

    try:
        result = run_cmd(
            gpio_command("inspect", "meta", "--json", str(parquet_path)),
            capture_output=True,
            text=True,
            check=True,
        )
        geo_meta = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        pass

    try:
        result = run_cmd(
            gpio_command("inspect", "meta", "--parquet", "--json", str(parquet_path)),
            capture_output=True,
            text=True,
            check=True,
        )
        parquet_meta = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        pass

    return geo_meta, parquet_meta


def extract_schema(parquet_meta: dict) -> dict:
    """Extract schema (column descriptions) from parquet metadata."""
    schema = {}
    schema_str = parquet_meta.get("schema", "")
    if not schema_str:
        return schema
    parts = schema_str.split(", ")
    for part in parts:
        if ": " not in part:
            continue
        name, type_str = part.split(": ", 1)
        if name == "schema":
            continue
        schema[name] = {"type": type_str}
    return schema


def extract_bbox(geo_meta: dict) -> list | None:
    """Extract bounding box from geoparquet metadata."""
    geoparquet_meta = geo_meta.get("geoparquet_metadata", {})
    columns = geoparquet_meta.get("columns", {})
    primary = geoparquet_meta.get("primary_column", "geometry")
    if primary in columns:
        return columns[primary].get("bbox")
    for col_data in columns.values():
        if "bbox" in col_data:
            return col_data["bbox"]
    return None


def extract_geometry_types(geo_meta: dict) -> list | None:
    """Extract geometry types from geoparquet metadata."""
    geoparquet_meta = geo_meta.get("geoparquet_metadata", {})
    columns = geoparquet_meta.get("columns", {})
    primary = geoparquet_meta.get("primary_column", "geometry")
    if primary in columns:
        return columns[primary].get("geometry_types")
    for col_data in columns.values():
        if "geometry_types" in col_data:
            return col_data["geometry_types"]
    return None


def create_parquet_meta(lname: str, parquet_files: list[Path]) -> None:
    """Create meta.json for partitioned parquet files."""
    meta = {"schema": {}, "geometry_types": set(), "extents": {}}

    for pf in parquet_files:
        geo_meta, parquet_meta = get_parquet_metadata(pf)
        if parquet_meta:
            for col_name, col_info in extract_schema(parquet_meta).items():
                if col_name not in meta["schema"]:
                    meta["schema"][col_name] = col_info
        if geo_meta:
            bbox = extract_bbox(geo_meta)
            if bbox:
                meta["extents"][pf.name] = {
                    "minx": bbox[0],
                    "miny": bbox[1],
                    "maxx": bbox[2],
                    "maxy": bbox[3],
                }
            geometry_types = extract_geometry_types(geo_meta)
            if geometry_types:
                meta["geometry_types"].update(geometry_types)

    # Convert set to sorted list for JSON serialization
    meta["geometry_types"] = sorted(meta["geometry_types"])

    meta_file = Path(f"{lname}.parquet.meta.json")
    with open(meta_file, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"created: {meta_file.name}")


def analyze_unique_fields(parquet_files: list[Path], memory_limit=None) -> dict:
    """
    Analyze parquet files to find which fields can uniquely identify features.

    Returns dict with field analysis results.
    """
    if not parquet_files:
        return {"error": "No parquet files provided"}

    # Query Parquet directly: materializing all geometries/properties duplicates
    # the dataset and can exhaust RAM or spill space on large server-side runs.
    config = {}
    if memory_limit:
        config = {
            "memory_limit": memory_limit,
            "threads": 1,
            "temp_directory": tempfile.gettempdir(),
            "preserve_insertion_order": False,
        }
    conn = duckdb.connect(config=config)
    file_list = [str(f) for f in parquet_files]

    try:
        conn.read_parquet(file_list).create_view("data")

        total_rows = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        columns_result = conn.execute("DESCRIBE data").fetchall()
        property_columns = [col[0] for col in columns_result if col[0] != "geometry"]

        results = {"total_features": total_rows, "fields": {}}

        for col in property_columns:
            try:
                distinct_count = conn.execute(f'SELECT COUNT(DISTINCT "{col}") FROM data').fetchone()[0]
                null_count = conn.execute(f'SELECT COUNT(*) FROM data WHERE "{col}" IS NULL').fetchone()[0]

                results["fields"][col] = {
                    "distinct_values": distinct_count,
                    "null_count": null_count,
                    "is_unique": distinct_count == total_rows and null_count == 0,
                }
            except Exception as e:
                results["fields"][col] = {"error": str(e)}

        conn.close()
        return results

    except Exception as e:
        conn.close()
        return {"error": str(e)}


def score_field_name(name: str) -> int | None:
    """Score a field name for ID-likeness. Higher is better. Returns None to exclude."""
    name_lower = name.lower()

    shape_patterns = ["shape", "length", "area", "perimeter", "centroid", "st_area", "st_length", "starea", "stlength"]
    if any(p in name_lower for p in shape_patterns):
        return None

    if name_lower == "id":
        return 100
    if name_lower == "uid" or name_lower == "uuid":
        return 95
    if name_lower.endswith("_id") or name_lower.endswith("id"):
        return 80
    if name_lower.startswith("id_") or name_lower.startswith("id"):
        return 75
    if "id" in name_lower:
        return 60
    if "code" in name_lower:
        return 50
    if "key" in name_lower:
        return 45
    if "name" in name_lower:
        return 20
    if any(x in name_lower for x in ["objectid", "fid", "inpoly", "simpgn", "simptol"]):
        return -5

    return 0


def pick_best_id(analysis: dict, threshold: float = 0.95) -> dict:
    """
    Pick the best unique ID field from analysis results.

    Returns dict with:
        - field: chosen field name (or None)
        - is_unique: whether field is truly unique
        - uniqueness_ratio: distinct_values / total_features
        - reason: explanation for choice
    """
    fields = analysis.get("fields", {})
    total = analysis.get("total_features", 0)

    if not fields or total == 0:
        return {"field": None, "reason": "no fields or no features"}

    unique_fields = []
    candidate_fields = []
    all_fields = []

    for name, info in fields.items():
        if isinstance(info, dict) and "error" not in info:
            name_score = score_field_name(name)
            if name_score is None:
                continue

            distinct = info.get("distinct_values", 0)
            null_count = info.get("null_count", 0)
            is_unique = info.get("is_unique", False)
            ratio = distinct / total if total > 0 else 0

            field_info = {
                "name": name,
                "distinct": distinct,
                "null_count": null_count,
                "ratio": ratio,
                "is_unique": is_unique,
                "name_score": name_score,
            }

            all_fields.append(field_info)

            if is_unique:
                unique_fields.append(field_info)
            elif ratio >= threshold:
                candidate_fields.append(field_info)

    if unique_fields:
        unique_fields.sort(key=lambda x: (-x["name_score"], len(x["name"])))
        best = unique_fields[0]
        return {
            "field": best["name"],
            "is_unique": True,
            "uniqueness_ratio": best["ratio"],
            "reason": f"unique field with name_score={best['name_score']}",
        }

    if candidate_fields:
        candidate_fields.sort(key=lambda x: (-x["ratio"], -x["name_score"]))
        best = candidate_fields[0]
        return {
            "field": best["name"],
            "is_unique": False,
            "uniqueness_ratio": best["ratio"],
            "reason": f"best candidate above threshold ({threshold})",
        }

    if all_fields:
        all_fields.sort(key=lambda x: (-x["ratio"], x["null_count"], -x["name_score"]))
        best = all_fields[0]
        return {
            "field": None,
            "reason": f"no unique or high-cardinality fields above {threshold}",
            "best_below_threshold": {"field": best["name"], "ratio": best["ratio"], "null_count": best["null_count"]},
        }

    return {"field": None, "reason": f"no unique or high-cardinality fields above {threshold}"}


def create_field_analysis(lname: str, parquet_files: list[Path], memory_limit=None) -> None:
    """Analyze parquet files and create field analysis JSON with picked unique ID."""
    print("analyzing fields for unique ID...")
    analysis = analyze_unique_fields(parquet_files, memory_limit=memory_limit)

    if "error" in analysis:
        print(f"  error: {analysis['error']}")
        return

    picked = pick_best_id(analysis)

    result = {"base_name": lname, "analysis": analysis, "picked_id": picked}

    analysis_file = Path(f"{lname}.analysis.json")
    with open(analysis_file, "w") as f:
        json.dump(result, f, indent=2)
    print(f"created: {analysis_file.name}")

    if picked.get("field"):
        unique_str = "unique" if picked.get("is_unique") else f"{picked.get('uniqueness_ratio', 0):.2%}"
        print(f"  picked ID: {picked['field']} ({unique_str})")
    else:
        print(f"  no suitable ID field found: {picked.get('reason', 'unknown')}")
        print("  available fields and their uniqueness:")
        fields = analysis.get("fields", {})
        total = analysis.get("total_features", 0)
        for name, info in sorted(
            fields.items(), key=lambda x: x[1].get("distinct_values", 0) if isinstance(x[1], dict) else 0, reverse=True
        ):
            if isinstance(info, dict) and "error" not in info:
                distinct = info.get("distinct_values", 0)
                null_count = info.get("null_count", 0)
                ratio = distinct / total if total > 0 else 0
                is_unique = info.get("is_unique", False)
                unique_str = "unique" if is_unique else f"{ratio:.2%}"
                null_str = f", {null_count} nulls" if null_count > 0 else ""
                print(f"    {name}: {unique_str}{null_str}")


@click.command("prepare")
@click.argument("file", type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.argument("src_url", required=False)
@click.argument("src_name", required=False)
@click.option("--points", is_flag=True, help="Use drop-densest-as-needed for point data")
@click.option(
    "--tile-reduction",
    type=click.Choice(["coalesce-smallest", "drop-densest", "drop-smallest", "drop-fraction"]),
    help="Override tile size reduction (default: coalesce-smallest, or drop-densest with --points)",
)
@click.option(
    "--tippecanoe-option",
    multiple=True,
    help="Additional Tippecanoe option; repeat as --tippecanoe-option=--option=value",
)
@click.option("--no-zip", is_flag=True, help="Skip creating the 7z archive")
@click.option("--no-pmtiles", is_flag=True, help="Skip creating mbtiles and pmtiles")
@click.option("--no-parquet", is_flag=True, help="Skip creating geoparquet file")
@click.option("--no-filter", is_flag=True, help="Skip input cleaning; GeoParquet coordinate validation still runs")
@click.option(
    "--coverage/--no-coverage",
    default=True,
    show_default=True,
    help="Create a 1-bit coverage PNG from the final GeoParquet files",
)
@click.option(
    "--coverage-grid-size",
    type=click.IntRange(1, 10000),
    default=1000,
    show_default=True,
    help="Coverage cells along each axis of the fixed India grid",
)
@click.option("--bounded-parquet", is_flag=True, help="Use disk-backed DuckDB conversion for large 2D WGS84 GeoJSONL")
@click.option("--parquet-memory-limit", default="256MB", show_default=True, help="DuckDB limit with --bounded-parquet")
@click.option(
    "--parquet-resume-db",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Resume a completed bounded import from its checkpoint database",
)
@click.option(
    "--no-india", is_flag=True, help="Disable India-based coordinate repairs; worldwide WGS84 validation still runs"
)
def prepare(
    file,
    src_url,
    src_name,
    points,
    tile_reduction,
    tippecanoe_option,
    no_zip,
    no_pmtiles,
    no_parquet,
    no_filter,
    no_india,
    bounded_parquet,
    parquet_memory_limit,
    parquet_resume_db,
    coverage,
    coverage_grid_size,
):
    """Prepare a GeoJSONL file for distribution."""
    if file.suffix != ".geojsonl":
        raise click.UsageError(f"{file} is not a geojsonl file")
    if coverage and no_parquet and click.get_current_context().get_parameter_source("coverage").name == "COMMANDLINE":
        raise click.UsageError("--coverage requires Parquet creation; use 'admin coverage' for existing Parquet files")
    if parquet_resume_db:
        if not bounded_parquet or no_parquet or not no_filter or not no_zip or not no_pmtiles:
            raise click.UsageError("--parquet-resume-db requires --bounded-parquet --no-filter --no-zip --no-pmtiles")
        parquet_resume_db = parquet_resume_db.resolve()

    if not no_pmtiles and (not src_url or not src_name):
        raise click.UsageError("SRC_URL and SRC_NAME are required unless --no-pmtiles is set")

    fbase = file.name
    lname = file.stem
    work_dir = file.parent

    os.chdir(work_dir)

    # Disabling cleaning must not allow bad coordinates into GeoParquet. The
    # bounded converter validates its imported table, including resumed imports,
    # without another scan of the potentially very large source GeoJSONL.
    if no_filter and not no_parquet and not bounded_parquet:
        try:
            validate_geojsonl(Path(fbase))
        except CoordinateError as exc:
            raise click.ClickException(str(exc)) from exc

    # Create archive from ORIGINAL data (before any cleaning)
    if not no_zip:
        print("creating archive")
        run_cmd(["7z", "a", "-v2000m", "-m0=PPMd", f"{fbase}.7z", fbase], check=True)
        # If only one split was created, rename to drop the .001 suffix
        split_file = Path(f"{fbase}.7z.001")
        if split_file.exists() and not Path(f"{fbase}.7z.002").exists():
            split_file.rename(f"{fbase}.7z")
            print(f"created: {fbase}.7z")
        else:
            # List all created split files
            for f in sorted(Path(".").glob(f"{fbase}.7z.*")):
                print(f"created: {f.name}")

    # Create cleaned file for pmtiles/parquet (fixes coords, drops unfixable)
    needs_clean = not no_filter and (not no_pmtiles or not no_parquet)
    cleaned_file = None
    if needs_clean:
        _, _, cleaned_file = filter_empty_geometries(Path(fbase), check_india=not no_india)
        tile_input = str(cleaned_file)
    else:
        tile_input = fbase

    try:
        # Create mbtiles and pmtiles unless --no-pmtiles is set
        if not no_pmtiles:
            # Choose tippecanoe command based on points vs polygons
            reduction = tile_reduction or ("drop-densest" if points else "coalesce-smallest")
            reduction_options = [f"--{reduction}-as-needed"]
            # Adaptive gamma and adaptive dropping both reduce feature density.
            # Preserve existing defaults; explicit dropping replaces gamma.
            if tile_reduction is None or reduction == "coalesce-smallest":
                reduction_options.append("--increase-gamma-as-needed")

            attribution = f'Source: <a href="{src_url}" target="_blank" rel="noopener noreferrer">{src_name}</a>'

            print("creating mbtiles file")
            run_cmd(
                [
                    "tippecanoe",
                    "--temporary-directory",
                    tempfile.gettempdir(),
                    "-P",
                    "-S",
                    "10",
                    "-zg",
                    "-o",
                    f"{lname}.mbtiles",
                    "--simplify-only-low-zooms",
                    *reduction_options,
                    "--extend-zooms-if-still-dropping",
                    "-n",
                    lname,
                    "-l",
                    lname,
                    "-A",
                    attribution,
                    *tippecanoe_option,
                    tile_input,
                ],
                check=True,
            )

            # Convert to pmtiles
            print("converting to pmtiles")
            run_cmd(["pmtiles", "convert", f"{lname}.mbtiles", f"{lname}.pmtiles"], check=True)

            pmtiles_file = Path(f"{lname}.pmtiles")
            if pmtiles_file.stat().st_size > TWO_GB:
                print("pmtiles file > 2GB, splitting...")
                split_dir = Path("split")
                split_dir.mkdir(exist_ok=True)
                run_cmd(
                    [
                        "uvx",
                        "--from",
                        "pmtiles-mosaic",
                        "partition",
                        "--from-source",
                        str(pmtiles_file),
                        "--to-pmtiles",
                        str(split_dir / pmtiles_file.name),
                    ],
                    check=True,
                )
                # Move split files and mosaic.json to current directory and remove original
                for f in split_dir.glob("*.pmtiles"):
                    shutil.move(str(f), f.name)
                    print(f"created: {f.name}")
                for f in split_dir.glob("*.mosaic.json"):
                    shutil.move(str(f), f.name)
                    print(f"created: {f.name}")
                split_dir.rmdir()
                pmtiles_file.unlink()
            else:
                print(f"created: {pmtiles_file.name}")

        # Create geoparquet file unless --no-parquet is set
        if not no_parquet:
            print("creating geoparquet file")
            parquet_file = Path(f"{lname}.parquet")
            if bounded_parquet:
                from .admin_parquet import bounded_parquet as convert_bounded

                parquet_files = convert_bounded(
                    tile_input,
                    parquet_file,
                    memory_limit=parquet_memory_limit,
                    max_bytes=TWO_GB,
                    resume_db=parquet_resume_db,
                )
                create_field_analysis(lname, parquet_files, memory_limit=parquet_memory_limit)
                if len(parquet_files) > 1:
                    create_parquet_meta(lname, parquet_files)
            else:
                # Stage conversion so invalid output cannot become a release
                # asset, and a failed rerun preserves the previous Parquet.
                with tempfile.TemporaryDirectory(prefix=f".{lname}.parquet-", dir=".") as stage:
                    staged_parquet = Path(stage) / parquet_file.name
                    run_cmd(
                        gpio_command(
                            "convert",
                            "--geoparquet-version",
                            "1.1",
                            "--compression",
                            "zstd",
                            "--compression-level",
                            "22",
                            tile_input,
                            str(staged_parquet),
                        ),
                        check=True,
                    )
                    try:
                        validate_parquet_coordinates(staged_parquet, progress=print)
                    except (ValueError, OSError) as exc:
                        raise click.ClickException(f"GeoParquet coordinate validation failed: {exc}") from exc
                    staged_parquet.replace(parquet_file)

                # Analyze fields for unique ID before potential partitioning
                create_field_analysis(lname, [parquet_file])

                if parquet_file.stat().st_size > TWO_GB:
                    print("parquet file > 2GB, splitting...")
                    partition_count = calculate_partition_count(parquet_file.stat().st_size)
                    split_dir = Path("split_parquet")

                    while True:
                        split_dir.mkdir(exist_ok=True)
                        run_cmd(
                            gpio_command(
                                "partition",
                                "kdtree",
                                str(parquet_file),
                                str(split_dir),
                                "--geoparquet-version",
                                "1.1",
                                "--compression",
                                "zstd",
                                "--compression-level",
                                "22",
                                "--partitions",
                                str(partition_count),
                                "-v",
                            ),
                            check=True,
                        )
                        # Check if any split exceeds 2GB
                        split_files = sorted(split_dir.glob("*.parquet"))
                        oversized = [f for f in split_files if f.stat().st_size > TWO_GB]
                        if not oversized:
                            break
                        # Clean up and retry with more partitions
                        print(
                            f"found {len(oversized)} split(s) > 2GB, retrying with {partition_count * 2} partitions..."
                        )
                        for f in split_files:
                            f.unlink()
                        split_dir.rmdir()
                        partition_count *= 2

                    # Move split files to current directory, prefixing with original name
                    for part in split_files:
                        try:
                            validate_parquet_coordinates(part, progress=print)
                        except (ValueError, OSError) as exc:
                            raise click.ClickException(f"GeoParquet coordinate validation failed: {exc}") from exc
                    renamed_files = []
                    for f in split_files:
                        new_name = f"{lname}.{f.name}"
                        shutil.move(str(f), new_name)
                        print(f"created: {new_name}")
                        renamed_files.append(Path(new_name))
                    split_dir.rmdir()
                    parquet_file.unlink()

                    # Create meta.json for partitioned files
                    print("creating parquet meta.json")
                    create_parquet_meta(lname, renamed_files)
                    parquet_files = renamed_files
                else:
                    print(f"created: {parquet_file.name}")
                    parquet_files = [parquet_file]
            if coverage:
                from iomaps.core.coverage import CoverageError, create_coverage

                coverage_file = Path(f"{lname}.coverage.png")
                try:
                    create_coverage(
                        parquet_files, coverage_file, grid_size=coverage_grid_size, overwrite=True, progress=print
                    )
                except (CoverageError, OSError) as exc:
                    raise click.ClickException(f"Coverage creation failed: {exc}") from exc
                print(f"created: {coverage_file.name}")
    finally:
        # Clean up the temporary cleaned file
        if cleaned_file and cleaned_file.exists():
            cleaned_file.unlink()


if __name__ == "__main__":
    prepare()
