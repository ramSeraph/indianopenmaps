#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
"""
Prepare a geojsonl file for distribution: compress and convert to mbtiles/pmtiles.
"""

import argparse
import json
import math
import os
import shutil
import subprocess
from pathlib import Path

TWO_GB = 2 * 1024 * 1024 * 1024


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
    return 2 ** power


def filter_empty_geometries(geojsonl_file: Path) -> tuple[int, int]:
    """Filter out features with empty or null geometries from a geojsonl file.
    Also renames properties containing 'geom' to avoid DuckDB geometry detection issues.
    
    Returns: (original_count, filtered_count)
    """
    filtered_file = geojsonl_file.parent / f"{geojsonl_file.stem}.filtered.geojsonl"
    original_count = 0
    filtered_count = 0
    renamed_props: dict[str, str] = {}
    rename_counter = 0

    print("filtering empty geometries...")

    with open(geojsonl_file, 'r') as infile, open(filtered_file, 'w') as outfile:
        for line in infile:
            original_count += 1
            line = line.strip()
            if not line:
                continue

            try:
                feature = json.loads(line)
                geometry = feature.get("geometry")

                # Skip if geometry is null or empty
                if geometry is None:
                    continue

                coords = geometry.get("coordinates")
                if coords is None:
                    continue

                # Check for empty coordinate arrays
                if isinstance(coords, list) and len(coords) == 0:
                    continue

                # For nested coordinate arrays (Polygon, MultiLineString, etc.)
                if isinstance(coords, list) and len(coords) > 0:
                    if isinstance(coords[0], list) and len(coords[0]) == 0:
                        continue

                # Rename properties containing 'geom' to avoid DuckDB issues
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
                outfile.write(json.dumps(feature) + '\n')
                filtered_count += 1

            except json.JSONDecodeError:
                # Skip malformed lines
                continue

    removed_count = original_count - filtered_count
    print(f"  original features: {original_count}")
    print(f"  after filtering: {filtered_count}")
    print(f"  removed (empty/null geometry): {removed_count}")
    if renamed_props:
        print(f"  renamed properties: {renamed_props}")

    # Replace original with filtered file
    geojsonl_file.unlink()
    filtered_file.rename(geojsonl_file)

    return original_count, filtered_count


def get_parquet_metadata(parquet_path: Path) -> tuple[dict | None, dict | None]:
    """Get parquet metadata from a file using gpio inspect meta."""
    geo_meta = None
    parquet_meta = None

    try:
        result = run_cmd(
            ["uvx", "--from", "geoparquet-io", "gpio", "inspect", "meta", "--json", str(parquet_path)],
            capture_output=True, text=True, check=True,
        )
        geo_meta = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError):
        pass

    try:
        result = run_cmd(
            ["uvx", "--from", "geoparquet-io", "gpio", "inspect", "meta", "--parquet", "--json", str(parquet_path)],
            capture_output=True, text=True, check=True,
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


def create_parquet_meta(lname: str, parquet_files: list[Path]) -> None:
    """Create meta.json for partitioned parquet files."""
    meta = {"schema": {}, "extents": {}}

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
                    "minx": bbox[0], "miny": bbox[1],
                    "maxx": bbox[2], "maxy": bbox[3],
                }

    meta_file = Path(f"{lname}.parquet.meta.json")
    with open(meta_file, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"created: {meta_file.name}")


def main():
    parser = argparse.ArgumentParser(
        description="Prepare a geojsonl file for distribution: compress and convert to mbtiles/pmtiles."
    )
    parser.add_argument("file", type=Path, help="Input geojsonl file")
    parser.add_argument("src_url", help="Source URL for attribution")
    parser.add_argument("src_name", help="Source name for attribution")
    parser.add_argument("--points", action="store_true", help="Use drop-densest-as-needed for point data")
    parser.add_argument("--no-zip", action="store_true", help="Skip creating the 7z archive")
    parser.add_argument("--no-pmtiles", action="store_true", help="Skip converting to pmtiles")
    parser.add_argument("--no-parquet", action="store_true", help="Skip creating geoparquet file")

    args = parser.parse_args()

    if args.file.suffix != ".geojsonl":
        parser.error(f"{args.file} is not a geojsonl file")

    fbase = args.file.name
    lname = args.file.stem
    work_dir = args.file.parent

    os.chdir(work_dir)

    # Filter out empty geometries first
    filter_empty_geometries(Path(fbase))

    # Create archive unless --no-zip is set
    if not args.no_zip:
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

    # Choose tippecanoe command based on points vs polygons
    cmd = "drop-densest-as-needed" if args.points else "coalesce-smallest-as-needed"

    attribution = f'Source: <a href="{args.src_url}" target="_blank" rel="noopener noreferrer">{args.src_name}</a>'

    print("creating mbtiles file")
    run_cmd(
        [
            "tippecanoe",
            "-P",
            "-S", "10",
            "--increase-gamma-as-needed",
            "-zg",
            "-o", f"{lname}.mbtiles",
            "--simplify-only-low-zooms",
            f"--{cmd}",
            "--extend-zooms-if-still-dropping",
            "-n", lname,
            "-l", lname,
            "-A", attribution,
            fbase,
        ],
        check=True,
    )

    # Convert to pmtiles unless --no-pmtiles is set
    if not args.no_pmtiles:
        print("converting to pmtiles")
        run_cmd(["pmtiles", "convert", f"{lname}.mbtiles", f"{lname}.pmtiles"], check=True)

        pmtiles_file = Path(f"{lname}.pmtiles")
        if pmtiles_file.stat().st_size > TWO_GB:
            print("pmtiles file > 2GB, splitting...")
            split_dir = Path("split")
            split_dir.mkdir(exist_ok=True)
            run_cmd(
                [
                    "uvx", "--from", "pmtiles-mosaic", "partition",
                    "--from-source", str(pmtiles_file),
                    "--to-pmtiles", str(split_dir / pmtiles_file.name),
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
    if not args.no_parquet:
        print("creating geoparquet file")
        parquet_file = Path(f"{lname}.parquet")
        run_cmd(
            [
                "uvx", "--from", "geoparquet-io", "gpio", "convert",
                "--geoparquet-version", "2.0",
                "--compression", "zstd",
                "--compression-level", "15",
                fbase,
                str(parquet_file),
            ],
            check=True,
        )

        if parquet_file.stat().st_size > TWO_GB:
            print("parquet file > 2GB, splitting...")
            partition_count = calculate_partition_count(parquet_file.stat().st_size)
            split_dir = Path("split_parquet")

            while True:
                split_dir.mkdir(exist_ok=True)
                run_cmd(
                    [
                        "uvx", "--from", "geoparquet-io", "gpio", "partition", "kdtree",
                        str(parquet_file), str(split_dir),
                        "--partitions", str(partition_count), "-v",
                    ],
                    check=True,
                )
                # Check if any split exceeds 2GB
                split_files = sorted(split_dir.glob("*.parquet"))
                oversized = [f for f in split_files if f.stat().st_size > TWO_GB]
                if not oversized:
                    break
                # Clean up and retry with more partitions
                print(f"found {len(oversized)} split(s) > 2GB, retrying with {partition_count * 2} partitions...")
                for f in split_files:
                    f.unlink()
                split_dir.rmdir()
                partition_count *= 2

            # Move split files to current directory, prefixing with original name
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
        else:
            print(f"created: {parquet_file.name}")


if __name__ == "__main__":
    main()
