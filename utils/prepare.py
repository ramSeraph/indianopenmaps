#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "duckdb",
# ]
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

import duckdb

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

                # Rename properties containing 'geom' to avoid GDAL confusion.
                # GDAL assigns 'geom' as the default geometry field name, so a property named 'geom'
                # clashes with it and would require special handling elsewhere during conversion.
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


def analyze_unique_fields(parquet_files: list[Path]) -> dict:
    """
    Analyze parquet files to find which fields can uniquely identify features.
    
    Returns dict with field analysis results.
    """
    if not parquet_files:
        return {"error": "No parquet files provided"}
    
    conn = duckdb.connect()
    file_list = [str(f) for f in parquet_files]
    
    try:
        if len(file_list) == 1:
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{file_list[0]}')")
        else:
            file_str = ", ".join([f"'{f}'" for f in file_list])
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet([{file_str}])")
        
        total_rows = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        columns_result = conn.execute("DESCRIBE data").fetchall()
        property_columns = [col[0] for col in columns_result if col[0] != 'geometry']
        
        results = {
            "total_features": total_rows,
            "fields": {}
        }
        
        for col in property_columns:
            try:
                distinct_count = conn.execute(f'SELECT COUNT(DISTINCT "{col}") FROM data').fetchone()[0]
                null_count = conn.execute(f'SELECT COUNT(*) FROM data WHERE "{col}" IS NULL').fetchone()[0]
                
                results["fields"][col] = {
                    "distinct_values": distinct_count,
                    "null_count": null_count,
                    "is_unique": distinct_count == total_rows and null_count == 0
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
                "name_score": name_score
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
            "reason": f"unique field with name_score={best['name_score']}"
        }
    
    if candidate_fields:
        candidate_fields.sort(key=lambda x: (-x["ratio"], -x["name_score"]))
        best = candidate_fields[0]
        return {
            "field": best["name"],
            "is_unique": False,
            "uniqueness_ratio": best["ratio"],
            "reason": f"best candidate above threshold ({threshold})"
        }
    
    if all_fields:
        all_fields.sort(key=lambda x: (-x["ratio"], x["null_count"], -x["name_score"]))
        best = all_fields[0]
        return {
            "field": None, 
            "reason": f"no unique or high-cardinality fields above {threshold}",
            "best_below_threshold": {
                "field": best["name"],
                "ratio": best["ratio"],
                "null_count": best["null_count"]
            }
        }
    
    return {"field": None, "reason": f"no unique or high-cardinality fields above {threshold}"}


def create_field_analysis(lname: str, parquet_files: list[Path]) -> None:
    """Analyze parquet files and create field analysis JSON with picked unique ID."""
    print("analyzing fields for unique ID...")
    analysis = analyze_unique_fields(parquet_files)
    
    if "error" in analysis:
        print(f"  error: {analysis['error']}")
        return
    
    picked = pick_best_id(analysis)
    
    result = {
        "base_name": lname,
        "analysis": analysis,
        "picked_id": picked
    }
    
    analysis_file = Path(f"{lname}.analysis.json")
    with open(analysis_file, "w") as f:
        json.dump(result, f, indent=2)
    print(f"created: {analysis_file.name}")
    
    if picked.get("field"):
        unique_str = "unique" if picked.get("is_unique") else f"{picked.get('uniqueness_ratio', 0):.2%}"
        print(f"  picked ID: {picked['field']} ({unique_str})")
    else:
        print(f"  no suitable ID field found: {picked.get('reason', 'unknown')}")


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

        # Analyze fields for unique ID before potential partitioning
        create_field_analysis(lname, [parquet_file])

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
