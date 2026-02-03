#!/usr/bin/env python3
"""
Process routes.json to create geoparquet files from geojsonl.7z files.

For each non-raster entry in routes.json:
1. Extract repo and release from pmtiles/mosaic.json URL
2. Locate corresponding geojsonl.7z files in data folder
3. Download and extract geojsonl.7z files
4. Convert to geoparquet using gpio
5. If > 2GB, partition using kdtree
6. Rename partitions and create extents.json
7. Upload to GitHub release
8. Cleanup intermediate files
9. Update files.txt

Usage:
    python create_geoparquet.py [--dry-run] [--filter PATTERN]
"""

import argparse
import json
import math
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

# Size threshold for partitioning (2GB)
SIZE_THRESHOLD_BYTES = 2 * 1024 * 1024 * 1024


def run_cmd(cmd: list[str], check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    """Run a command and optionally capture output."""
    print(f"  Running: {' '.join(cmd)}")
    return subprocess.run(cmd, check=check, capture_output=capture, text=True)


def parse_github_url(url: str) -> tuple[str, str, str] | None:
    """
    Parse a GitHub release URL to extract owner/repo, release tag, and filename.
    
    URL format: https://github.com/{owner}/{repo}/releases/download/{tag}/{filename}
    Returns: (repo_name, tag, filename) or None if not a valid GitHub release URL
    """
    pattern = r'https://github\.com/([^/]+)/([^/]+)/releases/download/([^/]+)/(.+)'
    match = re.match(pattern, url)
    if match:
        owner, repo, tag, filename = match.groups()
        return repo, tag, filename
    return None


def get_geojsonl_base_from_pmtiles(pmtiles_name: str) -> str:
    """Get the base name for geojsonl files from a pmtiles filename."""
    # Remove .pmtiles or .mosaic.json extension
    base = pmtiles_name
    if base.endswith('.pmtiles'):
        base = base[:-8]
    elif base.endswith('.mosaic.json'):
        base = base[:-12]
    # Remove -partXXXX suffix if present
    base = re.sub(r'-part\d+$', '', base)
    return base


def find_geojsonl_files(files_txt: Path, base_name: str) -> list[str]:
    """Find all geojsonl.7z files matching the base name in files.txt (case insensitive)."""
    if not files_txt.exists():
        return []
    
    files = files_txt.read_text().strip().split('\n')
    # Match files like: base_name.geojsonl.7z or base_name.geojsonl.7z.001 (case insensitive)
    pattern = re.compile(rf'^{re.escape(base_name)}\.geojsonl\.7z(\.\d+)?$', re.IGNORECASE)
    return sorted([f for f in files if pattern.match(f)])


def check_parquet_exists(files_txt: Path, base_name: str) -> bool:
    """Check if parquet file(s) already exist in the release."""
    if not files_txt.exists():
        return False
    
    files = files_txt.read_text().strip().split('\n')
    # Check for either single parquet or partitioned parquet files
    for f in files:
        if f == f"{base_name}.parquet":
            return True
        # Check for partitioned files like base_name.00000.parquet
        if re.match(rf'^{re.escape(base_name)}\.\d+\.parquet$', f):
            return True
    return False


def download_release_files(repo: str, tag: str, files: list[str], dest_dir: Path, dry_run: bool = False) -> bool:
    """Download files from a GitHub release."""
    if dry_run:
        print(f"  [DRY-RUN] Would download {len(files)} files to {dest_dir}")
        for f in files:
            print(f"    - {f}")
        return True
    
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"  Downloading {len(files)} file(s)...")
    for i, filename in enumerate(files, 1):
        dest_file = dest_dir / filename
        if dest_file.exists():
            print(f"    [{i}/{len(files)}] Already exists: {filename}")
            continue
        
        try:
            print(f"    [{i}/{len(files)}] Downloading: {filename}")
            cmd = ["gh", "release", "download", tag, 
                   "--repo", f"ramSeraph/{repo}",
                   "--pattern", filename,
                   "--dir", str(dest_dir)]
            run_cmd(cmd)
        except subprocess.CalledProcessError as e:
            print(f"  Error downloading {filename}: {e}", file=sys.stderr)
            return False
    
    return True


def extract_7z_files(archive_files: list[Path], dest_dir: Path, dry_run: bool = False) -> Path | None:
    """Extract 7z files (handles split archives)."""
    if not archive_files:
        return None
    
    if dry_run:
        print(f"  [DRY-RUN] Would extract from {len(archive_files)} archive file(s)")
        for f in archive_files:
            print(f"    - {f.name}")
        return dest_dir / "dummy.geojsonl"
    
    # For split archives (.7z.001, .7z.002, ...), use the first part
    # For single file (.7z), use it directly
    first_file = archive_files[0]
    
    if len(archive_files) > 1:
        print(f"  Extracting multipart archive ({len(archive_files)} parts):")
        for f in archive_files:
            print(f"    - {f.name}")
    else:
        print(f"  Extracting: {first_file.name}")
    
    try:
        cmd = ["7z", "x", "-y", f"-o{dest_dir}", str(first_file)]
        run_cmd(cmd)
        
        # Find the extracted geojsonl file
        geojsonl_files = list(dest_dir.glob("*.geojsonl"))
        if geojsonl_files:
            print(f"  Extracted: {geojsonl_files[0].name}")
            return geojsonl_files[0]
        
        print(f"  Error: No geojsonl file found after extraction", file=sys.stderr)
        return None
    except subprocess.CalledProcessError as e:
        print(f"  Error extracting 7z: {e}", file=sys.stderr)
        return None


def filter_empty_geometries(geojsonl_file: Path, dry_run: bool = False) -> tuple[Path, int, int]:
    """
    Filter out features with empty or null geometries from a geojsonl file.
    Returns: (filtered_file_path, original_count, filtered_count)
    """
    if dry_run:
        print(f"  [DRY-RUN] Would filter empty geometries from {geojsonl_file.name}")
        return geojsonl_file, 0, 0
    
    filtered_file = geojsonl_file.parent / f"{geojsonl_file.stem}.filtered.geojsonl"
    original_count = 0
    filtered_count = 0
    
    print(f"  Filtering empty geometries from {geojsonl_file.name}...")
    
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
                
                # Check for empty coordinates
                geom_type = geometry.get("type", "")
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
                
                # Valid geometry, write it
                outfile.write(line + '\n')
                filtered_count += 1
                
            except json.JSONDecodeError:
                # Skip malformed lines
                continue
    
    removed_count = original_count - filtered_count
    print(f"    Original features: {original_count}")
    print(f"    After filtering: {filtered_count}")
    print(f"    Removed (empty/null geometry): {removed_count}")
    
    # Replace original with filtered file
    geojsonl_file.unlink()
    filtered_file.rename(geojsonl_file)
    
    return geojsonl_file, original_count, filtered_count


def convert_to_geoparquet(geojsonl_file: Path, output_file: Path, dry_run: bool = False) -> bool:
    """Convert geojsonl to geoparquet using gpio."""
    if dry_run:
        print(f"  [DRY-RUN] Would convert {geojsonl_file.name} to {output_file.name}")
        return True
    
    try:
        cmd = ["uvx", "--from", "git+https://github.com/geoparquet/geoparquet-io.git", "gpio", "convert",
               "--geoparquet-version", "2.0",
               "--compression", "zstd",
               "--compression-level", "15",
               str(geojsonl_file), str(output_file)]
        run_cmd(cmd)
        return output_file.exists()
    except subprocess.CalledProcessError as e:
        print(f"  Error converting to geoparquet: {e}", file=sys.stderr)
        return False


def calculate_partition_count(file_size_bytes: int, max_partition_size: int = SIZE_THRESHOLD_BYTES) -> int:
    """Calculate the number of partitions needed (must be power of 2)."""
    if file_size_bytes <= max_partition_size:
        return 1
    
    # Calculate minimum partitions needed
    min_partitions = math.ceil(file_size_bytes / max_partition_size)
    
    # Round up to next power of 2
    power = math.ceil(math.log2(min_partitions))
    return 2 ** power


def partition_geoparquet(parquet_file: Path, output_dir: Path, partitions: int, dry_run: bool = False) -> bool:
    """Partition a geoparquet file using kdtree."""
    if dry_run:
        print(f"  [DRY-RUN] Would partition {parquet_file.name} into {partitions} parts")
        return True
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        cmd = ["uvx", "--from", "git+https://github.com/geoparquet/geoparquet-io.git", "gpio", "partition", "kdtree",
               str(parquet_file), str(output_dir),
               "--partitions", str(partitions), "-v"]
        run_cmd(cmd)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error partitioning: {e}", file=sys.stderr)
        return False


def get_parquet_metadata(parquet_path: Path) -> tuple[dict | None, dict | None]:
    """Get parquet metadata from a file using gpio inspect meta.
    
    Returns: (geo_metadata, parquet_metadata)
    """
    geo_meta = None
    parquet_meta = None
    
    # Get geo metadata (for bbox)
    try:
        result = subprocess.run(
            ["uvx", "--from", "geoparquet-io", "gpio", "inspect", "meta", "--json", str(parquet_path)],
            capture_output=True,
            text=True,
            check=True
        )
        geo_meta = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"  Warning: Failed to get geo metadata for {parquet_path}: {e}", file=sys.stderr)
    
    # Get parquet metadata (for schema)
    try:
        result = subprocess.run(
            ["uvx", "--from", "geoparquet-io", "gpio", "inspect", "meta", "--parquet", "--json", str(parquet_path)],
            capture_output=True,
            text=True,
            check=True
        )
        parquet_meta = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"  Warning: Failed to get parquet metadata for {parquet_path}: {e}", file=sys.stderr)
    
    return geo_meta, parquet_meta


def extract_schema(parquet_meta: dict) -> dict:
    """Extract schema (column descriptions) from parquet metadata."""
    schema = {}
    
    # Schema is in format: "schema: None, col1: TYPE1, col2: TYPE2, ..."
    schema_str = parquet_meta.get("schema", "")
    if not schema_str:
        return schema
    
    # Parse the schema string
    parts = schema_str.split(", ")
    for part in parts:
        if ": " not in part:
            continue
        name, type_str = part.split(": ", 1)
        # Skip the "schema" entry itself
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


def rename_partitions_and_create_meta(parquet_file: Path, partitioned_dir: Path, dry_run: bool = False) -> tuple[list[Path], Path | None]:
    """Rename partitioned files and create meta.json with schema and extents."""
    prefix = parquet_file.stem
    partition_files = sorted(partitioned_dir.glob("*.parquet"))
    
    if not partition_files:
        print(f"  Error: No parquet files found in {partitioned_dir}", file=sys.stderr)
        return [], None
    
    print(f"  Found {len(partition_files)} partition files")
    
    meta = {
        "schema": {},
        "extents": {}
    }
    renamed_files = []
    
    for partition_file in partition_files:
        old_name = partition_file.name
        
        if old_name.startswith(prefix):
            new_name = old_name
            new_path = partition_file
        else:
            new_name = f"{prefix}.{old_name}"
            new_path = partition_file.parent / new_name
            
            if not dry_run:
                partition_file.rename(new_path)
                print(f"    Renamed: {old_name} -> {new_name}")
            else:
                print(f"    [DRY-RUN] Would rename: {old_name} -> {new_name}")
        
        renamed_files.append(new_path)
        
        # Get metadata
        file_to_inspect = partition_file if dry_run else new_path
        geo_meta, parquet_meta = get_parquet_metadata(file_to_inspect)
        
        # Merge schema from all partitions
        if parquet_meta:
            partition_schema = extract_schema(parquet_meta)
            for col_name, col_info in partition_schema.items():
                if col_name not in meta["schema"]:
                    meta["schema"][col_name] = col_info
        
        # Get bbox for this partition
        if geo_meta:
            bbox = extract_bbox(geo_meta)
            if bbox:
                meta["extents"][new_name] = {
                    "minx": bbox[0],
                    "miny": bbox[1],
                    "maxx": bbox[2],
                    "maxy": bbox[3]
                }
    
    # Write meta.json
    meta_file = partitioned_dir / f"{prefix}.parquet.meta.json"
    if not dry_run:
        with open(meta_file, "w") as f:
            json.dump(meta, f, indent=2)
        print(f"  Created meta file: {meta_file.name}")
        print(f"    Schema columns: {len(meta['schema'])}")
        print(f"    Extents entries: {len(meta['extents'])}")
    else:
        print(f"  [DRY-RUN] Would create meta file: {meta_file.name}")
    
    return renamed_files, meta_file


def upload_to_release(repo: str, tag: str, files: list[Path], dry_run: bool = False) -> bool:
    """Upload files to a GitHub release."""
    if dry_run:
        print(f"  [DRY-RUN] Would upload {len(files)} files to {repo}/{tag}")
        for f in files:
            print(f"    - {f.name}")
        return True
    
    try:
        file_args = [str(f) for f in files]
        cmd = ["gh", "release", "upload", tag,
               "--repo", f"ramSeraph/{repo}",
               "--clobber"] + file_args
        run_cmd(cmd)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error uploading to release: {e}", file=sys.stderr)
        return False


def update_files_txt(files_txt: Path, new_files: list[str], dry_run: bool = False) -> bool:
    """Update files.txt with new files."""
    if dry_run:
        print(f"  [DRY-RUN] Would update {files_txt} with {len(new_files)} new files")
        return True
    
    existing_files = set()
    if files_txt.exists():
        existing_files = set(files_txt.read_text().strip().split('\n'))
    
    all_files = sorted(existing_files | set(new_files))
    files_txt.write_text('\n'.join(all_files) + '\n')
    print(f"  Updated {files_txt}")
    return True


def cleanup_staging(staging_dir: Path, dry_run: bool = False) -> None:
    """Clean up staging directory."""
    if not staging_dir.exists():
        return
    
    if dry_run:
        print(f"  [DRY-RUN] Would clean up {staging_dir}")
        return
    
    shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    print(f"  Cleaned up {staging_dir}")


def process_entry(route_key: str, entry: dict, data_dir: Path, dry_run: bool = False) -> bool:
    """Process a single route entry."""
    url = entry.get("url", "")
    handler_type = entry.get("handlertype", "")
    entry_type = entry.get("type", "")
    
    # Skip raster types
    if entry_type == "raster":
        return True
    
    # Only process pmtiles and mosaic types
    if handler_type not in ("pmtiles", "mosaic"):
        return True
    
    print(f"\nProcessing: {route_key}")
    print(f"  URL: {url}")
    
    # Parse GitHub URL
    parsed = parse_github_url(url)
    if not parsed:
        print(f"  Skipping: Not a valid GitHub release URL")
        return True
    
    repo, tag, filename = parsed
    print(f"  Repo: {repo}, Tag: {tag}, File: {filename}")
    
    # Locate the release directory
    release_dir = data_dir / repo / tag
    if not release_dir.exists():
        print(f"  Skipping: Release directory not found: {release_dir}")
        return True
    
    files_txt = release_dir / "files.txt"
    
    # Get base name for geojsonl files
    base_name = get_geojsonl_base_from_pmtiles(filename)
    print(f"  Base name: {base_name}")
    
    # Check if parquet already exists
    if check_parquet_exists(files_txt, base_name):
        print(f"  Skipping: Parquet files already exist for {base_name}")
        return True
    
    # Find geojsonl.7z files
    geojsonl_files = find_geojsonl_files(files_txt, base_name)
    if not geojsonl_files:
        print(f"  Skipping: No geojsonl.7z files found for {base_name}")
        return True
    
    print(f"  Found {len(geojsonl_files)} geojsonl.7z file(s)")
    
    # Set up staging directory (clear any existing files first)
    staging_dir = release_dir / "data" / "staging"
    if staging_dir.exists():
        if dry_run:
            print(f"  [DRY-RUN] Would clear staging directory: {staging_dir}")
        else:
            print(f"  Clearing staging directory: {staging_dir}")
            shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    # Download geojsonl.7z files
    print(f"  Downloading geojsonl.7z files...")
    if not download_release_files(repo, tag, geojsonl_files, staging_dir, dry_run):
        return False
    
    # Extract 7z files
    print(f"  Extracting 7z files...")
    if dry_run:
        geojsonl_file = staging_dir / f"{base_name}.geojsonl"
    else:
        # Find archive files case-insensitively
        all_files = list(staging_dir.iterdir())
        pattern = re.compile(rf'^{re.escape(base_name)}\.geojsonl\.7z(\.\d+)?$', re.IGNORECASE)
        archive_files = sorted([f for f in all_files if pattern.match(f.name)])
        geojsonl_file = extract_7z_files(archive_files, staging_dir, dry_run)
        if not geojsonl_file:
            return False
    
    # Filter out features with empty geometries
    geojsonl_file, orig_count, filt_count = filter_empty_geometries(geojsonl_file, dry_run)
    
    # Convert to geoparquet
    parquet_file = staging_dir / f"{base_name}.parquet"
    print(f"  Converting to geoparquet...")
    if not convert_to_geoparquet(geojsonl_file, parquet_file, dry_run):
        return False
    
    files_to_upload = []
    new_file_names = []
    
    # Check file size and partition if needed
    if not dry_run and parquet_file.exists():
        file_size = parquet_file.stat().st_size
        print(f"  Parquet size: {file_size / (1024**3):.2f} GB")
        
        if file_size > SIZE_THRESHOLD_BYTES:
            # Calculate partition count (power of 2, so each partition < 2GB)
            partition_count = calculate_partition_count(file_size)
            print(f"  File exceeds 2GB, partitioning into {partition_count} parts...")
            partitioned_dir = staging_dir / "partitioned"
            if not partition_geoparquet(parquet_file, partitioned_dir, partition_count, dry_run):
                return False
            
            # Rename partitions and create meta.json
            renamed_files, meta_file = rename_partitions_and_create_meta(
                parquet_file, partitioned_dir, dry_run
            )
            
            files_to_upload = renamed_files
            if meta_file:
                files_to_upload.append(meta_file)
            new_file_names = [f.name for f in files_to_upload]
        else:
            # Upload single file
            files_to_upload = [parquet_file]
            new_file_names = [parquet_file.name]
    elif dry_run:
        # Simulate for dry run
        files_to_upload = [parquet_file]
        new_file_names = [f"{base_name}.parquet"]
    
    # Upload to release
    if files_to_upload:
        print(f"  Uploading {len(files_to_upload)} file(s) to release...")
        if not upload_to_release(repo, tag, files_to_upload, dry_run):
            return False
        
        # Update files.txt
        update_files_txt(files_txt, new_file_names, dry_run)
    
    # Cleanup staging
    print(f"  Cleaning up...")
    cleanup_staging(staging_dir, dry_run)
    
    print(f"  Done processing {base_name}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create geoparquet files from geojsonl.7z for all non-raster routes"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show what would be done without making changes"
    )
    parser.add_argument(
        "--routes-file",
        type=Path,
        default=Path("routes.json"),
        help="Path to routes.json file"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Path to data directory"
    )
    args = parser.parse_args()
    
    # Load routes.json
    if not args.routes_file.exists():
        print(f"Error: Routes file not found: {args.routes_file}", file=sys.stderr)
        sys.exit(1)
    
    with open(args.routes_file) as f:
        routes = json.load(f)
    
    print(f"Loaded {len(routes)} routes from {args.routes_file}")
    
    # Process each route
    success_count = 0
    error_count = 0
    skip_count = 0
    
    for route_key, entry in routes.items():
        entry_type = entry.get("type", "")
        handler_type = entry.get("handlertype", "")
        
        # Skip raster types
        if entry_type == "raster":
            skip_count += 1
            continue
        
        # Only process pmtiles and mosaic types
        if handler_type not in ("pmtiles", "mosaic"):
            skip_count += 1
            continue
        
        try:
            if process_entry(route_key, entry, args.data_dir, args.dry_run):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"  Error processing {route_key}: {e}", file=sys.stderr)
            error_count += 1
    
    print(f"\n{'='*60}")
    print(f"Summary:")
    print(f"  Processed: {success_count}")
    print(f"  Errors: {error_count}")
    print(f"  Skipped: {skip_count}")


if __name__ == "__main__":
    main()
