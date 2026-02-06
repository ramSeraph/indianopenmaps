#!/usr/bin/env python3
"""
Create metadata JSON for partitioned parquet files.

For a given base name and list of parquet partition files, this script:
1. Fetches parquet metadata remotely for each partition
2. Creates a <base_name>.parquet.meta.json with:
   - schema: column descriptions
   - extents: bbox for each partition file

Usage:
    python create_parquet_meta.py <repo> <tag> <base_name> <parquet_file1> [parquet_file2 ...]
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


def get_remote_parquet_url(repo: str, tag: str, filename: str) -> str:
    """Construct the GitHub release download URL for a parquet file."""
    return f"https://github.com/ramSeraph/{repo}/releases/download/{tag}/{filename}"


def get_parquet_metadata(url: str) -> tuple[dict | None, dict | None]:
    """Get parquet metadata from a remote file using gpio inspect meta.
    
    Returns: (geo_metadata, parquet_metadata)
    """
    geo_meta = None
    parquet_meta = None
    
    # Get geo metadata (for bbox)
    try:
        result = subprocess.run(
            ["uvx", "--from", "geoparquet-io", "gpio", "inspect", "meta", "--json", url],
            capture_output=True,
            text=True,
            check=True
        )
        geo_meta = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"  Warning: Failed to get geo metadata for {url}: {e}", file=sys.stderr)
    
    # Get parquet metadata (for schema)
    try:
        result = subprocess.run(
            ["uvx", "--from", "geoparquet-io", "gpio", "inspect", "meta", "--parquet", "--json", url],
            capture_output=True,
            text=True,
            check=True
        )
        parquet_meta = json.loads(result.stdout)
    except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
        print(f"  Warning: Failed to get parquet metadata for {url}: {e}", file=sys.stderr)
    
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


def create_meta_json(repo: str, tag: str, base_name: str, parquet_files: list[str]) -> dict:
    """Create the metadata JSON structure."""
    meta = {
        "schema": {},
        "geometry_types": set(),
        "extents": {}
    }
    
    for filename in parquet_files:
        url = get_remote_parquet_url(repo, tag, filename)
        print(f"  Fetching metadata for {filename}...")
        
        geo_meta, parquet_meta = get_parquet_metadata(url)
        
        # Merge schema from all partitions
        if parquet_meta:
            partition_schema = extract_schema(parquet_meta)
            for col_name, col_info in partition_schema.items():
                if col_name not in meta["schema"]:
                    meta["schema"][col_name] = col_info
        
        # Extract bbox and geometry_types for this partition
        if geo_meta:
            bbox = extract_bbox(geo_meta)
            if bbox:
                meta["extents"][filename] = {
                    "minx": bbox[0],
                    "miny": bbox[1],
                    "maxx": bbox[2],
                    "maxy": bbox[3]
                }
            geometry_types = extract_geometry_types(geo_meta)
            if geometry_types:
                meta["geometry_types"].update(geometry_types)
    
    # Convert set to sorted list for JSON serialization
    meta["geometry_types"] = sorted(meta["geometry_types"])
    
    return meta


def upload_to_release(repo: str, tag: str, file_path: Path) -> bool:
    """Upload file to a GitHub release."""
    try:
        cmd = ["gh", "release", "upload", tag,
               "--repo", f"ramSeraph/{repo}",
               "--clobber", str(file_path)]
        print(f"  Uploading {file_path.name} to {repo}/{tag}...")
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error uploading to release: {e}", file=sys.stderr)
        return False


def update_files_txt(release_dir: Path, add_files: list[str], remove_files: list[str]) -> bool:
    """Update files.txt by adding and removing files."""
    files_txt = release_dir / "files.txt"
    
    existing_files = set()
    if files_txt.exists():
        existing_files = set(files_txt.read_text().strip().split('\n'))
    
    # Remove files
    for f in remove_files:
        existing_files.discard(f)
    
    # Add files
    existing_files.update(add_files)
    
    # Write back
    all_files = sorted(f for f in existing_files if f)
    files_txt.write_text('\n'.join(all_files) + '\n')
    print(f"  Updated {files_txt}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Create metadata JSON for partitioned parquet files"
    )
    parser.add_argument("repo", help="GitHub repository name")
    parser.add_argument("tag", help="Release tag")
    parser.add_argument("base_name", help="Base name for the parquet files")
    parser.add_argument("parquet_files", nargs="+", help="List of parquet partition files")
    parser.add_argument(
        "--release-dir",
        type=Path,
        help="Path to release directory (for updating files.txt)"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show what would be done without uploading"
    )
    parser.add_argument(
        "--force", "-f",
        action="store_true",
        help="Force writing meta.json even if it already exists"
    )
    args = parser.parse_args()
    
    meta_filename = f"{args.base_name}.parquet.meta.json"
    
    # Check if meta.json already exists in files.txt (skip unless force)
    if args.release_dir and not args.force:
        files_txt = args.release_dir / "files.txt"
        if files_txt.exists():
            existing_files = files_txt.read_text().strip().split('\n')
            if meta_filename in existing_files:
                print(f"Skipping {args.base_name}: {meta_filename} already exists (use --force to overwrite)")
                return
    
    print(f"Creating metadata for {args.base_name} ({len(args.parquet_files)} partitions)")
    
    # Create metadata JSON
    meta = create_meta_json(args.repo, args.tag, args.base_name, args.parquet_files)
    
    if not meta["schema"] and not meta["extents"] and not meta["geometry_types"]:
        print("Error: Could not extract any metadata", file=sys.stderr)
        sys.exit(1)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        meta_path = Path(tmpdir) / meta_filename
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)
        
        print(f"  Created {meta_filename}")
        print(f"    Schema columns: {len(meta['schema'])}")
        print(f"    Geometry types: {meta['geometry_types']}")
        print(f"    Extents entries: {len(meta['extents'])}")
        
        if args.dry_run:
            print(f"  [DRY-RUN] Would upload {meta_filename}")
            print(f"  [DRY-RUN] Would update files.txt")
            print(json.dumps(meta, indent=2))
        else:
            # Upload new meta file to release
            if not upload_to_release(args.repo, args.tag, meta_path):
                sys.exit(1)
            
            # Update files.txt if release_dir provided
            if args.release_dir:
                update_files_txt(
                    args.release_dir,
                    add_files=[meta_filename],
                    remove_files=[]
                )
    
    print("Done!")


if __name__ == "__main__":
    main()
