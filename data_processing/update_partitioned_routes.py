#!/usr/bin/env python3
"""
Update routes.json to add "partitioned_parquet": true for entries with partitioned parquet files.

For each route entry:
1. Parse the GitHub release URL to get repo and tag
2. Find the corresponding files.txt
3. Check if there are multiple parquet files matching the base name pattern
4. If so, add "partitioned_parquet": true to the route entry

Usage:
    python update_partitioned_routes.py [--dry-run]
"""

import argparse
import json
import re
import sys
from pathlib import Path


def parse_github_url(url: str) -> tuple[str, str, str] | None:
    """
    Parse a GitHub release URL to extract repo, release tag, and filename.
    
    URL format: https://github.com/{owner}/{repo}/releases/download/{tag}/{filename}
    Returns: (repo, tag, filename) or None if not a valid GitHub release URL
    """
    pattern = r'https://github\.com/([^/]+)/([^/]+)/releases/download/([^/]+)/(.+)'
    match = re.match(pattern, url)
    if match:
        owner, repo, tag, filename = match.groups()
        return repo, tag, filename
    return None


def get_base_name(filename: str) -> str:
    """Get the base name from a pmtiles or mosaic.json filename."""
    base = filename
    if base.endswith('.pmtiles'):
        base = base[:-8]
    elif base.endswith('.mosaic.json'):
        base = base[:-12]
    # Remove -partXXXX suffix if present
    base = re.sub(r'-part\d+$', '', base)
    return base


def has_partitioned_parquet(files_txt: Path, base_name: str) -> bool:
    """Check if there are multiple parquet files matching the base name pattern."""
    if not files_txt.exists():
        return False
    
    files = files_txt.read_text().strip().split('\n')
    
    # Match files like: base_name.NN.parquet (partitioned pattern)
    pattern = re.compile(rf'^{re.escape(base_name)}\.\d+\.parquet$')
    matching_files = [f for f in files if pattern.match(f)]
    
    return len(matching_files) > 1


def main():
    parser = argparse.ArgumentParser(
        description="Update routes.json to mark entries with partitioned parquet files"
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
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show what would be done without making changes"
    )
    args = parser.parse_args()
    
    # Load routes.json
    if not args.routes_file.exists():
        print(f"Error: Routes file not found: {args.routes_file}", file=sys.stderr)
        sys.exit(1)
    
    with open(args.routes_file) as f:
        routes = json.load(f)
    
    print(f"Loaded {len(routes)} routes from {args.routes_file}")
    
    updated_count = 0
    
    for route_key, entry in routes.items():
        url = entry.get("url", "")
        handler_type = entry.get("handlertype", "")
        entry_type = entry.get("type", "")
        
        # Skip raster types
        if entry_type == "raster":
            continue
        
        # Only process pmtiles and mosaic types
        if handler_type not in ("pmtiles", "mosaic"):
            continue
        
        # Skip if already marked
        if entry.get("partitioned_parquet"):
            continue
        
        # Parse GitHub URL
        parsed = parse_github_url(url)
        if not parsed:
            continue
        
        repo, tag, filename = parsed
        
        # Find files.txt
        files_txt = args.data_dir / repo / tag / "files.txt"
        if not files_txt.exists():
            continue
        
        # Get base name and check for partitioned parquet
        base_name = get_base_name(filename)
        
        if has_partitioned_parquet(files_txt, base_name):
            print(f"Found partitioned parquet for: {route_key}")
            print(f"  Repo: {repo}, Tag: {tag}, Base: {base_name}")
            entry["partitioned_parquet"] = True
            updated_count += 1
    
    print(f"\nFound {updated_count} routes with partitioned parquet files")
    
    if updated_count > 0:
        if args.dry_run:
            print("[DRY-RUN] Would update routes.json")
        else:
            with open(args.routes_file, "w") as f:
                json.dump(routes, f, indent=2)
            print(f"Updated {args.routes_file}")


if __name__ == "__main__":
    main()
