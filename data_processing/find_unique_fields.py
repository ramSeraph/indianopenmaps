#!/usr/bin/env python3
"""
Find unique identifier fields for each dataset in routes.json.

For each non-raster dataset:
1. Parse the URL to get repo/release/filename info
2. Locate corresponding parquet file(s) from files.txt
3. Download parquet files if needed
4. Analyze which property fields can uniquely identify features

Usage:
    uv run --with duckdb python find_unique_fields.py [--output results.json]
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


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


def get_parquet_base_from_pmtiles(pmtiles_name: str) -> str:
    """Get the base name for parquet files from a pmtiles filename."""
    base = pmtiles_name
    if base.endswith('.pmtiles'):
        base = base[:-8]
    elif base.endswith('.mosaic.json'):
        base = base[:-12]
    # Remove -partXXXX suffix if present
    base = re.sub(r'-part\d+$', '', base)
    return base


def find_parquet_files(files_txt: Path, base_name: str) -> list[str]:
    """Find all parquet files matching the base name in files.txt."""
    if not files_txt.exists():
        return []
    
    files = files_txt.read_text().strip().split('\n')
    parquet_files = []
    for f in files:
        # Match single parquet file or partitioned files
        if f == f"{base_name}.parquet":
            parquet_files.append(f)
        elif re.match(rf'^{re.escape(base_name)}\.\d+\.parquet$', f):
            parquet_files.append(f)
    return sorted(parquet_files)


def download_parquet_file(repo: str, tag: str, filename: str, dest_dir: Path) -> Path | None:
    """Download a parquet file from GitHub release."""
    dest_file = dest_dir / filename
    if dest_file.exists():
        return dest_file
    
    try:
        cmd = ["gh", "release", "download", tag,
               "--repo", f"ramSeraph/{repo}",
               "--pattern", filename,
               "--dir", str(dest_dir)]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        return dest_file
    except subprocess.CalledProcessError as e:
        print(f"  Error downloading {filename}: {e.stderr}", file=sys.stderr)
        return None


def analyze_unique_fields(parquet_files: list[Path]) -> dict:
    """
    Analyze parquet files to find which fields can uniquely identify features.
    
    Returns dict with field analysis results.
    """
    import duckdb
    
    if not parquet_files:
        return {"error": "No parquet files provided"}
    
    # Create a DuckDB connection
    conn = duckdb.connect()
    
    # Build file list for reading
    file_list = [str(f) for f in parquet_files]
    
    try:
        # Read parquet file(s)
        if len(file_list) == 1:
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('{file_list[0]}')")
        else:
            file_str = ", ".join([f"'{f}'" for f in file_list])
            conn.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet([{file_str}])")
        
        # Get total row count
        total_rows = conn.execute("SELECT COUNT(*) FROM data").fetchone()[0]
        
        # Get column names (exclude geometry)
        columns_result = conn.execute("DESCRIBE data").fetchall()
        property_columns = [col[0] for col in columns_result if col[0] != 'geometry']
        
        results = {
            "total_features": total_rows,
            "fields": {}
        }
        
        # Check each field for uniqueness stats
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
                # Skip columns that can't be analyzed (e.g., complex types)
                results["fields"][col] = {"error": str(e)}
        
        conn.close()
        return results
        
    except Exception as e:
        conn.close()
        return {"error": str(e)}


def process_dataset(route_key: str, entry: dict, data_dir: Path, temp_dir: Path, output_dir: Path | None = None) -> dict:
    """Process a single dataset and return analysis results."""
    url = entry.get("url", "")
    handler_type = entry.get("handlertype", "")
    entry_type = entry.get("type", "")
    
    result = {
        "route": route_key,
        "name": entry.get("name", ""),
        "url": url
    }
    
    # Skip raster types
    if entry_type == "raster":
        result["skipped"] = "raster type"
        return result
    
    # Only process pmtiles and mosaic types
    if handler_type not in ("pmtiles", "mosaic"):
        result["skipped"] = f"unsupported handler type: {handler_type}"
        return result
    
    # Parse GitHub URL
    parsed = parse_github_url(url)
    if not parsed:
        result["error"] = "Not a valid GitHub release URL"
        return result
    
    repo, tag, filename = parsed
    result["repo"] = repo
    result["release"] = tag
    
    # Locate the release directory
    release_dir = data_dir / repo / tag
    if not release_dir.exists():
        result["error"] = f"Release directory not found: {release_dir}"
        return result
    
    files_txt = release_dir / "files.txt"
    
    # Get base name for parquet files
    base_name = get_parquet_base_from_pmtiles(filename)
    result["base_name"] = base_name
    
    # Check if analysis already exists (skip if successful, retry if error)
    if output_dir:
        output_file = output_dir / repo / tag / f"{base_name}.json"
        if output_file.exists():
            try:
                with open(output_file) as f:
                    existing = json.load(f)
                # Re-run if previous attempt had an error
                if "error" not in existing and "error" not in existing.get("analysis", {}):
                    print(f"  Skipping (already analyzed): {base_name}")
                    result["skipped"] = "already analyzed"
                    return result
                else:
                    print(f"  Re-running (previous attempt had error): {base_name}")
            except:
                pass  # If can't read file, re-run
    
    # Find parquet files
    parquet_files = find_parquet_files(files_txt, base_name)
    if not parquet_files:
        result["error"] = f"No parquet files found for {base_name}"
        return result
    
    result["parquet_files"] = parquet_files
    
    # Download parquet files to temp directory
    downloaded_files = []
    for pf in parquet_files:  # Download all partitions
        downloaded = download_parquet_file(repo, tag, pf, temp_dir)
        if downloaded:
            downloaded_files.append(downloaded)
    
    if not downloaded_files:
        result["error"] = "Failed to download parquet files"
        return result
    
    # Analyze unique fields
    print(f"  Analyzing {base_name}...")
    analysis = analyze_unique_fields(downloaded_files)
    result["analysis"] = analysis
    
    # Save individual analysis file
    if output_dir:
        output_subdir = output_dir / repo / tag
        output_subdir.mkdir(parents=True, exist_ok=True)
        output_file = output_subdir / f"{base_name}.json"
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"  Saved: {output_file}")
    
    # Cleanup downloaded files
    for f in downloaded_files:
        try:
            f.unlink()
        except:
            pass
    
    return result


def main():
    parser = argparse.ArgumentParser(
        description="Find unique identifier fields for each dataset in routes.json"
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
        "--output-dir", "-o",
        type=Path,
        default=Path("field_analysis"),
        help="Output directory for per-dataset JSON files"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of datasets to process (for testing)"
    )
    args = parser.parse_args()
    
    # Load routes.json
    if not args.routes_file.exists():
        print(f"Error: Routes file not found: {args.routes_file}", file=sys.stderr)
        sys.exit(1)
    
    with open(args.routes_file) as f:
        routes = json.load(f)
    
    print(f"Loaded {len(routes)} routes from {args.routes_file}")
    
    # Filter to non-raster, pmtiles/mosaic entries
    valid_entries = {
        k: v for k, v in routes.items()
        if v.get("type") != "raster" and v.get("handlertype") in ("pmtiles", "mosaic")
    }
    print(f"Found {len(valid_entries)} non-raster datasets to analyze")
    print(f"Output directory: {args.output_dir}")
    
    if args.limit:
        valid_entries = dict(list(valid_entries.items())[:args.limit])
        print(f"Limiting to {args.limit} datasets")
    
    # Process each dataset
    results = []
    
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        
        for i, (route_key, entry) in enumerate(valid_entries.items(), 1):
            print(f"\n[{i}/{len(valid_entries)}] Processing: {route_key}")
            result = process_dataset(route_key, entry, args.data_dir, temp_path, args.output_dir)
            results.append(result)
            
            # Print summary
            if "analysis" in result:
                analysis = result["analysis"]
                if "error" in analysis:
                    print(f"  Error: {analysis['error']}")
                else:
                    unique_fields = [k for k, v in analysis.get("fields", {}).items() if v.get("is_unique")]
                    if unique_fields:
                        print(f"  Unique fields: {', '.join(unique_fields)}")
                    else:
                        print(f"  No single unique field found")
            elif "error" in result:
                print(f"  Error: {result['error']}")
            elif "skipped" in result:
                print(f"  Skipped: {result['skipped']}")
    
    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    
    analyzed = [r for r in results if "analysis" in r and "error" not in r.get("analysis", {})]
    datasets_with_unique = [r for r in analyzed if any(v.get("is_unique") for v in r["analysis"].get("fields", {}).values())]
    datasets_without_unique = [r for r in analyzed if not any(v.get("is_unique") for v in r["analysis"].get("fields", {}).values())]
    datasets_with_errors = [r for r in results if "error" in r or "error" in r.get("analysis", {})]
    datasets_skipped = [r for r in results if "skipped" in r]
    
    print(f"\nAnalyzed: {len(analyzed)}")
    print(f"Skipped (already done or other): {len(datasets_skipped)}")
    print(f"Errors: {len(datasets_with_errors)}")
    print(f"With unique field(s): {len(datasets_with_unique)}")
    print(f"Without unique field: {len(datasets_without_unique)}")


if __name__ == "__main__":
    main()
