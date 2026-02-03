#!/bin/bash
# Script to fix partitioned parquet files by creating metadata JSON
# For each pmtiles or mosaic.json file with multiple parquet partitions,
# creates a <base_name>.parquet.meta.json and uploads it to the release

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${1:-data}"

# Function to process parquet files for a given base name
process_parquets() {
    local base_name="$1"
    local files_txt="$2"
    local release_dir="$3"
    local source_file="$4"
    
    # Look for exact match or partitioned parquet files (base_name.NN.parquet pattern)
    parquet_files=$(grep -E "^${base_name}(\.[0-9]+)?\.parquet$" "$files_txt" 2>/dev/null || true)
    
    if [[ -n "$parquet_files" ]]; then
        count=$(echo "$parquet_files" | wc -l)
        if [[ $count -gt 1 ]]; then
            echo "=== $release_dir ==="
            echo "Source: $source_file"
            echo "Parquet files ($count):"
            echo "$parquet_files" | sed 's/^/  /'
            
            # Extract repo and tag from release_dir (format: data/<repo>/<tag>)
            repo=$(echo "$release_dir" | cut -d'/' -f2)
            tag=$(echo "$release_dir" | cut -d'/' -f3)
            
            # Convert parquet_files to array
            parquet_array=()
            while IFS= read -r pf; do
                parquet_array+=("$pf")
            done <<< "$parquet_files"
            
            # Call Python script to create and upload metadata
            echo "  Creating metadata..."
            python3 "$SCRIPT_DIR/create_parquet_meta.py" "$repo" "$tag" "$base_name" \
                --release-dir "$release_dir" \
                --force \
                "${parquet_array[@]}"
            
            echo ""
        fi
    fi
}

# Find all files.txt
find "$DATA_DIR" -name "files.txt" | while read -r files_txt; do
    release_dir=$(dirname "$files_txt")
    
    # Get pmtiles files from this files.txt
    pmtiles_files=$(grep '\.pmtiles$' "$files_txt" 2>/dev/null || true)
    
    if [[ -n "$pmtiles_files" ]]; then
        while IFS= read -r pmtiles; do
            base_name="${pmtiles%.pmtiles}"
            process_parquets "$base_name" "$files_txt" "$release_dir" "$pmtiles"
        done <<< "$pmtiles_files"
    fi
    
    # Get mosaic.json files from this files.txt
    mosaic_files=$(grep '\.mosaic\.json$' "$files_txt" 2>/dev/null || true)
    
    if [[ -n "$mosaic_files" ]]; then
        while IFS= read -r mosaic; do
            base_name="${mosaic%.mosaic.json}"
            process_parquets "$base_name" "$files_txt" "$release_dir" "$mosaic"
        done <<< "$mosaic_files"
    fi
done
