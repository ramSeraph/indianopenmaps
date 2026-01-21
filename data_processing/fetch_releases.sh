#!/bin/bash
# Resumable script to fetch all repos, their releases, and release assets
# Creates: data/<repo>/<release>/description.md and data/<repo>/<release>/files.txt

set -e

DATA_DIR="data"
mkdir -p "$DATA_DIR"

echo "Fetching list of repositories..."

# Get all repos (handles pagination)
repos=$(gh repo list --limit 1000 --json nameWithOwner -q '.[].nameWithOwner')

for repo in $repos; do
    repo_name=$(basename "$repo")
    repo_dir="$DATA_DIR/$repo_name"
    
    echo "Processing repo: $repo"
    
    # Check if repo directory exists and has a .done marker
    if [[ -f "$repo_dir/.done" ]]; then
        echo "  Skipping $repo (already completed)"
        continue
    fi
    
    mkdir -p "$repo_dir"
    
    # Get all releases for this repo
    releases=$(gh release list --repo "$repo" --limit 1000 --json tagName -q '.[].tagName' 2>/dev/null || echo "")
    
    if [[ -z "$releases" ]]; then
        echo "  No releases found for $repo"
        touch "$repo_dir/.done"
        continue
    fi
    
    for tag in $releases; do
        # Sanitize tag name for directory (replace / with -)
        safe_tag=$(echo "$tag" | tr '/' '-')
        release_dir="$repo_dir/$safe_tag"
        
        # Check if this release is already done
        if [[ -f "$release_dir/.done" ]]; then
            echo "  Skipping release $tag (already completed)"
            continue
        fi
        
        echo "  Processing release: $tag"
        mkdir -p "$release_dir"
        
        # Get release description (body) and save as markdown
        if [[ ! -f "$release_dir/description.md" ]]; then
            gh release view "$tag" --repo "$repo" --json body -q '.body' > "$release_dir/description.md" 2>/dev/null || echo "" > "$release_dir/description.md"
        fi
        
        # Get list of assets/files in the release
        if [[ ! -f "$release_dir/files.txt" ]]; then
            gh release view "$tag" --repo "$repo" --json assets -q '.assets[].name' > "$release_dir/files.txt" 2>/dev/null || echo "" > "$release_dir/files.txt"
        fi
        
        # Mark release as done
        touch "$release_dir/.done"
        echo "    Completed release: $tag"
    done
    
    # Mark repo as done
    touch "$repo_dir/.done"
    echo "  Completed repo: $repo"
done

echo "All done! Data saved to $DATA_DIR/"
