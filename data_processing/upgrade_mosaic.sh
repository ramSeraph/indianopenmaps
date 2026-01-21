#!/bin/bash
# Script to upgrade old mosaic.json files (version 0) to version 1
# Usage: ./upgrade_mosaic.sh <path_to_old_mosaic.json>
#
# Example: ./upgrade_mosaic.sh data/indian_cadastrals/odisha/data/Odisha4kgeo_OD_Cadastrals.mosaic.json

set -e

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <path_to_old_mosaic.json>"
    exit 1
fi

MOSAIC_FILE="$1"

if [[ ! -f "$MOSAIC_FILE" ]]; then
    echo "Error: Mosaic file not found: $MOSAIC_FILE"
    exit 1
fi

# Check if it's an old mosaic (no version field)
VERSION=$(jq -r ".version // \"0\"" "$MOSAIC_FILE")
if [[ "$VERSION" != "0" ]]; then
    echo "Mosaic is already version $VERSION, no upgrade needed."
    exit 0
fi

echo "=== Upgrading old mosaic: $MOSAIC_FILE ==="

# Get the directory containing the mosaic file
MOSAIC_DIR=$(dirname "$MOSAIC_FILE")
MOSAIC_NAME=$(basename "$MOSAIC_FILE" .mosaic.json)

# Parse the path to extract repo and tag
# Expected path: data/<repo>/<tag>/data/<mosaic>.mosaic.json
RELEASE_DIR=$(dirname "$MOSAIC_DIR")  # data/<repo>/<tag>
TAG=$(basename "$RELEASE_DIR")
REPO=$(basename $(dirname "$RELEASE_DIR"))

echo "Repo: $REPO, Tag: $TAG"

# Extract pmtiles file names from old mosaic (keys starting with ../)
echo "Extracting pmtiles file names from old mosaic..."
PMTILES_FILES=$(jq -r 'keys[] | select(startswith("../")) | ltrimstr("../")' "$MOSAIC_FILE")

if [[ -z "$PMTILES_FILES" ]]; then
    # Try keys without ../ prefix
    PMTILES_FILES=$(jq -r 'keys[]' "$MOSAIC_FILE")
fi

echo "Found pmtiles files:"
echo "$PMTILES_FILES"

# Download each pmtiles file from GitHub release
echo ""
echo "=== Downloading pmtiles files ==="
for pmfile in $PMTILES_FILES; do
    if [[ -f "$MOSAIC_DIR/$pmfile" ]]; then
        echo "Already exists: $MOSAIC_DIR/$pmfile"
    else
        echo "Downloading: $pmfile"
        gh release download "$TAG" --repo "ramSeraph/$REPO" --pattern "$pmfile" --dir "$MOSAIC_DIR" || {
            echo "  FAILED to download $pmfile"
            exit 1
        }
    fi
done

# Create staging folder for new files
STAGING_DIR="$MOSAIC_DIR/staging"
echo ""
echo "=== Creating staging folder: $STAGING_DIR ==="
mkdir -p "$STAGING_DIR"

# Create new mosaic using partition
echo ""
echo "=== Creating new mosaic with pmtiles_mosaic ==="
OUTPUT_PMTILES="$STAGING_DIR/${MOSAIC_NAME}.pmtiles"

# Build the --from-source arguments
FROM_SOURCES=""
for pmfile in $PMTILES_FILES; do
    FROM_SOURCES="$FROM_SOURCES --from-source $MOSAIC_DIR/$pmfile"
done

echo "Running: uvx --from pmtiles_mosaic partition $FROM_SOURCES --to-pmtiles $OUTPUT_PMTILES"
uvx --from pmtiles_mosaic partition $FROM_SOURCES --to-pmtiles "$OUTPUT_PMTILES"

echo ""
echo "=== Verifying new mosaic ==="
NEW_MOSAIC="$STAGING_DIR/${MOSAIC_NAME}.mosaic.json"
NEW_VERSION=$(jq -r ".version // \"0\"" "$NEW_MOSAIC")
echo "New mosaic version: $NEW_VERSION"

if [[ "$NEW_VERSION" == "0" ]]; then
    echo "ERROR: New mosaic still has version 0, something went wrong"
    exit 1
fi

# Get list of new pmtiles files
echo ""
echo "=== Identifying new pmtiles files ==="
NEW_PMTILES_FILES=$(jq -r '.slices | keys[]' "$NEW_MOSAIC")
echo "New pmtiles files:"
echo "$NEW_PMTILES_FILES"

# Delete old files from GitHub release
echo ""
echo "=== Removing old files from GitHub release ==="
OLD_MOSAIC_NAME=$(basename "$MOSAIC_FILE")
echo "Deleting old mosaic: $OLD_MOSAIC_NAME"
gh release delete-asset "$TAG" --repo "ramSeraph/$REPO" "$OLD_MOSAIC_NAME" --yes || echo "  (may not exist)"

for pmfile in $PMTILES_FILES; do
    echo "Deleting old pmtiles: $pmfile"
    gh release delete-asset "$TAG" --repo "ramSeraph/$REPO" "$pmfile" --yes || echo "  (may not exist)"
done

# Upload new files to GitHub release
echo ""
echo "=== Uploading new files to GitHub release ==="
echo "Uploading new mosaic: $NEW_MOSAIC"
gh release upload "$TAG" --repo "ramSeraph/$REPO" "$NEW_MOSAIC" --clobber

for pmfile in $NEW_PMTILES_FILES; do
    echo "Uploading new pmtiles: $STAGING_DIR/$pmfile"
    gh release upload "$TAG" --repo "ramSeraph/$REPO" "$STAGING_DIR/$pmfile" --clobber
done

# Cleanup local old pmtiles files (keep backups)
echo ""
echo "=== Cleaning up local old pmtiles files ==="
for pmfile in $PMTILES_FILES; do
    # Only delete if it's not in the new files list
    if ! echo "$NEW_PMTILES_FILES" | grep -q "^${pmfile}$"; then
        echo "Removing old local file: $MOSAIC_DIR/$pmfile"
        rm -f "$MOSAIC_DIR/$pmfile"
    fi
done

echo ""
echo "=== Upgrade complete ==="
echo "New mosaic and pmtiles in staging folder: $STAGING_DIR"
echo "New mosaic: $NEW_MOSAIC (version $NEW_VERSION)"
echo ""
echo "To finalize, you can move files from staging to parent and clean up:"
echo "  mv $STAGING_DIR/* $MOSAIC_DIR/"
echo "  rmdir $STAGING_DIR"
