# Raster STAC preparation and publication

Use the checkout's `python/.venv/bin/iomaps admin stac` and `admin make-release`
commands. These are available in `iomaps>=0.6.0`; install the `admin` extra and
GDAL (`gdalinfo` on PATH) for preparation. Read command help and the repository's
`AGENTS.md` before publication. `release_tools` no longer owns STAC generation.

## Identify the actual raster inputs

GeoTIFF/COG releases are usually named `*-georef`, with matching `*-orig`
releases for scans. Their consolidated `listing_files.csv` can reference assets
in several `-extra` releases. Inspect the existing release, its notes and
footprints instead of constructing a new release or indexing just its first
asset page. CartoDEM's corrected COGs, for example, share a release with terrain
tiles rather than using a `-georef` suffix. A PMTiles-only release does not
provide GeoTIFF windows for the raster client.

Compare TIFF counts and filenames with the release inventories when metadata
disagrees or downloads return 404. Listings can both retain removed files and
omit newly uploaded TIFFs. Preserve the downloaded inputs, build an audited
working CSV when necessary, and report unavailable assets separately from
successfully indexed rasters. Do not invent URLs for absent sheets.

The original listing must map each sheet basename to one source image/PDF.
Listings may also contain `.map` georeferencing sidecars or multiple image
versions. Select the image supported by the footprint's source filename/URL,
record the choice, and retain unresolved originals as missing. Split/inset COGs
can share an original scan; use an explicit working-listing alias only after
verifying that relationship. Keep the actual original download URL unchanged.

Choose a stable collection ID, final output basename, source URL, description,
explicit STAC license (`other` when unspecified), and timezone-qualified date.
Explain what the date means. A release date or catalog indexing date is usable
when acquisition dates are unknown, but must be labelled accordingly. Public
download access or a code repository's license does not establish map licensing.

## Prepare locally

An example using an existing release (do not republish it merely to test):

```bash
python/.venv/bin/iomaps admin stac \
  --repo ramSeraph/nepal_survey_maps --release jica-georef \
  --original-release jica-orig \
  --collection-id nepal-topo-jica --title 'Nepal JICA Topographic Maps' \
  --description 'Georeferenced JICA and Nepal Survey Department maps. Item datetime is derivative publication, not acquisition.' \
  --license other --datetime '2025-06-22T20:38:27Z' \
  --datetime-description 'Publication of the georeferenced release, not survey or imagery acquisition.' \
  --source-url https://www.maze.com.np/Maps/Topo-Nepal \
  --output scratch/stac/jica/Nepal_JICA.stac.parquet
```

`--repo` plus `--release` reads `listing_files.csv` and `bounds.geojson` from that
release. For other layouts or audited inputs, use `--listing LOCAL_OR_HTTP_CSV`,
optionally `--original-listing` and `--bounds`. Omit `--bounds` in listing mode
to derive WGS84 extents from the TIFF headers. Do not combine listing and repo
modes. CSVs require `name,url`; `size,sha256` strengthen header-cache identity.

Supplied footprints must have unique `id`/`properties.id` values covering every
TIFF and valid WGS84 Polygon/MultiPolygon geometry. The generator rejects
missing/invalid footprints and duplicate IDs. Preserve source geometry problems
in an audit. If using a COG-derived extent for an affected sheet, retain the raw
footprint and record the replacement and reason in the working metadata/notes;
do not silently repair it or discard the TIFF. A TIFF extent can include nodata
outside the actual map cutline.

Preparation reads headers, not full images. Successful reads persist in
`stac-cache/` beside the output, or `--cache-dir`. Retry with the same cache after
a timeout. `--workers` defaults to 4 (maximum 16); bound total concurrency across
simultaneous collections to available memory and upstream capacity.
`--refresh` rereads headers. `--reuse-index` explicitly trusts header metadata
from an existing index only for identical asset URLs; use it only when those
assets are unchanged. Refresh bypasses both reuse and cached headers.

Review the `.report.json`: expected/prepared counts, failures, missing
originals and `metadata_warnings`. Legacy TIFF comments can contain non-UTF-8
bytes; the generator preserves georeferencing, replaces undecodable descriptive
bytes and records the affected sheet IDs in the report/header cache. Any failed sheet prevents writing a new index; an old output can
remain, but the publisher rejects a failed latest report. Retry transient
failures, investigate persistent failures, and do not publish an unexplained
partial collection. Missing original scans do not prevent retaining valid COGs.

Outputs are the Parquet index, `.collection.json` sidecar and report. Collection
metadata is also embedded in the Parquet. Item geometry is WGS84 while projection
fields describe the actual raster. COGs and ordinary GeoTIFFs are labelled from
their headers. Dates and source properties retain their stated meanings.
The current generator writes Snappy-compressed row groups of at most 512 items
and Parquet page indexes to support bounded remote reads. Do not rewrite an
already-published index in place to change its layout: use a new basename and
regenerate the saved publication plan and hashes.

## Publish the saved preview

Read [publication.md](publication.md) for the shared authorization, sync, receipt
and resume workflow. Supply the actual license text explicitly; the STAC path
does not use the vector publisher's default license.

```bash
python/.venv/bin/iomaps admin make-release --dry-run --owner ramSeraph \
  --repo nepal_survey_maps --release jica-georef \
  --base-file scratch/stac/jica/Nepal_JICA.stac.parquet \
  --stac-collection scratch/stac/jica/Nepal_JICA.stac.collection.json \
  --license-file scratch/stac/jica/license.md \
  > scratch/stac/jica/release-preview.json
python/.venv/bin/iomaps admin make-release \
  --plan scratch/stac/jica/release-preview.json --verify-timeout 600
```

Review the exact asset names, hashes, collection metadata, notes and license in
the preview. Use `--notes`/`--notes-file` for source limitations, stale-listing
reconciliation, missing originals or footprint substitutions. Once publication
is authorized, execute that plan without another confirmation. The publisher
uploads index/sidecar, updates its release-note section, commits and pushes only
`worker/src/routes/stac_catalog.json`, and verifies GitHub plus deployment.
Publish catalog mutations sequentially in a shared checkout. Existing differing
release assets are not overwritten; choose a new index basename when replacing
published content. Phase/resume rules are the same as other dataset releases.

Check the receipt for `published and verified live`, including counts, sample
and late item IDs, bbox search and release asset URLs. Preparation alone is not
publication. For requested external-client compatibility checks, use the
repository's `python/tests/test_stac_compatibility.py` and
`worker/test/STAC_COMPATIBILITY.md`; distinguish a transient upstream failure
from a completed conformance test.

The Python raster client reads raw GitHub catalog metadata and release Parquet/
COG assets directly. It never needs the CSV or the Indian Open Maps STAC API.
Adding a collection requires no package release; users refresh cached metadata
with `iomaps cli stac --refresh collections`. The web imagery renderer supports
north-up EPSG:3857 byte COGs; other rasters, including numeric DEMs, remain usable
for discovery and Python/GDAL extraction.
