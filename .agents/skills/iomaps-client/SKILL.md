---
name: iomaps-client
description: Use the Indianopenmaps iomaps client to discover published vector datasets, find sources covering an area, and extract features for local analysis through the CLI or optional GUI. Excludes coverage generation, dataset preparation, publication, and repository implementation.
---

# Indianopenmaps client

Use the existing `iomaps cli` commands to discover and extract Indianopenmaps data.
The executable is `iomaps`, including when the request spells it “iompas”.

## Choose the runtime

Use an installed `iomaps` or `uvx iomaps`; a repository checkout is optional for
client tasks. Examples below use `iomaps`. Inspect the relevant command's
`--help` before relying on newer options because the published package may lag
the checkout.

When working in an Indianopenmaps checkout containing `python/pyproject.toml` and
`worker/src/routes/listing.json`, read its root `AGENTS.md` and prefer
`python/.venv/bin/iomaps` from the repository root. If that environment is missing,
use `uv sync --project python`. Client commands do not need the `admin` extra.
Consult `python/README.md` for maintained usage details. Repository implementation
work follows the checkout's instructions.

## Choose the evidence for the question

| Question | Command | What the result establishes |
| --- | --- | --- |
| What datasets are listed? | `cli sources --search ... --json` | Catalog names, routes and categories; no spatial coverage check. |
| Which datasets cover this area? | `cli search --bounds ... --category ... --json` | Approximate candidates and sources with unknown coverage. |
| Does this source contain features here? | `cli extract --source /exact/route/ --bounds ...` | Actual intersecting features; inspect the output count. |

For area-based discovery, start with `search` rather than downloading candidate
layers to find their coverage. If the source is already known, direct extraction
can answer the exact geometry question without building the whole coverage cache.
Do not repeat it merely to obtain a weaker coverage check. If a published CLI
lacks `search`, check the checkout runtime before declaring it unavailable.

Keep absence claims scoped: no catalog entry, unknown coverage, no indexed
coverage in a bbox, and zero extracted features are different results. Confirm
the intended route exists before interpreting an empty filtered search; fuzzy
name/category filters can also exclude it. A listed source need not cover the
whole country. The live catalog also does not inventory unpublished local files
or every release asset; distinguish those when the user asks what data they have.

## Find a source

```bash
iomaps cli source-categories --json
iomaps cli sources --search districts --json
iomaps cli sources --category buildings --json
```

Use `--json` for machine-readable stdout; status messages go to stderr. Source
listing covers vector datasets. Search matches names and routes with fuzzy
matching, so inspect the returned name, route, and categories before extracting.
The categories command is **`source-categories`**.

Prefer the exact returned route as a stable source reference. Numeric IDs are
positions in the current catalog and can change. `-s` means search for `sources`
and source for `extract`.

For a local catalog in the checkout, pass `--routes-file worker/src/routes/listing.json` to both
`cli sources` and `cli extract`. This makes discovery local; extraction still
reads the source's remote Parquet URLs. `--routes-url` selects another catalog
endpoint and cannot be combined with `--routes-file`.

## Find sources covering an area

```bash
iomaps cli search --bounds '77.1,28.5,77.3,28.7' --json
iomaps cli search --bounds '77.1,28.5,77.3,28.7' --force-update
```

The first search caches the catalog and coverage PNGs with eight download workers.
Warm searches read only a local combined bitmask index and source list: no network
requests, update checks, or SQLite access. Use `--force-update` (alias `--refresh`)
only when a refresh is wanted; it may use ETags and 304 responses. SQLite holds
original compressed downloads and ETags for creation/refresh, not spatial cells.
`--cache-dir` or `IOMAPS_CACHE_DIR` overrides the platform cache directory. Different
catalogs use separate caches; `--routes-file` and `--routes-url` select the catalog.

The JSON object has `matches` and `unknown` lists. Missing PNGs are **unknown**,
not known-empty coverage. Do not claim absence when relevant sources are unknown.
`coverage` contains whole-catalog counts; `--category` and `--search` filter both
lists. Results give stable routes for `cli extract --source`. Matches are
approximate at cell resolution; no exact geometry check runs during discovery.
Search is limited to `(67,5,98,38)`. Partially outside bboxes report clipped bounds;
wholly outside bboxes fail.

The default 1000×1000 coverage grid has cells of 0.031° longitude by 0.033°
latitude, roughly **3.5 × 3.5 km** (east-west ground width varies with latitude).
A matching cell can contain a feature outside the requested bbox. Use extraction
to resolve exact presence; search does not measure shoreline position or gaps.

The PNG uses the common Parquet basename: `Plots.parquet` or all partitions in
`Plots.parquet.meta.json` map to one `Plots.coverage.png`. Its URL is derived from
the corresponding PMTiles/mosaic URL unless the catalog sets `coverage_url`.
Existing sources need their coverage uploaded before they can match spatially.

## Extract an area

```bash
iomaps cli extract \
  --source /states/soi/ \
  --bounds '77.1,28.5,77.3,28.7' \
  --output-file scratch/extract/states.gpkg
```

Create the output directory first. Substitute a route discovered for the user's
task. Bounds are **west,south,east,north**, in WGS84 longitude/latitude. Omit the
spatial filter only when a full-source extraction is intended.

`--filter-file area.geojson` supplies a polygon instead of `--bounds`. The filter
coordinates must be WGS84 longitude/latitude; the current reader does not reproject
them. For a file with several polygons, select one with
`--pick-filter-feature-id 0` (zero-based polygon position) or
`--pick-filter-feature-kv 'name=Area'`. Bounds and filter-file are mutually exclusive.

The command selects intersecting features and retains their full geometry; it
does not clip them to the requested area. It handles partition metadata and reads
Parquet ranges through DuckDB. Do not download complete datasets first merely to
perform a small extraction. Range reads can still transfer large row groups.

Choose the output by extension (`.gpkg`, `.geojson`, `.geojsonl`, `.fgb`, `.csv`,
`.shp`, `.parquet`) or use a driver advertised by `--help`. GeoParquet drivers are
`geoparquet-1.1` and `geoparquet-2.0`; `.parquet` defaults to 1.1. Keep all Shapefile
sidecars together. Confirm that output was created and inspect its feature count,
CRS, and representative geometry/properties before reporting success. Some empty
or unresolved-source paths log a message and return without writing a file.

## Match administrative boundaries to a register

Inspect catalog source/notes and extracted field schemas before joining a
boundary layer to an administrative register. `sources --json` gives discovery
fields, not the full provenance or effective date. Retain the underlying catalog
entry and source URL. Dataset names and download dates do not establish current
administrative coverage. Points, habitations and local-body polygons are not
interchangeable with revenue village or subdistrict polygons.

Inspect entity types within a layer: a panchayat dataset can also contain
municipal polygons and unnamed or zero-code features (observed in Goa's Bhuvan
and LGD extracts). Match and classify these separately before counting coverage
or unioning them. A source field called `ulbcode` can contain census town codes;
bridge it through the register's census field rather than treating it as an LGD
local-body code. Likewise, `gp_mapping`/ULB coverage associations do not supply
precise boundaries or justify copying a municipality outline to each village.
Retain state-specific source preferences and compare actual polygon fit; do not
infer a universal quality ranking from code coverage or a gap-free union.

Prefer verified identifiers over names, preserving code namespaces and leading
zeros. Record original source parents alongside current register parents: a
transferred village can retain its code. Report expected codes, located codes,
missing codes and multiple features per code separately. A bbox selection can
include neighbouring districts and exclude misplaced features; verify unresolved
codes against the broader source before declaring source-wide absence.

When the task restricts provenance, check original sources and derivatives before
using them. Keep excluded data out of matching and corroboration, including
implicit geocoder or viewer basemap use. Do not infer source eligibility from a
catalog route name.

## Before using extracted polygons as a mask

A source name describes a dataset, not necessarily every feature in it. Before
rasterizing or unioning polygons, inspect geometry types, classification-field
values, and polygon holes. A river layer can include separate island or other
non-water polygons which fill holes when all features are unioned. Likewise,
coastal regulation lines can mix high/low tide lines with hazard lines and setback
boundaries. Select the relevant feature classes instead of treating the whole
layer as water or coastline.

Verify code meanings from source metadata when available. If a code dictionary
is absent, keep classes separate and label interpretations as provisional; shape
or overlap alone does not establish a general code mapping. Preserve interior
rings in any diagnostic rasterization. Report selected classes, feature counts,
area, bounds and input identity so an apparent geometry problem can be traced
back to the selection. This is needed for mask analysis, not ordinary extraction.

For interactive use, `iomaps ui` provides remote source selection and extraction
with the optional `ui` extra (`pip install "iomaps[ui]"`, or
`uv sync --project python --extra ui` in the checkout). The default CLI install is
headless. The legacy `filter-7z`, `infer-schema`, and their UI views are removed.
