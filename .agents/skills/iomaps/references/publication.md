# Dataset preparation and publication

Use the checkout's current `AGENTS.md` and the “Preparing and publishing datasets”
section of `python/README.md` as the operational references. The commands are
`iomaps admin prepare` and `iomaps admin make-release`, not the older utility
scripts. A request to extract data or create coverage does not imply publication.

## Prepare

Inspect similar catalog entries and existing releases to choose the final
repository, release tag, route, basename, categories, and source ID. Keep working
data under ignored `scratch/<dataset>/` and preserve the supplied original.
Choose the basename before preparation because it determines archive members,
tile layer names, and derived asset names.

```bash
python/.venv/bin/iomaps admin prepare \
  scratch/dataset/Dataset.geojsonl \
  'https://example.org/source' 'Source name'
```

Substitute the actual source URL and name. Preparation produces original archives,
PMTiles, GeoParquet (possibly partitioned), ID analysis, and coverage PNG by default.
Use `--no-coverage` to skip the PNG; publication includes it automatically when present.
Check counts, coordinates, properties, and IDs in both Parquet and PMTiles. Record
reported source geometry problems. `OGC_FID` introduced during conversion may not
exist in the tiles; choose `--promoteid` from an ID that is actually present there.

Preparation keeps valid WGS84 coordinates outside India unchanged. Cleaning reports
and drops malformed/non-finite coordinates and repairs only globally out-of-range
values when possible. `--no-india` disables India-based repair guesses, not global
coordinate validation. `--no-filter` skips cleaning but still validates input or
imported geometry and generated Parquet; `--no-coverage` is not a validation bypass.
Review all reported repairs and drops before publication. Coverage alone has a
different policy: warn and skip unusable features while continuing the index.

Large inputs can use `--bounded-parquet --parquet-memory-limit 256MB`. Consult the
repository guide for remote preparation, disk capacity, durable logs, resuming
imports, and source preservation; the DuckDB limit is not a process-wide RAM cap.
`--no-parquet` also skips coverage; an explicit `--coverage --no-parquet` is an
error. Use `admin coverage` for existing Parquet instead.

## Publish through a saved plan

The route publisher handles its own synchronization. Do not add a separate manual
fetch merely to publish. Separate tooling commits require the synchronization
checks described in `AGENTS.md` and must preserve existing local edits.

Build a concrete offline preview with all intended publication options:

```bash
python/.venv/bin/iomaps admin make-release --dry-run --owner ramSeraph \
  --repo DATA_REPOSITORY --release EXISTING_RELEASE_TAG \
  --base-file scratch/dataset/Dataset.geojsonl \
  --description 'Dataset name' --source 'Source name' \
  --source-url 'https://example.org/source' \
  --route /chosen/route/ --category CATEGORY --promoteid SOURCE_ID \
  --license-file scratch/dataset/license.md \
  > scratch/dataset/release-preview.json
```

Fill in actual values after inspecting existing conventions. Read the saved JSON
and show the exact filenames, destinations, route, and actual license text when
review is requested. Once publication is authorized, execute that exact plan:

```bash
python/.venv/bin/iomaps admin make-release \
  --plan scratch/dataset/release-preview.json
```

Let this command perform uploads, release notes, route commit/push, and verification.
Do not implement parallel publication scripts or hand-edit listing entries for
this workflow. Existing authorization persists; do not ask for it again.
`--no-uploads` still changes notes/routes; it is not a dry run. `--no-verify` skips
live checks, so that run alone does not establish that the dataset is live.

Inspect the generated receipt. After interruption, retry the same plan and receipt:
matching assets are verified and skipped, while differing assets are never
overwritten. Inspect conflicts rather than deleting/replacing remote assets.
For server-side assets and local route commits, use the existing `--phase assets`
and `--phase routes`; only the exact plan and receipt need to travel between hosts.
Ready-file batches use the existing `--phase uploads` workflow in the README.

Use `--owner ramSeraph` explicitly for this project's releases. The command reads
`GH_TOKEN`, then `GITHUB_TOKEN`, then the checkout's supported token files. Keep
credentials out of output and handoff files. Route pushes use the checkout's Git
credentials, which can differ from GitHub API credentials.
