---
name: iomaps-admin
description: Use the Indianopenmaps iomaps admin commands to generate coverage PNGs from local GeoParquet, prepare dataset assets, and publish or resume releases through saved plans and receipts. Excludes client discovery and extraction, and repository implementation.
---

# Indianopenmaps administration

Use the existing `iomaps admin` commands for coverage generation, dataset
preparation, and publication. The executable is `iomaps`, including when the
request spells it “iompas”. Creating coverage or preparing assets does not imply
publication.

## Choose the runtime

Use the Indianopenmaps checkout containing `python/pyproject.toml` and
`worker/src/routes/listing.json`. Read its root `AGENTS.md` for local instructions.
Run commands from that repository root using `python/.venv/bin/iomaps`; examples
below use that environment. If it is missing, use
`uv sync --project python --extra admin`.

Admin commands are hidden from top-level help but available through
`python/.venv/bin/iomaps admin --help`. Inspect the relevant command's `--help`
before relying on newer options. Consult `python/README.md` for maintained usage,
external tool requirements, and remote preparation setup. Repository implementation
work follows the checkout's instructions.

## Create coverage

```bash
python/.venv/bin/iomaps admin coverage \
  scratch/dataset/Dataset.parquet \
  --output scratch/dataset/Dataset.coverage.png
```

Pass every final Parquet partition, or its existing `Dataset.parquet.meta.json`.
Inputs are local GeoParquet 1.0/1.1 WKB with planar WGS84 coordinates. Do not use
simplified PMTiles as the geometry source.

The output is one **1-bit PNG**: white means occupied, black means empty, north is
at the top. The fixed bounds are `(67,5,98,38)`; default dimensions are 1000×1000.
`--grid-size N` changes resolution and `--batch-size N` changes the geometry batch.
Features entirely outside those bounds contribute nothing; crossing features
contribute only within them. Cells touched at edges or corners count. There is no
coverage manifest or provenance sidecar. Null/empty geometry contributes nothing;
invalid shapes with usable coordinates mark their own bbox cells and produce a
warning identifying the source and first affected row in the batch. This may add
false positives. Malformed WKB, unusable coordinates, and geometry-engine errors
produce warnings and skip only the affected features; the command reports their
count as `skipped_geometries`. Unreadable Parquet files and unsupported CRS metadata
still fail creation.
Report source problems without editing or repairing the Parquet. `--overwrite`
replaces an existing PNG only after successful creation.

`admin prepare` generates coverage by default after final partitioning; use
`--no-coverage` to opt out. Its resolution option is `--coverage-grid-size`.
`--no-parquet` also skips coverage. `make-release` automatically includes the
common `<basename>.coverage.png` when present, with normal asset hash verification.

## Prepare or publish

For dataset preparation or publication, read
[references/publication.md](references/publication.md), plus the checkout's
`AGENTS.md` and `python/README.md`. Use the existing preparation and release
commands, including their saved-plan, receipt, and resume workflows. Archive
generation remains part of this admin workflow even though the old
archive-consumer tools have been removed.
