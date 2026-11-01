STAC Parquet regression fixtures:

- `stac-legacy.parquet`: the first record (`40D_15`) from the previously published
  `ramSeraph/india_topo_maps`, `50k-osm-georef/collection.parquet`. It intentionally
  has no GeoParquet schema metadata or projection-extension declaration.
- `stac-modern.parquet`: a synthetic triangular footprint produced by
  `iomaps admin stac`, including GeoParquet metadata and projection extension v2.

Tests decode the actual bytes through hyparquet. They do not fetch imagery.

`stac-ranges.parquet` expands the synthetic modern fixture into 192 items with
six row groups, page indexes and deterministic payloads. It checks that remote
search and pagination skip unrelated columns and row groups. Regenerate with
`python/.venv/bin/python worker/test/helpers/stac_range_fixture.py` from the root.
