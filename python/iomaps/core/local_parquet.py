"""Resolve local GeoParquet files without consulting the remote catalog."""

import json
from pathlib import Path

from shapely.geometry import box


def local_parquet_paths(inputs, filter_shape=None):
    paths = []
    seen = set()
    for value in inputs:
        path = Path(value).resolve()
        if path.name.endswith(".parquet.meta.json"):
            extents = json.loads(path.read_text()).get("extents")
            if not isinstance(extents, dict) or not extents:
                raise ValueError("Partition manifest must contain nonempty extents")
            candidates = []
            for name, extent in extents.items():
                if Path(name).name != name or not name.endswith(".parquet"):
                    raise ValueError(f"Invalid partition name: {name}")
                candidates.append((path.parent / name, extent))
        else:
            candidates = [(path, None)]
        for candidate, extent in candidates:
            candidate = candidate.resolve()
            if candidate.suffix != ".parquet" or not candidate.is_file():
                raise ValueError(f"Missing local Parquet: {candidate}")
            if candidate in seen:
                raise ValueError(f"Duplicate local Parquet: {candidate}")
            seen.add(candidate)
            if filter_shape is not None and extent is not None:
                try:
                    bounds = [float(extent[k]) for k in ("minx", "miny", "maxx", "maxy")]
                    import math

                    if not all(math.isfinite(v) for v in bounds) or bounds[0] > bounds[2] or bounds[1] > bounds[3]:
                        raise ValueError("Invalid partition extent")
                    if not filter_shape.intersects(box(*bounds)):
                        continue
                except (KeyError, TypeError, ValueError):
                    # Incomplete extents must never suppress a valid partition.
                    pass
            paths.append(str(candidate))
    return paths
