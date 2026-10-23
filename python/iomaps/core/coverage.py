"""Build conservative source occupancy on a fixed longitude/latitude grid."""

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import shapely

INDIA_BOUNDS = (67.0, 5.0, 98.0, 38.0)


class CoverageError(ValueError):
    """Coverage could not be completed without silently losing geometry."""


@dataclass(frozen=True)
class CoverageGrid:
    """Closed cells: a geometry on a shared edge belongs to both neighbours."""

    bounds: tuple[float, float, float, float] = INDIA_BOUNDS
    width: int = 1000
    height: int = 1000

    def __post_init__(self):
        if len(self.bounds) != 4 or not all(np.isfinite(self.bounds)):
            raise CoverageError("Grid bounds must contain four finite numbers")
        west, south, east, north = self.bounds
        if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
            raise CoverageError("Grid bounds must be ordered longitude/latitude bounds without antimeridian wrapping")
        if any(isinstance(n, bool) or not isinstance(n, int) or n <= 0 for n in (self.width, self.height)):
            raise CoverageError("Grid width and height must be positive integers")
        if self.width * self.height > 100_000_000:
            raise CoverageError("Coverage grids are limited to 100 million cells")


class Coverage:
    """Accumulate occupancy; memory is proportional to the grid and input batch.

    Internal rows run south to north. The export layer chooses its storage order.
    Small features use their cell ranges directly. Larger features traverse grid
    blocks, pruning empty/previously occupied blocks and filling covered blocks.
    This avoids a geometry-by-all-cells join, including for long diagonal lines.
    """

    def __init__(self, grid=None, *, warning=None):
        self.grid = grid or CoverageGrid()
        self.warning = warning or logging.getLogger(__name__).warning
        west, south, east, north = self.grid.bounds
        self.x_edges = np.linspace(west, east, self.grid.width + 1)
        self.y_edges = np.linspace(south, north, self.grid.height + 1)
        self.extent = shapely.box(west, south, east, north)
        self.cells = np.zeros((self.grid.height, self.grid.width), dtype=np.bool_)
        self.counts = {
            "rows": 0,
            "null_geometries": 0,
            "empty_geometries": 0,
            "invalid_geometries": 0,
            "skipped_geometries": 0,
            "intersecting_geometries": 0,
            "outside_geometries": 0,
            "partly_outside_geometries": 0,
        }

    def add_wkb(self, values, *, source="input", row_offset=0):
        """Decode one bounded batch, warning and skipping individual bad records."""
        skipped = []
        try:
            geometries = shapely.from_wkb(values)
        except (shapely.errors.GEOSException, TypeError, ValueError):
            geometries = np.empty(len(values), dtype=object)
            for index, value in enumerate(values):
                try:
                    geometries[index] = shapely.from_wkb(value)
                except (shapely.errors.GEOSException, TypeError, ValueError) as row_exc:
                    geometries[index] = None
                    skipped.append((index, f"invalid WKB: {row_exc}"))
            if skipped:
                self._warn_skipped([i for i, _ in skipped], source, row_offset, skipped[0][1])
        self.add(geometries, source=source, row_offset=row_offset)
        # Bad WKB uses a None placeholder to preserve original row numbers.
        self.counts["null_geometries"] -= len(skipped)

    def _warn_skipped(self, indices, source, row_offset, reason):
        self.counts["skipped_geometries"] += len(indices)
        self.warning(
            f"{source}: skipping {len(indices):,} geometries; first at row {row_offset + int(indices[0]) + 1}: {reason}"
        )

    @staticmethod
    def _predicate(predicate, geometries, *args):
        """Keep vectorized predicates fast, isolating individual GEOS failures."""
        try:
            return predicate(geometries, *args), []
        except shapely.errors.GEOSException:
            result = np.zeros(len(geometries), dtype=bool)
            failed = []
            for index, geometry in enumerate(geometries):
                try:
                    result[index] = predicate(geometry, *args)
                except shapely.errors.GEOSException as exc:
                    failed.append((index, str(exc).strip()))
            return result, failed

    def add(self, geometries, *, source="input", row_offset=0):
        """Add WGS84 geometries, conservatively using bboxes for invalid shapes."""
        geometries = np.array(geometries, dtype=object, copy=True)
        if not len(geometries):
            return
        missing = shapely.is_missing(geometries)
        empty = shapely.is_empty(geometries)
        present = ~(missing | empty)
        bounds = shapely.bounds(geometries)
        bad_bounds = present & (
            ~np.isfinite(bounds).all(axis=1)
            | (bounds[:, 0] < -180)
            | (bounds[:, 2] > 180)
            | (bounds[:, 1] < -90)
            | (bounds[:, 3] > 90)
        )
        if bad_bounds.any():
            self._warn_skipped(
                np.flatnonzero(bad_bounds), source, row_offset, "non-finite or out-of-range WGS84 coordinates"
            )
            present[bad_bounds] = False
            geometries[bad_bounds] = None

        valid, failed = self._predicate(shapely.is_valid, geometries)
        if failed:
            indices = [i for i, _ in failed]
            self._warn_skipped(indices, source, row_offset, f"geometry validation failed: {failed[0][1]}")
            present[indices] = False
            geometries[indices] = None
        invalid = np.flatnonzero(present & ~valid)
        if len(invalid):
            # GEOS bounds can ignore NaN vertices. Such a shape has no reliable
            # bbox; skip it instead of marking cells from a misleading bbox.
            coordinates, owners = shapely.get_coordinates(geometries[invalid], return_index=True)
            bad_coordinates = ~np.isfinite(coordinates).all(axis=1)
            if bad_coordinates.any():
                indices = invalid[np.unique(owners[bad_coordinates])]
                self._warn_skipped(indices, source, row_offset, "non-finite WGS84 coordinates")
                present[indices] = False
                geometries[indices] = None
                invalid = invalid[present[invalid]]
        if len(invalid):
            first = int(invalid[0])
            try:
                reason = shapely.is_valid_reason(geometries[first])
            except shapely.errors.GEOSException as exc:
                reason = str(exc).strip()
            self.warning(
                f"{source}: using feature bboxes for {len(invalid):,} invalid geometries; "
                f"first at row {row_offset + first + 1}: {reason}"
            )
            geometries = geometries.copy()
            for index in invalid:
                west, south, east, north = bounds[index]
                if west == east and south == north:
                    geometries[index] = shapely.Point(west, south)
                elif west == east or south == north:
                    geometries[index] = shapely.LineString([(west, south), (east, north)])
                else:
                    geometries[index] = shapely.box(west, south, east, north)

        intersects, failed = self._predicate(shapely.intersects, geometries, self.extent)
        if failed:
            indices = [i for i, _ in failed]
            self._warn_skipped(indices, source, row_offset, f"geometry intersection failed: {failed[0][1]}")
            present[indices] = False
        intersecting = present & intersects
        west, south, east, north = self.grid.bounds
        partly_outside = intersecting & (
            (bounds[:, 0] < west) | (bounds[:, 1] < south) | (bounds[:, 2] > east) | (bounds[:, 3] > north)
        )
        self.counts["rows"] += len(geometries)
        self.counts["null_geometries"] += int(missing.sum())
        self.counts["empty_geometries"] += int(empty.sum())
        self.counts["invalid_geometries"] += len(invalid)
        self.counts["intersecting_geometries"] += int(intersecting.sum())
        self.counts["outside_geometries"] += int((present & ~intersecting).sum())
        self.counts["partly_outside_geometries"] += int(partly_outside.sum())
        if not intersecting.any() or self.cells.all():
            return

        row_indices = np.flatnonzero(intersecting)
        geometries, bounds = geometries[intersecting], bounds[intersecting]
        # Search the actual shared edges, avoiding rounding a point on an edge
        # into just one cell. Stops are exclusive; starts include the west/south
        # neighbour when a geometry's minimum lies exactly on an interior edge.
        c0 = np.maximum(0, np.searchsorted(self.x_edges, bounds[:, 0], side="left") - 1)
        c1 = np.minimum(self.grid.width, np.searchsorted(self.x_edges, bounds[:, 2], side="right"))
        r0 = np.maximum(0, np.searchsorted(self.y_edges, bounds[:, 1], side="left") - 1)
        r1 = np.minimum(self.grid.height, np.searchsorted(self.y_edges, bounds[:, 3], side="right"))
        single = (c1 - c0 == 1) & (r1 - r0 == 1)
        self.cells[r0[single], c0[single]] = True
        failed = []
        for index in np.flatnonzero(~single):
            region = self.cells[r0[index] : r1[index], c0[index] : c1[index]]
            if region.all():
                continue
            before = region.copy()
            try:
                self._burn(geometries[index], int(c0[index]), int(r0[index]), int(c1[index]), int(r1[index]))
            except shapely.errors.GEOSException as exc:
                # A feature may fail after marking some cells. Preserve cells
                # from earlier features, while removing this feature's changes.
                region[:] = before
                original_index = int(row_indices[index])
                self.counts["intersecting_geometries"] -= 1
                self.counts["partly_outside_geometries"] -= int(partly_outside[original_index])
                failed.append((original_index, str(exc).strip()))
        if failed:
            self._warn_skipped(
                [i for i, _ in failed], source, row_offset, f"geometry processing failed: {failed[0][1]}"
            )

    def _burn(self, geometry, c0, r0, c1, r1):
        shapely.prepare(geometry)
        pending = [(c0, r0, c1, r1)]
        while pending:
            c0, r0, c1, r1 = pending.pop()
            region = self.cells[r0:r1, c0:c1]
            if region.all():
                continue
            block = shapely.box(self.x_edges[c0], self.y_edges[r0], self.x_edges[c1], self.y_edges[r1])
            if not shapely.intersects(geometry, block):
                continue
            if shapely.covers(geometry, block):
                region[:] = True
            elif region.size <= 256:
                rows, cols = np.nonzero(~region)
                rows, cols = rows + r0, cols + c0
                boxes = shapely.box(
                    self.x_edges[cols], self.y_edges[rows], self.x_edges[cols + 1], self.y_edges[rows + 1]
                )
                self.cells[rows, cols] = shapely.intersects(geometry, boxes)
            elif c1 - c0 >= r1 - r0:
                middle = (c0 + c1) // 2
                pending.extend([(c0, r0, middle, r1), (middle, r0, c1, r1)])
            else:
                middle = (r0 + r1) // 2
                pending.extend([(c0, r0, c1, middle), (c0, middle, c1, r1)])


def _parquet_inputs(inputs):
    """Expand existing preparation partition metadata, checking every part first."""
    paths = []
    for value in inputs:
        path = Path(value).resolve()
        if path.name.endswith(".parquet.meta.json"):
            try:
                extents = json.loads(path.read_text())["extents"]
                if not isinstance(extents, dict) or not extents:
                    raise ValueError("no partitions")
                for name in extents:
                    if not name.endswith(".parquet") or Path(name).name != name:
                        raise ValueError(f"invalid partition filename {name!r}")
                    paths.append(path.parent / name)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                raise CoverageError(f"Cannot read partition list {path}: {exc}") from exc
        else:
            paths.append(path)
    if not paths:
        raise CoverageError("At least one GeoParquet file is required")
    if len(set(paths)) != len(paths):
        raise CoverageError("A Parquet input was listed more than once")
    for path in paths:
        if path.suffix != ".parquet" or not path.is_file():
            raise CoverageError(f"Missing or unsupported Parquet input: {path}")
    return paths


def _geometry_column(parquet, path):
    """Accept the planar WGS84 WKB files produced by preparation, without guessing CRS."""
    import pyarrow as pa
    from pyproj import CRS
    from pyproj.exceptions import CRSError

    try:
        metadata = json.loads((parquet.schema_arrow.metadata or {})[b"geo"])
        if not isinstance(metadata, dict):
            raise ValueError("geo metadata must be an object")
        if metadata.get("version") not in {"1.0.0", "1.1.0"}:
            raise ValueError("requires GeoParquet 1.0 or 1.1 WKB")
        name = metadata["primary_column"]
        column = metadata["columns"][name]
        if not isinstance(column, dict):
            raise ValueError("geometry column metadata must be an object")
        if column["encoding"] != "WKB":
            raise ValueError("geometry encoding must be WKB")
        if column.get("edges", "planar") != "planar":
            raise ValueError("spherical edges are not supported")
        crs = column.get("crs", "OGC:CRS84")
        if crs is None:
            raise ValueError("geometry CRS is explicitly unknown")
        parsed = CRS.from_user_input(crs)
        if parsed not in (CRS.from_epsg(4326), CRS.from_string("OGC:CRS84")):
            raise ValueError("reproject the Parquet to WGS84 longitude/latitude before creating coverage")
        field_type = parquet.schema_arrow.field(name).type
        if not (pa.types.is_binary(field_type) or pa.types.is_large_binary(field_type)):
            raise ValueError("the WKB geometry column must be binary")
        return name
    except (KeyError, TypeError, ValueError, CRSError) as exc:
        raise CoverageError(f"{path}: invalid or unsupported GeoParquet metadata: {exc}") from exc


def _write_png(coverage, output, overwrite):
    """Write only occupancy. PNG row zero is north; each row runs west to east."""
    from PIL import Image

    packed = np.packbits(coverage.cells[::-1], axis=1, bitorder="big")
    image = Image.frombytes("1", (coverage.grid.width, coverage.grid.height), packed.tobytes())
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=output.parent, prefix=f".{output.name}.", suffix=".tmp", delete=False
        ) as stream:
            temporary = Path(stream.name)
            image.save(stream, format="PNG")
            stream.flush()
            os.fsync(stream.fileno())
        if overwrite:
            os.replace(temporary, output)
        else:
            # Publish without a check-then-replace race that could clobber an
            # existing artifact. The temporary file is on the same filesystem.
            os.link(temporary, output)
    finally:
        image.close()
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def create_coverage(inputs, output, *, grid_size=1000, batch_size=8192, overwrite=False, progress=None):
    """Create one standalone PNG from local GeoParquet files or partition metadata.

    Only the geometry column is scanned, in record batches. Invalid shapes use
    their bboxes with a warning. Unusable coordinates, undecodable geometry, and
    geometry-engine failures are warned about and skipped one feature at a time.
    Unreadable partitions still abort creation and preserve any previous PNG.
    Null/empty geometries have no spatial occupancy. The grid always uses
    INDIA_BOUNDS; the PNG dimensions define the resolution. No sidecars or PNG
    metadata are written. Geometry Z/M values do not affect this 2D index.
    """
    import pyarrow.parquet as pq

    output = Path(output).absolute()
    if output.suffix.lower() != ".png":
        raise CoverageError("Coverage output must end in .png")
    if not overwrite and os.path.lexists(output):
        raise CoverageError(f"Output already exists: {output}; use --overwrite to replace it")
    if not isinstance(batch_size, int) or batch_size <= 0:
        raise CoverageError("Batch size must be a positive integer")
    paths = _parquet_inputs(inputs)
    warning = (lambda message: progress(f"Warning: {message}")) if progress else None
    coverage = Coverage(CoverageGrid(width=grid_size, height=grid_size), warning=warning)
    last_progress = time.monotonic()
    for path in paths:
        if progress:
            progress(f"Reading {path.name}")
        before = path.stat()
        row_offset = 0
        try:
            with pq.ParquetFile(path) as parquet:
                column = _geometry_column(parquet, path)
                for batch in parquet.iter_batches(batch_size=batch_size, columns=[column], use_threads=False):
                    coverage.add_wkb(batch.column(0).to_pylist(), source=path.name, row_offset=row_offset)
                    row_offset += batch.num_rows
                    if progress and time.monotonic() - last_progress >= 5:
                        progress(f"{path.name}: {row_offset:,}/{parquet.metadata.num_rows:,} rows")
                        last_progress = time.monotonic()
                if row_offset != parquet.metadata.num_rows:
                    raise CoverageError(f"{path}: row count changed during the scan")
        except CoverageError:
            raise
        except (OSError, ValueError, shapely.errors.GEOSException) as exc:
            raise CoverageError(f"Cannot read coverage from {path}: {exc}") from exc
        after = path.stat()
        if (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino):
            raise CoverageError(f"{path}: input changed during coverage creation")
    _write_png(coverage, output, overwrite)
    return coverage
