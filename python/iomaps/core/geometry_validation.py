"""Validate coordinate values without restricting data to a particular country."""

import json
import math
import time
from pathlib import Path


class CoordinateError(ValueError):
    def __init__(self, message, *, repairable=False):
        super().__init__(message)
        self.repairable = repairable


def _positions(geometry):
    if not isinstance(geometry, dict):
        raise CoordinateError("geometry must be an object")
    kind = geometry.get("type")
    if kind == "GeometryCollection":
        members = geometry.get("geometries")
        if not isinstance(members, list):
            raise CoordinateError("GeometryCollection.geometries must be an array")
        for member in members:
            yield from _positions(member)
        return
    depths = {"Point": 0, "MultiPoint": 1, "LineString": 1, "MultiLineString": 2, "Polygon": 2, "MultiPolygon": 3}
    if kind not in depths:
        raise CoordinateError(f"unsupported geometry type: {kind!r}")

    def walk(value, depth):
        if not isinstance(value, (list, tuple)):
            raise CoordinateError("coordinates must be arrays of numeric positions")
        if depth == 0:
            if len(value) < 2:
                raise CoordinateError("each position needs longitude and latitude")
            yield value
        else:
            for child in value:
                yield from walk(child, depth - 1)

    coordinates = geometry.get("coordinates")
    if coordinates == []:
        return
    yield from walk(coordinates, depths[kind])


def validate_geometry_coordinates(geometry):
    """Return the number of positions; null and empty geometry have none.

    Every ordinate must be numeric and finite. Longitude/latitude use worldwide
    WGS84 limits. Topology validity and country membership are separate concerns.
    """
    if geometry is None:
        return 0
    count = 0
    for position in _positions(geometry):
        for value in position:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise CoordinateError("coordinates must be numeric, not strings, booleans or null")
            try:
                finite = math.isfinite(value)
            except OverflowError:
                finite = False
            if not finite:
                raise CoordinateError("non-finite coordinates")
        lon, lat = position[:2]
        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            raise CoordinateError(f"out-of-range WGS84 coordinates ({lon}, {lat})", repairable=True)
        count += 1
    return count


def validate_geojsonl(path):
    """Read-only validation for conversion without the input-cleaning pass."""
    path = Path(path)
    with path.open() as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                feature = json.loads(line)
                if not isinstance(feature, dict) or feature.get("type") != "Feature" or "geometry" not in feature:
                    raise CoordinateError("expected a GeoJSON Feature with a geometry member")
                validate_geometry_coordinates(feature["geometry"])
            except (ValueError, TypeError, RecursionError) as exc:
                raise CoordinateError(f"{path}, line {line_number}: {exc}") from exc


def validate_wkb_batches(batches, source, *, progress=None):
    """Validate every XY vertex in bounded batches, including hidden NaN vertices."""
    import numpy as np
    import shapely

    offset, last_progress = 0, time.monotonic()
    for batch in batches:
        values = batch.column(0).to_pylist()
        try:
            geometries = shapely.from_wkb(values)
        except (shapely.errors.GEOSException, ValueError, TypeError) as exc:
            for index, value in enumerate(values):
                try:
                    shapely.from_wkb(value)
                except (shapely.errors.GEOSException, ValueError, TypeError) as row_exc:
                    raise CoordinateError(f"{source}, row {offset + index + 1}: invalid WKB: {row_exc}") from row_exc
            raise CoordinateError(f"{source}: invalid WKB: {exc}") from exc
        coordinates, owners = shapely.get_coordinates(geometries, return_index=True)
        bad = (
            ~np.isfinite(coordinates).all(axis=1)
            | (coordinates[:, 0] < -180)
            | (coordinates[:, 0] > 180)
            | (coordinates[:, 1] < -90)
            | (coordinates[:, 1] > 90)
        )
        if bad.any():
            index = int(np.flatnonzero(bad)[0])
            row = offset + int(owners[index]) + 1
            raise CoordinateError(
                f"{source}, row {row}: non-finite or out-of-range WGS84 coordinates {coordinates[index].tolist()}"
            )
        offset += batch.num_rows
        if progress and time.monotonic() - last_progress >= 5:
            progress(f"Validated coordinates for {offset:,} rows from {source}")
            last_progress = time.monotonic()
    return offset


def validate_parquet_coordinates(path, *, progress=None):
    """Check generated Parquet before making it a publication asset."""
    import pyarrow.parquet as pq

    from iomaps.core.coverage import _geometry_column

    path = Path(path)
    with pq.ParquetFile(path) as parquet:
        column = _geometry_column(parquet, path)
        validate_wkb_batches(
            parquet.iter_batches(batch_size=1024, columns=[column], use_threads=False), str(path), progress=progress
        )
