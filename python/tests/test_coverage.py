import json
import struct
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from click.testing import CliRunner
from PIL import Image
from shapely import GeometryCollection, LineString, MultiPoint, MultiPolygon, Point, Polygon, box

from iomaps.commands.admin import admin
from iomaps.commands.admin_coverage import coverage as coverage_command
from iomaps.core.coverage import INDIA_BOUNDS, Coverage, CoverageError, CoverageGrid, create_coverage


def write_parquet(path, geometries, *, column="geometry", column_metadata=None, geo_metadata=True):
    values = [g if isinstance(g, bytes) else shapely.to_wkb(g) for g in geometries]
    table = pa.table({"id": range(len(values)), column: pa.array(values, type=pa.binary())})
    if geo_metadata:
        metadata = {
            "version": "1.1.0",
            "primary_column": column,
            "columns": {column: {"encoding": "WKB", "geometry_types": [], **(column_metadata or {})}},
        }
        table = table.replace_schema_metadata({b"geo": json.dumps(metadata).encode()})
    pq.write_table(table, path, row_group_size=2)
    return path


def exact_cells(grid, geometries):
    """Independent brute-force oracle; only used for small test grids."""
    west, south, east, north = grid.bounds
    xs = np.linspace(west, east, grid.width + 1)
    ys = np.linspace(south, north, grid.height + 1)
    cells = [box(xs[c], ys[r], xs[c + 1], ys[r + 1]) for r in range(grid.height) for c in range(grid.width)]
    expected = np.zeros(len(cells), dtype=bool)
    for geometry in geometries:
        expected |= shapely.intersects(geometry, cells)
    return expected.reshape(grid.height, grid.width)


@pytest.mark.parametrize(
    ("geometry", "expected"),
    [
        (Point(1, 1), {(0, 0), (0, 1), (1, 0), (1, 1)}),
        (Point(0, 0), {(0, 0)}),
        (Point(4, 4), {(3, 3)}),
        (Point(-1, 2), set()),
        (LineString([(0, 2), (4, 2)]), {(r, c) for r in (1, 2) for c in range(4)}),
    ],
)
def test_shared_edges_corners_and_outer_limits(geometry, expected):
    result = Coverage(CoverageGrid((0, 0, 4, 4), 4, 4))
    result.add([geometry])
    assert set(zip(*np.nonzero(result.cells), strict=True)) == expected


def test_polygons_fill_interiors_and_preserve_holes():
    polygon = Polygon([(0, 0), (8, 0), (8, 8), (0, 8)], holes=[[(2, 2), (6, 2), (6, 6), (2, 6)]])
    result = Coverage(CoverageGrid((0, 0, 8, 8), 8, 8))
    result.add([polygon])
    expected = np.ones((8, 8), dtype=bool)
    expected[3:5, 3:5] = False
    np.testing.assert_array_equal(result.cells, expected)
    result.add([Point(3.5, 3.5)])
    assert result.cells[3, 3]
    assert not result.cells[4, 4]


def test_large_features_and_batches_match_exact_intersections():
    grid = CoverageGrid((0, 0, 8, 8), 64, 64)
    geometries = [
        LineString([(-1, -1), (9, 9)]),
        LineString([(0, 3.01), (8, 3.02)]),
        Polygon([(1, 1), (7, 1), (1, 7)]),
        MultiPolygon([box(0.1, 6.2, 0.2, 7.8), box(6.1, 0.1, 6.2, 0.2)]),
        GeometryCollection([MultiPoint([(4, 6), (4.01, 6.01)]), box(7.1, 4.1, 7.2, 4.2)]),
        # The bbox crosses the grid, but neither point intersects it.
        MultiPoint([(-1, 3), (9, 3)]),
        None,
        GeometryCollection(),
    ]
    result = Coverage(grid)
    for geometry in geometries:
        result.add([geometry])
    np.testing.assert_array_equal(result.cells, exact_cells(grid, geometries))
    assert result.counts == {
        "rows": 8,
        "null_geometries": 1,
        "empty_geometries": 1,
        "invalid_geometries": 0,
        "skipped_geometries": 0,
        "intersecting_geometries": 5,
        "outside_geometries": 1,
        "partly_outside_geometries": 1,
    }
    before = result.cells.copy()
    result.add(geometries[:5])
    np.testing.assert_array_equal(result.cells, before)


def test_float_grid_boundaries_and_random_geometry_match_oracle():
    grid = CoverageGrid(INDIA_BOUNDS, 17, 17)
    rng = np.random.default_rng(61)
    west, south, east, north = grid.bounds
    points = rng.uniform([west - 1, south - 1], [east + 1, north + 1], size=(50, 2))
    geometries = [Point(p) for p in points]
    geometries += [LineString(points[i : i + 2]) for i in range(0, len(points), 2)]
    geometries += [box(x, y, x + 0.7, y + 0.5) for x, y in points]
    # Exactly the edges that define the grid, including non-binary decimal steps.
    geometries += [Point(x, y) for x, y in zip(np.linspace(west, east, 18), np.linspace(south, north, 18), strict=True)]
    result = Coverage(grid)
    result.add(geometries)
    np.testing.assert_array_equal(result.cells, exact_cells(grid, geometries))


def test_features_outside_india_do_not_mark_edge_cells(tmp_path):
    geometries = [
        Point(66.999, 20),
        Point(98.001, 20),
        box(76, 3, 78, 4.999),
        LineString([(76, 38.001), (78, 40)]),
        # An overlapping feature bbox alone must not contribute coverage.
        MultiPoint([(66, 20), (99, 20)]),
    ]
    source = write_parquet(tmp_path / "outside.parquet", geometries)
    output = tmp_path / "outside.png"
    result = create_coverage([source], output, grid_size=20, batch_size=2)
    assert not result.cells.any()
    assert result.counts["outside_geometries"] == len(geometries)
    with Image.open(output) as image:
        assert not np.asarray(image).any()


def test_features_crossing_india_bounds_contribute_only_the_overlap(tmp_path):
    geometries = [
        box(65, 15, 68, 16),
        LineString([(97, 28), (100, 28)]),
        box(77, 3, 78, 6),
        LineString([(80, 37), (80, 40)]),
    ]
    grid = CoverageGrid(width=20, height=20)
    source = write_parquet(tmp_path / "crossing.parquet", geometries)
    result = create_coverage([source], tmp_path / "crossing.png", grid_size=20)
    clipped = shapely.intersection(geometries, box(*INDIA_BOUNDS))
    np.testing.assert_array_equal(result.cells, exact_cells(grid, clipped))
    assert result.counts["partly_outside_geometries"] == len(geometries)


def test_png_is_one_bit_north_up_and_contains_only_occupancy(tmp_path):
    source = write_parquet(tmp_path / "points.parquet", [Point(67.1, 37.9), Point(97.9, 5.1), None, Point()])
    output = tmp_path / "points.coverage.png"
    result = create_coverage([source], output, grid_size=9, batch_size=1)
    with Image.open(output) as image:
        assert image.mode == "1"
        assert image.size == (9, 9)
        assert image.info == {}
        assert image.getpixel((0, 0)) == 255
        assert image.getpixel((8, 8)) == 255
        assert image.getpixel((0, 8)) == 0
        np.testing.assert_array_equal(np.asarray(image), result.cells[::-1])
    data = output.read_bytes()
    assert data[:8] == b"\x89PNG\r\n\x1a\n"
    assert data[24:26] == b"\x01\x00"  # one bit, grayscale
    position, chunks = 8, []
    while position < len(data):
        length = struct.unpack_from(">I", data, position)[0]
        chunks.append(data[position + 4 : position + 8])
        position += 12 + length
    assert set(chunks) == {b"IHDR", b"IDAT", b"IEND"}
    assert sorted(p.name for p in tmp_path.iterdir()) == [output.name, source.name]
    assert result.counts["rows"] == 4


def test_all_partitions_and_named_geometry_columns_are_combined(tmp_path):
    first = write_parquet(tmp_path / "data.a.parquet", [Point(67.1, 37.9)], column="parcel shape")
    second = write_parquet(tmp_path / "data.b.parquet", [Point(97.9, 5.1)], column="geom")
    metadata = tmp_path / "data.parquet.meta.json"
    metadata.write_text(json.dumps({"extents": {first.name: {}, second.name: {}}}))
    output = tmp_path / "data.coverage.png"
    result = create_coverage([metadata], output, grid_size=4)
    assert result.cells.sum() == 2
    assert result.counts["rows"] == 2
    second.unlink()
    previous = output.read_bytes()
    with pytest.raises(CoverageError, match="Missing"):
        create_coverage([metadata], output, overwrite=True)
    assert output.read_bytes() == previous


@pytest.mark.parametrize(
    ("column_metadata", "geo_metadata", "message"),
    [
        ({"crs": None}, True, "unknown"),
        ({"crs": "EPSG:3857"}, True, "reproject"),
        ({"crs": "not-a-crs"}, True, "metadata"),
        ({"edges": "spherical"}, True, "spherical"),
        ({"encoding": "point"}, True, "WKB"),
        ({}, False, "metadata"),
    ],
)
def test_unsupported_geoparquet_does_not_write_output(tmp_path, column_metadata, geo_metadata, message):
    source = write_parquet(
        tmp_path / "data.parquet", [Point(77, 28)], column_metadata=column_metadata, geo_metadata=geo_metadata
    )
    output = tmp_path / "coverage.png"
    with pytest.raises(CoverageError, match=message):
        create_coverage([source], output)
    assert not output.exists()


@pytest.mark.parametrize("crs", ["EPSG:4326", "OGC:CRS84"])
def test_explicit_wgs84_crs_keeps_longitude_first(tmp_path, crs):
    from pyproj import CRS

    source = write_parquet(
        tmp_path / "data.parquet", [Point(77, 28)], column_metadata={"crs": CRS.from_string(crs).to_json_dict()}
    )
    result = create_coverage([source], tmp_path / "coverage.png", grid_size=4)
    assert result.cells.sum() == 1
    assert result.cells[2, 1]


@pytest.mark.parametrize(
    ("bad_geometry", "message"),
    [
        (b"bad wkb", "invalid WKB"),
        (LineString([(77, 28), (float("nan"), 29), (78, 28)]), "non-finite"),
        (Point(400, 28), "out-of-range"),
    ],
)
@pytest.mark.parametrize("batch_size", [1, 8192])
def test_bad_geometry_warns_and_keeps_other_features(tmp_path, bad_geometry, message, batch_size):
    good = [Point(77, 28), Point(90, 20)]
    source = write_parquet(tmp_path / "data.parquet", [good[0], bad_geometry, None, good[1]])
    original_source = source.read_bytes()
    output = tmp_path / "coverage.png"
    output.write_bytes(b"old coverage")
    messages = []
    result = create_coverage(
        [source], output, batch_size=batch_size, grid_size=10, overwrite=True, progress=messages.append
    )
    np.testing.assert_array_equal(result.cells, exact_cells(result.grid, good))
    assert result.counts["rows"] == 4
    assert result.counts["skipped_geometries"] == 1
    assert result.counts["null_geometries"] == 1
    assert any("Warning:" in msg and "row 2:" in msg and message in msg for msg in messages)
    assert source.read_bytes() == original_source
    with Image.open(output) as image:
        assert image.mode == "1" and image.size == (10, 10)
    assert not list(tmp_path.glob(".*.tmp"))


def test_geos_predicate_error_skips_only_the_failed_feature(monkeypatch):
    good = [Point(1.5, 1.5), Point(3.5, 3.5)]
    bad = LineString([(0.2, 0.2), (2.8, 2.8)])
    original = shapely.intersects

    def intersects(geometry, other):
        objects = np.atleast_1d(geometry)
        if any(g is bad for g in objects):
            raise shapely.errors.GEOSException("failed predicate")
        return original(geometry, other)

    monkeypatch.setattr(shapely, "intersects", intersects)
    messages = []
    result = Coverage(CoverageGrid((0, 0, 4, 4), 4, 4), warning=messages.append)
    result.add([good[0], bad, good[1]], source="part.parquet", row_offset=20)
    np.testing.assert_array_equal(result.cells, exact_cells(result.grid, good))
    assert result.counts["skipped_geometries"] == 1
    assert result.counts["intersecting_geometries"] == 2
    assert any("part.parquet" in msg and "row 22:" in msg for msg in messages)


def test_geos_burn_error_rolls_back_only_the_failed_feature(monkeypatch):
    good = [Point(1.5, 1.5), Point(3.5, 3.5)]
    bad = box(0.2, 0.2, 2.8, 2.8)
    original = Coverage._burn

    def burn(self, geometry, c0, r0, c1, r1):
        if geometry is bad:
            self.cells[r0:r1, c0:c1] = True
            raise shapely.errors.GEOSException("failed after marking cells")
        return original(self, geometry, c0, r0, c1, r1)

    monkeypatch.setattr(Coverage, "_burn", burn)
    messages = []
    result = Coverage(CoverageGrid((0, 0, 4, 4), 4, 4), warning=messages.append)
    result.add([good[0], bad, good[1]])
    np.testing.assert_array_equal(result.cells, exact_cells(result.grid, good))
    assert result.counts["skipped_geometries"] == 1
    assert result.counts["intersecting_geometries"] == 2
    assert any("row 2:" in msg and "failed after marking cells" in msg for msg in messages)


def test_invalid_features_use_their_bboxes_with_a_warning(tmp_path):
    invalid = Polygon([(66, 20), (69, 23), (66, 23), (69, 20), (66, 20)])
    outside = Polygon([(100, 20), (103, 23), (100, 23), (103, 20), (100, 20)])
    source = write_parquet(tmp_path / "invalid.parquet", [Point(77, 28), invalid, outside])
    original = source.read_bytes()
    messages = []
    result = create_coverage([source], tmp_path / "coverage.png", grid_size=20, progress=messages.append)
    expected = exact_cells(result.grid, [Point(77, 28), box(*invalid.bounds)])
    np.testing.assert_array_equal(result.cells, expected)
    assert result.counts["invalid_geometries"] == 2
    assert result.counts["outside_geometries"] == 1
    assert any("Warning:" in msg and "row 2:" in msg and "bboxes" in msg for msg in messages)
    assert source.read_bytes() == original


@pytest.mark.parametrize(
    "invalid",
    [Polygon([(1, 1), (2, 1), (3, 1), (1, 1)]), Polygon([(1, 1), (1, 1), (1, 1)])],
)
def test_invalid_degenerate_shapes_and_input_arrays_are_preserved(invalid):
    geometries = np.array([invalid], dtype=object)
    result = Coverage(CoverageGrid((0, 0, 4, 4), 4, 4), warning=lambda msg: None)
    result.add(geometries)
    expected_geometry = LineString([(1, 1), (3, 1)]) if invalid.bounds[0] != invalid.bounds[2] else Point(1, 1)
    np.testing.assert_array_equal(result.cells, exact_cells(result.grid, [expected_geometry]))
    assert geometries[0] is invalid
    assert not invalid.is_valid


def test_creation_overwrite_and_admin_command(tmp_path):
    source = write_parquet(tmp_path / "data.parquet", [Point(77, 28)])
    output = tmp_path / "coverage.png"
    args = [str(source), "-o", str(output), "--grid-size", "4", "--batch-size", "1"]
    result = CliRunner().invoke(admin, ["coverage", *args])
    assert result.exit_code == 0, result.output
    previous = output.read_bytes()
    result = CliRunner().invoke(coverage_command, args)
    assert result.exit_code != 0
    assert "already exists" in result.output
    assert output.read_bytes() == previous
    write_parquet(source, [Point(97.9, 5.1)])
    result = CliRunner().invoke(coverage_command, [*args, "--overwrite"])
    assert result.exit_code == 0, result.output
    with Image.open(output) as image:
        assert image.getpixel((3, 3)) == 255
        assert image.getpixel((1, 1)) == 0
    assert not list(tmp_path.glob(".*.tmp"))


def test_empty_input_writes_empty_image_and_invalid_grid_is_rejected(tmp_path):
    source = write_parquet(tmp_path / "empty.parquet", [])
    result = create_coverage([source], tmp_path / "empty.png", grid_size=1)
    assert result.counts["rows"] == 0
    assert not result.cells.any()
    for options in [{"grid_size": 0}, {"grid_size": 10001}, {"batch_size": 0}]:
        with pytest.raises(CoverageError):
            create_coverage([source], tmp_path / "bad.png", **options)


def test_png_write_failure_keeps_previous_output_and_cleans_temporary_file(tmp_path, monkeypatch):
    source = write_parquet(tmp_path / "data.parquet", [Point(77, 28)])
    output = tmp_path / "coverage.png"
    output.write_bytes(b"previous")

    def fail(self, stream, **kwargs):
        stream.write(b"incomplete PNG")
        raise OSError("disk full")

    monkeypatch.setattr(Image.Image, "save", fail)
    with pytest.raises(OSError, match="disk full"):
        create_coverage([source], output, overwrite=True)
    assert output.read_bytes() == b"previous"
    assert not list(tmp_path.glob(".*.tmp"))


@pytest.mark.parametrize("mode", ["regular", "split", "bounded"])
def test_prepare_builds_coverage_from_final_partitions(tmp_path, monkeypatch, mode):
    from iomaps.commands import admin_parquet, admin_prepare

    monkeypatch.chdir(tmp_path)
    source = tmp_path / "Plots.geojsonl"
    original = (
        json.dumps({"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [77, 28]}}) + "\n"
    )
    source.write_text(original)
    seen_parts = []
    monkeypatch.setattr(admin_prepare, "create_field_analysis", lambda *args, **kwargs: None)
    monkeypatch.setattr(admin_prepare, "create_parquet_meta", lambda name, files: seen_parts.extend(files))

    def bounded(input_path, output_path, **kwargs):
        return [
            write_parquet(tmp_path / "Plots.part1.parquet", [Point(67.1, 37.9)]),
            write_parquet(tmp_path / "Plots.part2.parquet", [Point(97.9, 5.1)]),
        ]

    def run(cmd, **kwargs):
        if "convert" in cmd:
            path = Path(cmd[-1])
            if mode == "split":
                # Valid intermediate Parquet with enough metadata to force the
                # split path while keeping the feature count unchanged.
                write_parquet(path, [Point(67.1, 37.9), Point(97.9, 5.1)])
                table = pq.read_table(path)
                table = table.replace_schema_metadata({**table.schema.metadata, b"test_padding": b"x" * 6000})
                pq.write_table(table, path)
            else:
                write_parquet(path, [Point(67.1, 37.9), Point(97.9, 5.1)])
        elif "partition" in cmd:
            for index, point in enumerate([Point(67.1, 37.9), Point(97.9, 5.1)]):
                write_parquet(tmp_path / "split_parquet" / f"part{index}.parquet", [point])
        else:
            raise AssertionError(cmd)

    monkeypatch.setattr(admin_parquet, "bounded_parquet", bounded)
    monkeypatch.setattr(admin_prepare, "run_cmd", run)
    if mode == "split":
        monkeypatch.setattr(admin_prepare, "TWO_GB", 5000)
    extra = ["--bounded-parquet"] if mode == "bounded" else []
    result = CliRunner().invoke(
        admin_prepare.prepare,
        [str(source), "--no-zip", "--no-pmtiles", "--no-filter", "--coverage-grid-size", "4", *extra],
    )
    assert result.exit_code == 0, result.output
    with Image.open(tmp_path / "Plots.coverage.png") as image:
        assert image.mode == "1"
        assert image.size == (4, 4)
        assert image.getpixel((0, 0)) == image.getpixel((3, 3)) == 255
        assert np.asarray(image).sum() == 2
    assert source.read_text() == original
    if mode != "regular":
        assert len(seen_parts) == 2


def test_prepare_rejects_coverage_without_parquet_before_processing(tmp_path):
    from iomaps.commands.admin_prepare import prepare

    source = tmp_path / "input.geojsonl"
    source.write_text("unchanged")
    result = CliRunner().invoke(prepare, [str(source), "--coverage", "--no-parquet"])
    assert result.exit_code != 0
    assert "requires Parquet" in result.output
    assert list(tmp_path.iterdir()) == [source]


def test_prepare_can_disable_coverage(tmp_path, monkeypatch):
    from iomaps.commands import admin_prepare

    monkeypatch.chdir(tmp_path)
    source = tmp_path / "Plots.geojsonl"
    source.write_text(
        json.dumps({"type": "Feature", "properties": {}, "geometry": {"type": "Point", "coordinates": [77, 28]}}) + "\n"
    )
    monkeypatch.setattr(admin_prepare, "create_field_analysis", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        admin_prepare, "run_cmd", lambda cmd, **kwargs: write_parquet(Path(cmd[-1]), [Point(77.2, 28.6)])
    )
    result = CliRunner().invoke(
        admin_prepare.prepare, [str(source), "--no-zip", "--no-pmtiles", "--no-filter", "--no-coverage"]
    )
    assert result.exit_code == 0, result.output
    assert (tmp_path / "Plots.parquet").exists()
    assert not (tmp_path / "Plots.coverage.png").exists()
