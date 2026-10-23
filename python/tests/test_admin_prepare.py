import json

import duckdb
import pytest
from click.testing import CliRunner

from iomaps.commands import admin_prepare
from iomaps.commands.admin_prepare import analyze_unique_fields, filter_empty_geometries


def geojson_feature(geometry, identifier=1):
    return {"type": "Feature", "properties": {"id": identifier}, "geometry": geometry}


@pytest.mark.parametrize("check_india", [True, False])
def test_cleaning_preserves_valid_coordinates_outside_india(tmp_path, check_india):
    geometries = [
        {"type": "Point", "coordinates": [-74, 40.7]},
        {"type": "Point", "coordinates": [120, 40]},
        {"type": "Point", "coordinates": [0, 0]},
        {"type": "Point", "coordinates": [-180, -90]},
        {"type": "Point", "coordinates": [180, 90, 123]},
        {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [28, 77]}]},
    ]
    features = [geojson_feature(g, i) for i, g in enumerate(geometries)]
    source = tmp_path / "world.geojsonl"
    original = "".join(json.dumps(f) + "\n" for f in features)
    source.write_text(original)
    total, kept, cleaned = filter_empty_geometries(source, check_india=check_india)
    assert total == kept == len(features)
    assert [json.loads(s) for s in cleaned.read_text().splitlines()] == features
    assert source.read_text() == original


@pytest.mark.parametrize("check_india", [True, False])
def test_cleaning_drops_unusable_coordinates_with_line_diagnostics(tmp_path, capsys, check_india):
    good = geojson_feature({"type": "Point", "coordinates": [-74, 40.7]})
    bad = [
        {"type": "LineString", "coordinates": [[float("nan"), float("nan")], [float("nan"), float("nan")]]},
        {"type": "LineString", "coordinates": [[77, 28], [float("nan"), 29], [78, 28]]},
        {"type": "Point", "coordinates": [float("inf"), 28]},
        {"type": "Point", "coordinates": [-1.7976931348623157e308, -1.7976931348623157e308]},
        {"type": "Point", "coordinates": ["77", 28]},
        {"type": "Point", "coordinates": [True, 28]},
        {"type": "Point", "coordinates": [77]},
        {"type": "Point", "coordinates": [77, 28, float("nan")]},
        {"type": "Unknown", "coordinates": [77, 28]},
        {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [77, None]}]},
    ]
    features = [good, *[geojson_feature(g, i + 2) for i, g in enumerate(bad)]]
    source = tmp_path / "bad.geojsonl"
    original = "".join(json.dumps(f) + "\n" for f in features)
    source.write_text(original)
    total, kept, cleaned = filter_empty_geometries(source, check_india=check_india)
    assert total == len(features) and kept == 1
    assert json.loads(cleaned.read_text()) == good
    messages = capsys.readouterr().out
    for line in range(2, len(features) + 1):
        assert f"line {line}:" in messages
    assert source.read_text() == original


def test_no_india_keeps_global_range_validation(tmp_path, capsys):
    geometries = [
        {"type": "Point", "coordinates": [79.5624833, 119.4761833]},
        {"type": "Point", "coordinates": [74.76696440795382, 334339744615.0]},
        {"type": "Point", "coordinates": [-189120.34375, 185042.65625]},
        {"type": "Point", "coordinates": [120, 40]},
    ]
    source = tmp_path / "data.geojsonl"
    source.write_text("".join(json.dumps(geojson_feature(g, i)) + "\n" for i, g in enumerate(geometries)))
    total, kept, cleaned = filter_empty_geometries(source, check_india=False)
    assert (total, kept) == (4, 1)
    assert json.loads(cleaned.read_text())["geometry"] == geometries[-1]
    assert capsys.readouterr().out.count("out-of-range WGS84") == 3


def test_repair_keeps_valid_outside_india_members_unchanged(tmp_path):
    geometry = {
        "type": "GeometryCollection",
        "geometries": [
            {"type": "Point", "coordinates": [120, 40]},
            {"type": "Point", "coordinates": [7750, 28]},
        ],
    }
    source = tmp_path / "mixed.geojsonl"
    source.write_text(json.dumps(geojson_feature(geometry)) + "\n")
    _, kept, cleaned = filter_empty_geometries(source)
    assert kept == 1
    members = json.loads(cleaned.read_text())["geometry"]["geometries"]
    assert members[0] == geometry["geometries"][0]
    assert members[1]["coordinates"] == [77.5, 28]


def test_no_filter_rejects_bad_coordinates_before_conversion(tmp_path, monkeypatch):
    source = tmp_path / "bad.geojsonl"
    original = json.dumps(geojson_feature({"type": "Point", "coordinates": [79, 119]})) + "\n"
    source.write_text(original)
    monkeypatch.chdir(tmp_path)
    calls = []
    monkeypatch.setattr(admin_prepare, "run_cmd", lambda cmd, **kw: calls.append(cmd))
    result = CliRunner().invoke(
        admin_prepare.prepare, [str(source), "--no-zip", "--no-pmtiles", "--no-filter", "--no-india", "--no-coverage"]
    )
    assert result.exit_code != 0
    assert "line 1" in result.output and "out-of-range WGS84" in result.output
    assert not calls and not (tmp_path / "bad.parquet").exists()
    assert source.read_text() == original


def test_converter_output_validation_preserves_previous_parquet(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq
    import shapely

    source = tmp_path / "input.geojsonl"
    source.write_text(json.dumps(geojson_feature({"type": "Point", "coordinates": [120, 40]})) + "\n")
    output = tmp_path / "input.parquet"
    output.write_bytes(b"previous asset")
    monkeypatch.chdir(tmp_path)

    def convert(cmd, **kwargs):
        table = pa.table({"geometry": [shapely.to_wkb(shapely.Point(79, 119))]})
        table = table.replace_schema_metadata(
            {
                b"geo": json.dumps(
                    {"version": "1.1.0", "primary_column": "geometry", "columns": {"geometry": {"encoding": "WKB"}}}
                ).encode()
            }
        )
        pq.write_table(table, cmd[-1])

    monkeypatch.setattr(admin_prepare, "run_cmd", convert)
    result = CliRunner().invoke(
        admin_prepare.prepare, [str(source), "--no-zip", "--no-pmtiles", "--no-filter", "--no-coverage"]
    )
    assert result.exit_code != 0 and "GeoParquet coordinate validation failed" in result.output
    assert output.read_bytes() == b"previous asset"
    assert not list(tmp_path.glob(".input.parquet-*"))


def test_cleaning_preserves_nonempty_geometry_collections(tmp_path):
    point = {"type": "Point", "coordinates": [77.2, 28.6]}
    collection = {"type": "GeometryCollection", "geometries": [point]}
    nested = {"type": "GeometryCollection", "geometries": [collection]}
    empty = {"type": "GeometryCollection", "geometries": []}
    geometries = [collection, nested, empty, {"type": "GeometryCollection", "geometries": [empty]}, None]
    features = [
        {"type": "Feature", "properties": {"id": i}, "geometry": geometry} for i, geometry in enumerate(geometries)
    ]
    source = tmp_path / "plots.geojsonl"
    original = "".join(json.dumps(feature) + "\n" for feature in features)
    source.write_text(original)

    total, kept, cleaned = filter_empty_geometries(source)

    assert (total, kept) == (5, 2)
    assert [json.loads(line) for line in cleaned.read_text().splitlines()] == features[:2]
    assert source.read_text() == original


def test_id_analysis_queries_partitioned_parquet_with_quoted_paths(tmp_path):
    parts = [tmp_path / "plots'first.parquet", tmp_path / "plots'second.parquet"]
    con = duckdb.connect()
    for index, path in enumerate(parts):
        con.sql(
            "SELECT ? AS id, 'repeated' AS name, NULL::VARCHAR AS missing, 'wkb' AS geometry", params=[index]
        ).write_parquet(str(path))
    con.close()
    result = analyze_unique_fields(parts)
    assert result["total_features"] == 2
    assert result["fields"]["id"]["is_unique"]
    assert not result["fields"]["name"]["is_unique"]
    assert result["fields"]["missing"]["null_count"] == 2
    assert "geometry" not in result["fields"]


@pytest.mark.parametrize("reduction", [None, "drop-densest", "drop-smallest"])
def test_tile_preparation_passes_the_configured_temp_directory(tmp_path, monkeypatch, reduction):
    monkeypatch.chdir(tmp_path)
    source = tmp_path / "Plots.geojsonl"
    source.write_text("{}\n")
    calls = []
    monkeypatch.setattr(admin_prepare.tempfile, "gettempdir", lambda: "/external/tmp")

    def run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:2] == ["pmtiles", "convert"]:
            (tmp_path / "Plots.pmtiles").write_bytes(b"tiles")

    monkeypatch.setattr(admin_prepare, "run_cmd", run)
    options = []
    if reduction:
        options = [
            "--tile-reduction",
            reduction,
            "--tippecanoe-option=--extend-zooms-if-still-dropping-maximum=3",
        ]
    result = CliRunner().invoke(
        admin_prepare.prepare,
        [str(source), "https://example.com", "Source", "--no-zip", "--no-parquet", "--no-filter", *options],
    )
    assert result.exit_code == 0, result.output
    assert calls[0][:3] == ["tippecanoe", "--temporary-directory", "/external/tmp"]
    if reduction:
        assert f"--{reduction}-as-needed" in calls[0]
        assert "--coalesce-smallest-as-needed" not in calls[0]
        assert "--increase-gamma-as-needed" not in calls[0]
        assert "--extend-zooms-if-still-dropping-maximum=3" in calls[0]
    else:
        assert "--coalesce-smallest-as-needed" in calls[0]
        assert "--increase-gamma-as-needed" in calls[0]
