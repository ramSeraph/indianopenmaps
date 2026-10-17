import json

import duckdb
from click.testing import CliRunner

from iomaps.commands import admin_prepare
from iomaps.commands.admin_prepare import analyze_unique_fields, filter_empty_geometries


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


def test_tile_preparation_passes_the_configured_temp_directory(tmp_path, monkeypatch):
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
    result = CliRunner().invoke(
        admin_prepare.prepare,
        [str(source), "https://example.com", "Source", "--no-zip", "--no-parquet", "--no-filter"],
    )
    assert result.exit_code == 0, result.output
    assert calls[0][:3] == ["tippecanoe", "--temporary-directory", "/external/tmp"]
