import json
import random
import string

import click
import duckdb
import pytest

from iomaps.commands.admin_parquet import bounded_parquet, source_identity


@pytest.mark.parametrize("split", [False, True])
def test_bounded_export_preserves_features_and_metadata(tmp_path, monkeypatch, split):
    import tempfile

    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    randomizer = random.Random(123)
    source = tmp_path / "Plots'input.geojsonl"
    features = [
        {
            "type": "Feature",
            "properties": {
                "OBJECTID": index,
                "label": "क्षेत्र",
                "optional": None if index % 2 else "yes",
                "data": "".join(randomizer.choices(string.ascii_letters, k=256)),
            },
            "geometry": {"type": "Point", "coordinates": [70 + index / 100, 25 + index / 1000]},
        }
        for index in range(128)
    ]
    source.write_text("".join(json.dumps(f) + "\n" for f in features))
    conn = duckdb.connect()
    try:
        conn.execute("LOAD spatial")
    except duckdb.Error:
        pytest.skip("DuckDB spatial extension must be installed to run the integration test")
    limit = 10000 if split else 2 * 1024**3
    paths = bounded_parquet(source, tmp_path / "Plots.parquet", memory_limit="64MB", max_bytes=limit)
    assert (len(paths) > 1) == split
    assert all(path.stat().st_size <= limit for path in paths)
    assert source.exists()
    assert not list(tmp_path.glob("iomaps-parquet-*"))
    read = conn.read_parquet([str(p) for p in paths])
    read.create_view("result")
    rows = conn.execute("SELECT OBJECTID, label, optional, data FROM result ORDER BY OBJECTID").fetchall()
    assert rows == [
        (i, f["properties"]["label"], f["properties"]["optional"], f["properties"]["data"])
        for i, f in enumerate(features)
    ]
    geometry_type = next(row[1] for row in conn.execute("DESCRIBE result").fetchall() if row[0] == "geometry")
    expression = "geometry" if geometry_type.startswith("GEOMETRY") else "ST_GeomFromWKB(geometry)"
    coords = conn.execute(f"SELECT ST_X({expression}), ST_Y({expression}) FROM result ORDER BY OBJECTID").fetchall()
    assert coords == [tuple(f["geometry"]["coordinates"]) for f in features]
    for path in paths:
        kv = dict(conn.execute("SELECT key, value FROM parquet_kv_metadata(?)", [str(path)]).fetchall())
        meta = json.loads(kv[b"geo"])
        assert meta["version"] == "1.1.0"
        geometry = meta["columns"]["geometry"]
        assert geometry["encoding"] == "WKB"
        assert geometry["geometry_types"] == ["Point"]
        assert geometry["covering"]["bbox"]["xmin"] == ["bbox", "xmin"]
        bounds = conn.execute(
            "SELECT min(bbox.xmin), min(bbox.ymin), max(bbox.xmax), max(bbox.ymax) FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
        assert tuple(geometry["bbox"]) == bounds
    conn.close()


def test_resume_uses_import_checkpoint_and_rejects_changed_source(tmp_path):
    source = tmp_path / "Plots.geojsonl"
    # A resume must use the database; this deliberately cannot be parsed as GeoJSON.
    source.write_text("already imported source")
    db = tmp_path / "features.duckdb"
    con = duckdb.connect(str(db))
    try:
        con.execute("LOAD spatial")
    except duckdb.Error:
        pytest.skip("DuckDB spatial extension must be installed")
    con.execute("CREATE TABLE features AS SELECT 42 AS OBJECTID, ST_Point(75,25) AS geom")
    con.execute("CREATE TABLE iomaps_import(source_json VARCHAR, feature_count UBIGINT)")
    con.execute("INSERT INTO iomaps_import VALUES (?,1)", [json.dumps(source_identity(source))])
    con.close()
    paths = bounded_parquet(source, tmp_path / "Plots.parquet", resume_db=db, memory_limit="64MB")
    assert db.exists()
    con = duckdb.connect()
    assert con.execute("SELECT OBJECTID FROM read_parquet(?)", [str(paths[0])]).fetchall() == [(42,)]
    con.close()
    source.write_text("changed source")
    with pytest.raises(click.ClickException, match="different or changed source"):
        bounded_parquet(source, tmp_path / "Other.parquet", resume_db=db)
    assert db.exists()


def test_bounded_export_preserves_mixed_polygons_and_refuses_overwrite(tmp_path):
    conn = duckdb.connect()
    try:
        conn.execute("LOAD spatial")
    except duckdb.Error:
        pytest.skip("DuckDB spatial extension must be installed")
    ring = [[75, 25], [76, 25], [76, 26], [75, 25]]
    geometries = [
        {"type": "Polygon", "coordinates": [ring]},
        {"type": "MultiPolygon", "coordinates": [[ring]]},
    ]
    source = tmp_path / "Parcels.geojsonl"
    source.write_text(
        "".join(
            json.dumps({"type": "Feature", "properties": {"id": i}, "geometry": g}) + "\n"
            for i, g in enumerate(geometries)
        )
    )
    output = tmp_path / "Parcels.parquet"
    bounded_parquet(source, output, memory_limit="64MB")
    conn.execute("SET enable_geoparquet_conversion=false")
    actual = conn.execute(
        "SELECT id, ST_AsGeoJSON(ST_GeomFromWKB(geometry)) FROM read_parquet(?) ORDER BY id", [str(output)]
    ).fetchall()
    assert [json.loads(g) for _, g in actual] == geometries
    kv = dict(conn.execute("SELECT key, value FROM parquet_kv_metadata(?)", [str(output)]).fetchall())
    assert json.loads(kv[b"geo"])["columns"]["geometry"]["geometry_types"] == ["MultiPolygon", "Polygon"]
    with pytest.raises(click.ClickException, match="already exists"):
        bounded_parquet(source, output)
    conn.close()


@pytest.mark.parametrize("resume", [False, True])
def test_bounded_rejects_bad_coordinates_before_export(tmp_path, resume):
    conn = duckdb.connect(str(tmp_path / "input.duckdb") if resume else ":memory:")
    try:
        conn.execute("LOAD spatial")
    except duckdb.Error:
        conn.close()
        pytest.skip("DuckDB spatial extension must be installed")
    source = tmp_path / "bad.geojsonl"
    source.write_text(
        json.dumps(
            {
                "type": "Feature",
                "properties": {},
                "geometry": {"type": "LineString", "coordinates": [[77, 28], [79, 119]]},
            }
        )
        + "\n"
    )
    options = {}
    if resume:
        # A NaN interior vertex can be hidden by an otherwise finite bbox.
        import shapely

        bad = shapely.to_wkb(shapely.LineString([(77, 28), (float("nan"), 29), (78, 28)]))
        conn.execute("CREATE TABLE features AS SELECT ST_GeomFromWKB(?) AS geom", [bad])
        conn.execute("CREATE TABLE iomaps_import(source_json VARCHAR, feature_count UBIGINT)")
        conn.execute("INSERT INTO iomaps_import VALUES (?,1)", [json.dumps(source_identity(source))])
        options["resume_db"] = tmp_path / "input.duckdb"
    conn.close()
    with pytest.raises(click.ClickException, match="non-finite or out-of-range WGS84"):
        bounded_parquet(source, tmp_path / "bad.parquet", memory_limit="64MB", **options)
    assert source.exists() and not list(tmp_path.glob("*.parquet"))
    if resume:
        assert options["resume_db"].exists()


def test_bounded_preserves_worldwide_coordinates(tmp_path):
    conn = duckdb.connect()
    try:
        conn.execute("LOAD spatial")
    except duckdb.Error:
        conn.close()
        pytest.skip("DuckDB spatial extension must be installed")
    coordinates = [[-74, 40.7], [120, 40], [0, 0], [180, 90], [-180, -90]]
    source = tmp_path / "world.geojsonl"
    source.write_text(
        "".join(
            json.dumps({"type": "Feature", "properties": {"id": i}, "geometry": {"type": "Point", "coordinates": p}})
            + "\n"
            for i, p in enumerate(coordinates)
        )
    )
    paths = bounded_parquet(source, tmp_path / "world.parquet", memory_limit="64MB")
    conn.execute("SET enable_geoparquet_conversion=false")
    rows = conn.execute(
        "SELECT ST_X(ST_GeomFromWKB(geometry)), ST_Y(ST_GeomFromWKB(geometry)) FROM read_parquet(?) ORDER BY id",
        [str(paths[0])],
    ).fetchall()
    assert rows == [tuple(p) for p in coordinates]
    conn.close()
