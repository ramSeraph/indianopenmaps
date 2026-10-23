import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from shapely import from_wkb
from shapely.geometry import Point, Polygon, box

from iomaps.commands import extract as extract_module
from iomaps.commands.extract import build_source_query, get_covering_bbox, get_partitions_for_filter


@pytest.fixture
def con():
    connection = extract_module.get_duckdb_connection()
    yield connection
    connection.close()


def write_parquet(path, points, covering=True, column_name="bbox"):
    data = {
        "id": [p[0] for p in points],
        "geometry": [Point(x, y).wkb for _, x, y in points],
    }
    metadata = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    if covering:
        data[column_name] = [{"xmin": x, "ymin": y, "xmax": x, "ymax": y} for _, x, y in points]
        metadata["columns"]["geometry"]["covering"] = {
            "bbox": {axis: [column_name, axis] for axis in ("xmin", "ymin", "xmax", "ymax")}
        }
    table = pa.table(data).replace_schema_metadata({b"geo": json.dumps(metadata).encode()})
    pq.write_table(table, path, row_group_size=2)
    return str(path)


def selected_ids(con, urls, polygon, coverings=None):
    query = build_source_query(urls, polygon, coverings)
    return sorted(row[0] for row in con.execute(f"SELECT id FROM ({query})").fetchall())


def test_multiple_partitions_and_covering_pushdown(con, tmp_path):
    urls = [
        write_parquet(tmp_path / "a.parquet", [(1, 5, 5), (2, 500, 500)]),
        write_parquet(tmp_path / "b.parquet", [(3, 10, 10), (4, 1000, 1000)]),
    ]
    coverings = {url: get_covering_bbox(con, url) for url in urls}
    polygon = box(0, 0, 10, 10)
    assert selected_ids(con, urls, polygon, coverings) == selected_ids(con, urls, polygon) == [1, 3]
    plan = json.loads(
        con.execute("EXPLAIN (FORMAT JSON) " + build_source_query(urls, polygon, coverings)).fetchone()[1]
    )
    # The old ST_Intersects-only plan had no filters inside READ_PARQUET.
    scans = []

    def visit(node):
        if node["name"].strip() == "READ_PARQUET":
            scans.append(node["extra_info"])
        for child in node.get("children", []):
            visit(child)

    for node in plan:
        visit(node)
    assert len(scans) == 2
    assert all("xmin" in str(scan.get("Filters", "")) for scan in scans)


def test_exact_geometry_filter_retained(con, tmp_path):
    url = write_parquet(tmp_path / "a.parquet", [(1, 1, 1), (2, 5, 5), (3, 50, 50)])
    polygon = Polygon(box(0, 0, 10, 10).exterior.coords, [box(3, 3, 7, 7).exterior.coords])
    assert selected_ids(con, [url], polygon, {url: get_covering_bbox(con, url)}) == [1]


def test_partition_without_covering_and_quoted_paths(con, tmp_path):
    urls = [
        write_parquet(tmp_path / "quote's.parquet", [(1, 5, 5)], column_name='odd"bbox'),
        write_parquet(tmp_path / "no-covering.parquet", [(2, 6, 6)], covering=False),
    ]
    coverings = {url: get_covering_bbox(con, url) for url in urls}
    assert coverings[urls[1]] is None
    assert selected_ids(con, urls, box(0, 0, 10, 10), coverings) == [1, 2]
    assert selected_ids(con, urls, None, coverings) == [1, 2]


def test_covering_null_does_not_drop_valid_geometry(con, tmp_path):
    url = write_parquet(tmp_path / "null.parquet", [(1, 5, 5), (2, 6, 6)])
    table = pq.read_table(url)
    table = table.set_column(
        table.schema.get_field_index("bbox"),
        "bbox",
        pa.array([None, {"xmin": 6.0, "ymin": 6.0, "xmax": 6.0, "ymax": 6.0}]),
    )
    pq.write_table(table, url)
    assert selected_ids(con, [url], box(0, 0, 10, 10), {url: get_covering_bbox(con, url)}) == [1, 2]


def test_partition_selection_includes_boundary():
    metadata = {
        "extents": {
            "a.parquet": {"minx": 0, "miny": 0, "maxx": 10, "maxy": 10},
            "b.parquet": {"minx": 10, "miny": 0, "maxx": 20, "maxy": 10},
            "c.parquet": {"minx": 30, "miny": 0, "maxx": 40, "maxy": 10},
        }
    }
    assert get_partitions_for_filter("https://example.org/data.meta.json", box(5, 5, 10, 8), metadata) == [
        "https://example.org/a.parquet",
        "https://example.org/b.parquet",
    ]


def test_empty_source_rejected():
    with pytest.raises(ValueError, match="At least one"):
        build_source_query([])


@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_parquet_output_preserves_geometry_and_metadata(con, tmp_path, version):
    url = write_parquet(tmp_path / "source.parquet", [(1, 5, 6), (2, 8, 9), (3, 50, 60)])
    query = build_source_query([url], box(0, 0, 10, 10), {url: get_covering_bbox(con, url)})
    output = tmp_path / "out.parquet"
    extract_module.write_parquet_output(con, query, str(output), geoparquet_version=version)
    table = pq.read_table(output)
    rows = sorted(table.to_pylist(), key=lambda row: row["id"])
    assert [row["id"] for row in rows] == [1, 2]
    assert [list(from_wkb(row["geometry"]).coords)[0] for row in rows] == [(5, 6), (8, 9)]
    metadata = json.loads(table.schema.metadata[b"geo"])
    assert metadata["version"].startswith(version)
    assert metadata["primary_column"] == "geometry"
    assert metadata["columns"]["geometry"]["bbox"] == [5, 6, 8, 9]


@pytest.mark.parametrize("http_client", ["default", "curl", "httplib"])
def test_cli_writes_matches_from_both_partitions(con, tmp_path, monkeypatch, http_client):
    write_parquet(tmp_path / "a.parquet", [(1, 5, 6), (2, 50, 50)])
    write_parquet(tmp_path / "b.parquet", [(3, 8, 9), (4, 80, 80)])
    config = {"name": "Test", "url": str(tmp_path / "source.pmtiles"), "partitioned_parquet": True}
    metadata = {
        "extents": {
            "a.parquet": {"minx": 5, "miny": 5, "maxx": 50, "maxy": 50},
            "b.parquet": {"minx": 8, "miny": 8, "maxx": 80, "maxy": 80},
        }
    }
    monkeypatch.setattr(extract_module, "get_vector_sources", lambda **kwargs: [config])
    monkeypatch.setattr(extract_module, "resolve_source", lambda *args: config)
    monkeypatch.setattr(extract_module, "fetch_partition_metadata", lambda url: metadata)
    monkeypatch.setattr(extract_module, "get_duckdb_connection", lambda: con)
    original_covering = extract_module.get_covering_bbox

    def checked_covering(connection, url):
        if http_client != "default":
            assert (
                connection.execute("SELECT current_setting('httpfs_client_implementation')").fetchone()[0]
                == http_client
            )
        if http_client == "curl":
            assert (
                connection.execute("SELECT current_setting('enable_curl_server_cert_verification')").fetchone()[0]
                is True
            )
        return original_covering(connection, url)

    monkeypatch.setattr(extract_module, "get_covering_bbox", checked_covering)
    output = tmp_path / "out.geojsonl"
    result = CliRunner().invoke(
        extract_module.extract, ["-s", "Test", "-b", "0,0,10,10", "-o", str(output), "--http-client", http_client]
    )
    assert result.exit_code == 0, result.output
    rows = [json.loads(line.lstrip("\x1e")) for line in output.read_text().splitlines() if line.strip()]
    assert sorted(row["properties"]["id"] for row in rows) == [1, 3]
    assert sorted(row["geometry"]["coordinates"] for row in rows) == [[5, 6], [8, 9]]
    boxes = [row["properties"]["bbox"] for row in rows]
    boxes = [json.loads(bbox) if isinstance(bbox, str) else bbox for bbox in boxes]
    assert sorted(bbox["xmin"] for bbox in boxes) == [5, 8]
