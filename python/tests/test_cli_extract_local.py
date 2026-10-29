import json

import pytest
from click.testing import CliRunner
from shapely.geometry import box
from test_cli_extract import write_parquet

from iomaps.commands import extract as command
from iomaps.core.local_parquet import local_parquet_paths


def test_local_partition_cli_does_not_fetch_catalog(tmp_path, monkeypatch):
    write_parquet(tmp_path / "a.parquet", [(1, 5, 5), (2, 50, 50)])
    write_parquet(tmp_path / "b.parquet", [(3, 6, 6)])
    manifest = tmp_path / "data.parquet.meta.json"
    manifest.write_text(json.dumps({"extents": {"a.parquet": {}, "b.parquet": {}}}))

    def no_network(*args, **kwargs):
        pytest.fail("Local extraction attempted remote catalog access")

    monkeypatch.setattr(command, "get_vector_sources", no_network)
    monkeypatch.setattr(command, "fetch_partition_metadata", no_network)
    output = tmp_path / "out.geojsonl"
    result = CliRunner().invoke(command.extract, ["--parquet", str(manifest), "-b", "0,0,10,10", "-o", str(output)])
    assert result.exit_code == 0, result.output
    rows = [json.loads(line.lstrip("\x1e")) for line in output.read_text().splitlines() if line.strip()]
    assert sorted(r["properties"]["id"] for r in rows) == [1, 3]


def test_manifest_prunes_extents_but_preserves_boundary_and_missing_bounds(tmp_path):
    extents = {
        "a.parquet": {"minx": 0, "miny": 0, "maxx": 10, "maxy": 10},
        "b.parquet": {"minx": 50, "miny": 50, "maxx": 60, "maxy": 60},
        "c.parquet": {},
    }
    for name in extents:
        (tmp_path / name).touch()
    manifest = tmp_path / "data.parquet.meta.json"
    manifest.write_text(json.dumps({"extents": extents}))
    assert local_parquet_paths([manifest], box(10, 0, 20, 10)) == [
        str(tmp_path / "a.parquet"),
        str(tmp_path / "c.parquet"),
    ]
    with pytest.raises(ValueError, match="Duplicate"):
        local_parquet_paths([manifest, tmp_path / "a.parquet"])
    manifest.write_text(json.dumps({"extents": {"../outside.parquet": {}}}))
    with pytest.raises(ValueError, match="Invalid partition"):
        local_parquet_paths([manifest])


def test_cli_rejects_mixed_source_options(tmp_path):
    path = tmp_path / "a.parquet"
    path.touch()
    result = CliRunner().invoke(
        command.extract, ["-s", "anything", "--parquet", str(path), "-o", str(tmp_path / "out.geojsonl")]
    )
    assert result.exit_code != 0
    assert "exactly one" in result.output
