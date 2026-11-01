import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock

import click
import pyarrow.parquet as pq
import pytest
from conftest import git
from shapely.geometry import Polygon, box, mapping

from iomaps.commands import admin_publish, admin_stac
from iomaps.commands.admin_stac_publish import STAC_FILE, build_stac_plan, merge_catalog


@pytest.mark.parametrize("legacy_text", [False, True])
def test_header_encoding_does_not_discard_georeferencing(tmp_path, monkeypatch, legacy_text):
    data = {
        "coordinateSystem": {"wkt": "test CRS"},
        "size": [100, 200],
        "geoTransform": [0, 0.04, 0, 4, 0, -0.02],
        "wgs84Extent": mapping(box(0, 0, 4, 4)),
        "stac": {"proj:epsg": 4326},
        "metadata": {"IMAGE_STRUCTURE": {"LAYOUT": "COG"}, "": {"description": "degree PLACEHOLDER"}},
    }
    raw = json.dumps(data).encode().replace(b"PLACEHOLDER", b"\xb0" if legacy_text else b"symbol")
    reader = Mock(return_value=SimpleNamespace(stdout=raw, stderr=b"legacy diagnostic \xb0"))
    monkeypatch.setattr(admin_stac.subprocess, "run", reader)
    output = tmp_path / "maps.stac.parquet"
    options = {
        "rows": [{"name": "old.tif", "url": "https://example.com/old.tif"}],
        "originals": {},
        "footprints": None,
        "output": output,
        "collection_id": "maps",
        "title": "Maps",
        "description": "Old map",
        "license_id": "other",
        "timestamp": datetime(2025, 1, 1, tzinfo=UTC),
        "datetime_description": "Publication",
        "source_url": "https://example.com",
        "cache": tmp_path / "cache",
    }
    report = admin_stac.prepare_stac(**options)
    row = pq.read_table(output).to_pylist()[0]
    assert row["proj:code"] == "EPSG:4326"
    assert row["proj:transform"] == [0.04, 0, 0, 0, -0.02, 4, 0, 0, 1]
    assert row["assets"]["cog"]["href"] == "https://example.com/old.tif"
    assert bool(report["metadata_warnings"]) is legacy_text
    if legacy_text:
        assert report["metadata_warnings"][0]["id"] == "old"
    assert admin_stac.prepare_stac(**options)["metadata_warnings"] == report["metadata_warnings"]
    reader.assert_called_once()


@pytest.fixture
def prepared_index(tmp_path, monkeypatch):
    metadata = {
        "size": [100, 200],
        "geoTransform": [0, 0.04, 0, 4, 0, -0.02],
        "wgs84Extent": mapping(box(0, 0, 4, 4)),
        "stac": {"proj:epsg": 4326},
        "metadata": {"IMAGE_STRUCTURE": {"LAYOUT": "COG"}},
    }
    monkeypatch.setattr(admin_stac, "raster_metadata", lambda *args: metadata)
    output = tmp_path / "maps.stac.parquet"
    options = {
        "rows": [{"name": "one.tif", "url": "https://github.com/example/releases/download/maps/one.tif"}],
        "originals": {"one": {"name": "one.jpg", "url": "https://github.com/example/releases/download/scans/one.jpg"}},
        "footprints": {
            "one": {"geometry": mapping(Polygon([(0, 0), (4, 0), (0, 4)])), "properties": {"edition": 1956}}
        },
        "output": output,
        "collection_id": "maps",
        "title": "Maps",
        "description": "Test maps",
        "license_id": "other",
        "timestamp": datetime(2025, 1, 1, tzinfo=UTC),
        "datetime_description": "Derivative publication",
        "source_url": "https://example.com",
        "cache": tmp_path / "cache",
    }
    admin_stac.prepare_stac(**options)
    return options


def test_generator_preserves_footprint_projection_originals_and_real_geoparquet(prepared_index):
    output = prepared_index["output"]
    table = pq.read_table(output)
    assert json.loads(table.schema.metadata[b"geo"])["primary_column"] == "geometry"
    row = table.to_pylist()[0]
    assert row["proj:code"] == "EPSG:4326"
    assert row["proj:shape"] == [200, 100]
    assert row["proj:transform"] == [0.04, 0, 0, 0, -0.02, 4, 0, 0, 1]
    assert row["iomaps:source_properties"] == {"edition": 1956}
    assert row["assets"]["original"]["href"].endswith("/one.jpg")
    from shapely import from_wkb

    assert from_wkb(row["geometry"]).area == 8
    collection = json.loads(output.with_suffix(".collection.json").read_text())
    assert collection["license"] == "other"
    assert collection["item_count"] == 1
    assert collection["extent"]["temporal"]["interval"] == [["2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"]]


def test_large_index_supports_bounded_remote_page_reads(prepared_index):
    options = {**prepared_index, "footprints": None}
    options["rows"] = [{"name": f"sheet-{i:04}.tif", "url": f"https://example.com/{i}.tif"} for i in range(1300)]
    admin_stac.prepare_stac(**options)
    parquet = pq.ParquetFile(options["output"])
    assert [parquet.metadata.row_group(i).num_rows for i in range(parquet.num_row_groups)] == [512, 512, 276]
    assert parquet.metadata.row_group(0).column(0).has_offset_index
    assert parquet.read_row_group(1, columns=["id"])["id"].to_pylist() == [f"sheet-{i:04}" for i in range(512, 1024)]
    assert json.loads(parquet.schema_arrow.metadata[b"stac-geoparquet"])["collections"]["maps"]["item_count"] == 1300


def test_failed_header_read_does_not_publish_partial_index(prepared_index, monkeypatch):
    original = prepared_index["output"].read_bytes()
    monkeypatch.setattr(admin_stac, "raster_metadata", Mock(side_effect=RuntimeError("source unavailable")))
    with pytest.raises(ValueError, match="no index written"):
        admin_stac.prepare_stac(**prepared_index)
    assert prepared_index["output"].read_bytes() == original
    report = json.loads(prepared_index["output"].with_suffix(".report.json").read_text())
    assert report["expected"] == 1 and report["prepared"] == 0
    assert report["failures"][0]["id"] == "one"
    with pytest.raises(click.UsageError, match="latest preparation failed"):
        make_plan(prepared_index)


def test_reuse_requires_the_same_asset_url_and_can_be_bypassed(prepared_index, monkeypatch):
    from iomaps.core.stac_client import RasterCatalog

    items = list(RasterCatalog(indexes={"maps": prepared_index["output"]}).search())
    options = {**prepared_index, "reuse_items": {item["id"]: item for item in items}}
    reader = Mock(side_effect=RuntimeError("new header required"))
    monkeypatch.setattr(admin_stac, "raster_metadata", reader)
    assert admin_stac.prepare_stac(**options)["reused_headers"] == 1
    reader.assert_not_called()
    with pytest.raises(ValueError, match="no index written"):
        admin_stac.prepare_stac(**options, refresh=True)
    reader.assert_called_once()
    reader.reset_mock()
    options["rows"] = [{**options["rows"][0], "url": "https://example.com/changed.tif"}]
    with pytest.raises(ValueError, match="no index written"):
        admin_stac.prepare_stac(**options)
    reader.assert_called_once()


def make_plan(options, **kwargs):
    return build_stac_plan(
        owner="ramSeraph",
        repo="indian_land_features",
        release="urban-land-use",
        index=options["output"],
        collection_path=options["output"].with_suffix(".collection.json"),
        license_md="License not specified by source.",
        **kwargs,
    )


def test_stac_publication_uses_shared_receipts_and_only_commits_catalog(
    prepared_index,
    git_checkout,
    release_api,
    tmp_path,
):
    api, release = release_api
    path = git_checkout / STAC_FILE
    existing = {"id": "root", "links": [{"rel": "child", "href": "./keep", "geoparquet": "https://example.com/keep"}]}
    path.write_text(json.dumps(existing))
    git(git_checkout, "add", STAC_FILE)
    git(git_checkout, "commit", "-m", "Existing STAC catalog")
    git(git_checkout, "push", "origin", "main")
    remote = git(git_checkout, "remote", "get-url", "origin")
    api.get_repo("ramSeraph/indianopenmaps").get_contents.side_effect = lambda path, ref: SimpleNamespace(
        decoded_content=git(remote, "show", f"{ref}:{path}").encode()
    )
    (git_checkout / "unrelated.txt").write_text("uncommitted edit\n")
    plan = make_plan(prepared_index)
    receipt = tmp_path / "receipt.json"
    admin_publish.publish_plan(plan, api, git_checkout, receipt, verify=False)
    assert json.loads(path.read_text()) == merge_catalog(existing, plan)
    assert git(git_checkout, "diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD") == STAC_FILE
    assert (git_checkout / "unrelated.txt").read_text() == "uncommitted edit\n"
    assert release.upload_asset.call_count == 2
    admin_publish.publish_plan(plan, api, git_checkout, receipt, verify=False)
    assert release.upload_asset.call_count == 2, "resume verifies existing assets"
    assert len(json.loads(path.read_text())["links"]) == 2
    assert release.body.count("### STAC: Maps") == 1


def test_stac_plan_rejects_changed_assets_and_invalid_notes(prepared_index):
    plan = make_plan(prepared_index)
    plan["release_notes_markdown"] = "no heading"
    with pytest.raises(click.ClickException, match="heading"):
        admin_publish.validate_plan(plan)
    plan = make_plan(prepared_index)
    prepared_index["output"].write_bytes(b"changed")
    with pytest.raises(click.ClickException, match="changed since preview"):
        admin_publish.validate_plan(plan)


@pytest.mark.parametrize("interrupt", [False, True])
def test_stac_replacement_checks_old_hash_and_resumes_after_delete(prepared_index, release_api, tmp_path, interrupt):
    api, release = release_api
    old = make_plan(prepared_index)
    admin_publish.publish_plan(old, api, tmp_path, tmp_path / "old.json", phase="assets")
    originals = {a.name: a for a in release.get_assets()}
    options = {**prepared_index, "title": "Corrected title"}
    admin_stac.prepare_stac(**options)
    plan = make_plan(options, replacements=[(a["name"], a["sha256"]) for a in old["uploads"]])
    entry = next(iter(plan["route_entry"].values()))
    assert entry["geoparquet"] == next(iter(old["route_entry"].values()))["geoparquet"]
    assert entry["geoparquet_sha256"] == plan["uploads"][0]["sha256"]
    receipt = tmp_path / "replacement.json"
    uploader = release.upload_asset.side_effect
    if interrupt:
        release.upload_asset.side_effect = RuntimeError("upload interrupted")
        with pytest.raises(click.ClickException, match="upload interrupted"):
            admin_publish.publish_plan(plan, api, tmp_path, receipt, phase="assets")
        assert len(release.get_assets()) == 1
        release.upload_asset.side_effect = uploader
    admin_publish.publish_plan(plan, api, tmp_path, receipt, phase="assets")
    for asset in plan["uploads"]:
        originals[asset["name"]].delete_asset.assert_called_once()
    assert {a.name for a in release.get_assets()} == {a["name"] for a in old["uploads"]}
    admin_publish.verify_release(plan, api.get_repo(plan["data_repository"]))
    calls = release.upload_asset.call_count
    admin_publish.publish_plan(plan, api, tmp_path, receipt, phase="assets")
    assert release.upload_asset.call_count == calls


def test_stac_replacement_rejects_unreviewed_remote_contents_before_any_write(prepared_index, release_api, tmp_path):
    api, release = release_api
    old = make_plan(prepared_index)
    admin_publish.publish_plan(old, api, tmp_path, tmp_path / "old.json", phase="assets")
    admin_stac.prepare_stac(**{**prepared_index, "title": "Corrected title"})
    plan = make_plan(prepared_index, replacements=[(a["name"], "f" * 64) for a in old["uploads"]])
    calls = release.upload_asset.call_count
    with pytest.raises(click.ClickException, match="different content"):
        admin_publish.publish_plan(plan, api, tmp_path, tmp_path / "replacement.json", phase="assets")
    assert release.upload_asset.call_count == calls
    for asset in release.get_assets():
        asset.delete_asset.assert_not_called()


def test_replacement_cli_is_offline_and_records_expected_hashes(prepared_index):
    from click.testing import CliRunner

    from iomaps.commands.admin_release import make_release

    output = prepared_index["output"]
    result = CliRunner().invoke(
        make_release,
        [
            "--dry-run",
            "--owner",
            "ramSeraph",
            "--repo",
            "indian_land_features",
            "--release",
            "urban-land-use",
            "--base-file",
            str(output),
            "--stac-collection",
            str(output.with_suffix(".collection.json")),
            "--license",
            "Source terms",
            "--replace-asset",
            output.name,
            "a" * 64,
        ],
    )
    assert result.exit_code == 0, result.output
    plan = json.loads(result.output)
    assert plan["uploads"][0]["replaces_sha256"] == "a" * 64
    plan["uploads"][0]["replaces_sha256"] = "invalid"
    with pytest.raises(click.ClickException, match="replacement SHA-256"):
        admin_publish.validate_plan(plan)
    with pytest.raises(click.UsageError, match="must name"):
        make_plan(prepared_index, replacements=[("unrelated.tif", "a" * 64)])
