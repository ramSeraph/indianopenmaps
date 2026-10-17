import json
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

pytest.importorskip("github")
pytest.importorskip("questionary")

from iomaps.commands import admin_release as release

GET_GITHUB_API = release.get_github_api


@pytest.fixture(autouse=True)
def no_github(monkeypatch):
    """Every test must explicitly opt in to a fake GitHub client."""
    monkeypatch.setattr(release, "get_github_api", Mock(side_effect=AssertionError("Unexpected GitHub access")))


@pytest.fixture
def prepared(tmp_path):
    for suffix in ("geojsonl.7z", "pmtiles", "parquet"):
        (tmp_path / f"DDA_Delhi_Master_Plan.{suffix}").write_bytes(b"prepared asset")
    (tmp_path / "DDA_Delhi_Master_Plan.analysis.json").write_text(json.dumps({"picked_id": {"field": "fid"}}))
    return [
        "--owner",
        "ramSeraph",
        "--repo",
        "indian_land_features",
        "--release",
        "urban-land-use",
        "--base-file",
        str(tmp_path / "DDA_Delhi_Master_Plan.geojsonl.7z"),
        "--description",
        "DDA Delhi Land Use Plan",
        "--source",
        "Delhi Development Authority",
        "--source-url",
        "https://gis.dda.org.in/example",
        "--route",
        "/not-so-open/urban/regulation/landuse/delhi/dda/",
        "--category",
        "landuse",
        "--category",
        "delhi",
    ]


def preview(args):
    result = CliRunner().invoke(release.make_release, [*args, "--dry-run"])
    assert result.exit_code == 0, result.output
    return json.loads(result.output)


def test_dry_run_is_offline_and_shows_exact_destinations(prepared):
    plan = preview(prepared)
    assert plan["dry_run"] is True
    assert plan["data_repository"] == "ramSeraph/indian_land_features"
    assert plan["release"] == "urban-land-use"
    assert plan["route_repository"] == "ramSeraph/indianopenmaps"
    assert plan["route_branch"] == "main"
    assert {a["name"] for a in plan["uploads"]} == {
        "DDA_Delhi_Master_Plan.geojsonl.7z",
        "DDA_Delhi_Master_Plan.pmtiles",
        "DDA_Delhi_Master_Plan.parquet",
    }
    route, entry = next(iter(plan["route_entry"].items()))
    assert route == "/not-so-open/urban/regulation/landuse/delhi/dda/"
    assert entry["promoteid"] == "fid"
    assert entry["category"] == ["landuse", "delhi"]
    assert "license" not in entry  # Preserve the site's default attribution behavior.
    assert (
        "CC0 1.0 but attribute datameet and the original government source where possible"
        in (plan["release_license_markdown"])
    )
    assert f"- License: {plan['release_license_markdown']}" in plan["release_notes_markdown"]


@pytest.mark.parametrize("no_uploads", [False, True])
def test_published_payloads_match_preview_and_preserve_existing_content(
    prepared, monkeypatch, git_checkout, release_api, no_uploads
):
    args = [*prepared, *(["--no-uploads"] if no_uploads else [])]
    plan = preview(args)
    api, remote_release = release_api
    monkeypatch.setattr(release, "get_github_api", lambda: api)
    if no_uploads:
        for asset in plan["uploads"]:
            remote_release.upload_asset(path=asset["path"], name=asset["name"])
        remote_release.upload_asset.reset_mock()
    result = CliRunner().invoke(release.make_release, [*args, "--checkout", str(git_checkout), "--no-verify"])
    assert result.exit_code == 0, result.output
    assert remote_release.upload_asset.call_count == (0 if no_uploads else 3)
    remote_release.update_release.assert_called_once_with(
        "Urban Land Use", "Existing release notes\n" + "\n\n" + plan["release_notes_markdown"]
    )
    routes = json.loads((git_checkout / "worker/src/routes/listing.json").read_text())
    assert routes == {"/existing/": {"name": "Keep this", "handlertype": "pmtiles"}, **plan["route_entry"]}
    api.get_user.assert_not_called()


def test_custom_license_and_notes_keep_markdown_in_release_and_html_in_route(prepared, tmp_path):
    license_file = tmp_path / "license.txt"
    notes_file = tmp_path / "notes.txt"
    license_file.write_text("[Custom license](https://example.com/license)\n")
    notes_file.write_text("[Source notes](https://example.com/notes)\n")
    plan = preview([*prepared, "--license-file", str(license_file), "--notes-file", str(notes_file)])
    entry = next(iter(plan["route_entry"].values()))
    assert entry["license"] == '<a href="https://example.com/license">Custom license</a>'
    assert entry["notes"] == '<a href="https://example.com/notes">Source notes</a>'
    assert "- License: [Custom license](https://example.com/license)" in plan["release_notes_markdown"]
    assert "- Notes: [Source notes](https://example.com/notes)" in plan["release_notes_markdown"]


@pytest.mark.parametrize("option", ["--owner", "--release", "--description", "--source", "--source-url"])
def test_missing_preview_inputs_fail_without_github(prepared, option):
    args = prepared.copy()
    index = args.index(option)
    del args[index : index + 2]
    result = CliRunner().invoke(release.make_release, [*args, "--dry-run"])
    assert result.exit_code == 2
    assert option in result.output


def test_invalid_route_fails_before_any_github_call(prepared):
    args = prepared.copy()
    args[args.index("--route") + 1] = "missing-slashes"
    result = CliRunner().invoke(release.make_release, args)
    assert result.exit_code == 2
    assert "--route must start and end with /" in result.output


def test_split_assets_are_all_present_in_preview(prepared, tmp_path):
    for path in tmp_path.glob("DDA_Delhi_Master_Plan.*"):
        path.unlink()
    source = tmp_path / "DDA_Delhi_Master_Plan.geojsonl"
    source.write_bytes(b"source")
    suffixes = [
        ".geojsonl.7z.001",
        ".geojsonl.7z.002",
        "-part0000.pmtiles",
        "-part0001.pmtiles",
        ".mosaic.json",
        ".0.parquet",
        ".1.parquet",
        ".parquet.meta.json",
    ]
    for suffix in suffixes:
        (tmp_path / f"DDA_Delhi_Master_Plan{suffix}").write_bytes(b"prepared partition")
    args = prepared.copy()
    args[args.index("--base-file") + 1] = str(source)
    plan = preview([*args, "--promoteid", "fid"])
    assert {a["name"] for a in plan["uploads"]} == {f"DDA_Delhi_Master_Plan{s}" for s in suffixes}
    entry = next(iter(plan["route_entry"].values()))
    assert entry["handlertype"] == "mosaic"
    assert entry["partitioned_parquet"] is True
    assert entry["url"].endswith("DDA_Delhi_Master_Plan.mosaic.json")
    assert "[Part 1]" in plan["release_notes_markdown"] and "[Part 2]" in plan["release_notes_markdown"]


def test_publish_without_owner_keeps_authenticated_user_default(prepared, monkeypatch, git_checkout, release_api):
    args = prepared[2:]
    api, remote_release = release_api
    monkeypatch.setattr(release, "get_github_api", lambda: api)
    result = CliRunner().invoke(release.make_release, [*args, "--checkout", str(git_checkout), "--no-verify"])
    assert result.exit_code == 0, result.output
    api.get_user.assert_called_once()
    assert remote_release.upload_asset.call_count == 3


@pytest.mark.parametrize("source", ["repository", "legacy", "environment"])
def test_token_lookup_uses_local_secret_without_printing_it(tmp_path, monkeypatch, source):
    monkeypatch.setattr(release, "get_github_api", GET_GITHUB_API)
    monkeypatch.setattr(release, "gh_api", None)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    (tmp_path / ".git").mkdir()
    (tmp_path / ".secrets").mkdir()
    (tmp_path / ".secrets/gh_token.txt").write_text("repository-test-token\n")
    if source == "legacy":
        (tmp_path / ".secrets/gh_token.txt").unlink()
        (tmp_path / "utils").mkdir()
        (tmp_path / "utils/token.txt").write_text("legacy-test-token\n")
    if source == "environment":
        monkeypatch.setenv("GH_TOKEN", "environment-test-token")
        monkeypatch.setenv("GITHUB_TOKEN", "lower-priority-test-token")
    (tmp_path / "python").mkdir()
    monkeypatch.chdir(tmp_path / "python")
    github = Mock()
    monkeypatch.setattr(release, "Github", github)
    assert release.get_github_api() is github.return_value
    assert github.call_args.kwargs["auth"].token == f"{source}-test-token"
