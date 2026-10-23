import json
from pathlib import Path
from unittest.mock import Mock

import click
import pytest
from click.testing import CliRunner
from conftest import git

pytest.importorskip("github")
pytest.importorskip("questionary")

from iomaps.commands import admin_publish as publish
from iomaps.commands import admin_release as cli


@pytest.fixture
def saved_plan(tmp_path):
    for suffix in ("geojsonl.7z", "pmtiles", "parquet"):
        (tmp_path / f"Test_Plots.{suffix}").write_bytes(b"prepared asset")
    result = CliRunner().invoke(
        cli.make_release,
        [
            "--dry-run",
            "--owner",
            "ramSeraph",
            "--repo",
            "indian_land_features",
            "--release",
            "urban-land-use",
            "--base-file",
            str(tmp_path / "Test_Plots.pmtiles"),
            "--description",
            "Test plots",
            "--source",
            "Test authority",
            "--source-url",
            "https://example.com/source",
            "--route",
            "/plots/test/",
            "--category",
            "plots",
            "--promoteid",
            "id",
        ],
    )
    assert result.exit_code == 0, result.output
    path = tmp_path / "preview.json"
    path.write_text(result.output)
    return path


def execute(path, checkout, **kwargs):
    return CliRunner().invoke(
        cli.make_release,
        [
            "--plan",
            str(path),
            "--checkout",
            str(checkout),
            "--no-verify",
            *kwargs.get("args", []),
        ],
    )


def test_upload_batch_then_publish_after_local_cleanup(saved_plan, git_checkout, authenticated, monkeypatch):
    from iomaps.commands import admin_verify

    api, release = authenticated
    original = json.loads(saved_plan.read_text())
    files = [a for a in original["uploads"] if not a["name"].endswith(".pmtiles")]
    args = [
        "--phase",
        "uploads",
        "--owner",
        "ramSeraph",
        "--repo",
        "indian_land_features",
        "--release",
        "urban-land-use",
    ]
    for asset in files:
        args += ["--asset", asset["path"]]
    result = CliRunner().invoke(cli.make_release, [*args, "--dry-run"])
    assert result.exit_code == 0, result.output
    api.get_repo.assert_not_called()
    batch = saved_plan.with_name("batch.json")
    batch.write_text(result.output)
    before = git(git_checkout, "rev-parse", "HEAD")
    sample = Mock(side_effect=AssertionError("Uploads must not need tiles"))
    monkeypatch.setattr(admin_verify, "choose_sample_tile", sample)
    upload_args = ["--phase", "uploads", "--plan", str(batch)]
    for _ in range(2):
        result = CliRunner().invoke(cli.make_release, upload_args)
        assert result.exit_code == 0, result.output
    assert release.upload_asset.call_count == 2
    release.update_release.assert_not_called()
    assert git(git_checkout, "rev-parse", "HEAD") == before
    receipt = json.loads(batch.with_suffix(".receipt.json").read_text())
    assert receipt["stage"] == "uploads_complete"
    assert "assets_verified" not in receipt  # cannot authorize route publication
    for asset in files:
        Path(asset["path"]).unlink()
    result = CliRunner().invoke(cli.make_release, upload_args)
    assert result.exit_code == 0, result.output
    assert release.upload_asset.call_count == 2
    preview_args = [
        "--owner",
        "ramSeraph",
        "--repo",
        "indian_land_features",
        "--release",
        "urban-land-use",
        "--base-file",
        str(saved_plan.parent / "Test_Plots.pmtiles"),
        "--description",
        "Test plots",
        "--source",
        "Test authority",
        "--source-url",
        "https://example.com/source",
        "--route",
        "/plots/test/",
        "--category",
        "plots",
        "--promoteid",
        "id",
        "--uploaded-plan",
        str(batch),
        "--dry-run",
    ]
    result = CliRunner().invoke(cli.make_release, preview_args)
    assert result.exit_code == 0, result.output
    final = json.loads(result.output)
    assert len(final["uploads"]) == 3
    assert sum(a.get("remote_only", False) for a in final["uploads"]) == 2
    saved_plan.write_text(result.output)
    result = execute(saved_plan, git_checkout)
    assert result.exit_code == 0, result.output
    assert release.upload_asset.call_count == 3
    assert release.update_release.call_count == 1


@pytest.mark.parametrize("problem", ["missing", "changed"])
def test_remote_only_asset_is_reverified_before_other_writes(saved_plan, git_checkout, authenticated, problem):
    _, release = authenticated
    plan = json.loads(saved_plan.read_text())
    asset = plan["uploads"][0]
    remote = release.upload_asset(path=asset["path"], name=asset["name"])
    if problem == "missing":
        release.get_assets.side_effect = lambda: []
    else:
        remote.raw_data["digest"] = "sha256:" + "0" * 64
    asset["remote_only"] = True
    Path(asset["path"]).unlink()
    saved_plan.write_text(json.dumps(plan))
    release.upload_asset.reset_mock()
    before = git(git_checkout, "rev-parse", "HEAD")
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    release.upload_asset.assert_not_called()
    release.update_release.assert_not_called()
    assert git(git_checkout, "rev-parse", "HEAD") == before


@pytest.fixture
def authenticated(monkeypatch, release_api):
    api, release = release_api
    monkeypatch.setattr(cli, "get_github_api", lambda: api)
    return api, release


def test_saved_plan_commits_only_listing_and_retry_is_noop(saved_plan, git_checkout, authenticated):
    api, release = authenticated
    other = git_checkout / "unrelated.txt"
    other.write_text("staged change\n")
    git(git_checkout, "add", "unrelated.txt")
    other.write_text("unstaged addition\n")
    staged = git(git_checkout, "show", ":unrelated.txt")
    first = execute(saved_plan, git_checkout)
    assert first.exit_code == 0, first.output
    head = git(git_checkout, "rev-parse", "HEAD")
    assert git(git_checkout, "diff-tree", "--no-commit-id", "--name-only", "-r", head) == publish.ROUTE_FILE
    assert git(git_checkout, "rev-parse", "origin/main") == head
    assert git(git_checkout, "show", ":unrelated.txt") == staged
    assert other.read_text() == "unstaged addition\n"
    assert git(git_checkout, "show", "HEAD:unrelated.txt") == "original"
    assert release.upload_asset.call_count == 3
    body = release.body
    second = execute(saved_plan, git_checkout)
    assert second.exit_code == 0, second.output
    assert release.upload_asset.call_count == 3
    assert release.update_release.call_count == 1
    assert release.body == body
    assert git(git_checkout, "rev-parse", "HEAD") == head
    receipt = json.loads(saved_plan.with_suffix(".receipt.json").read_text())
    assert receipt["github_verified"] is True
    assert receipt["live_verification"] == {"status": "skipped"}
    api.get_repo("ramSeraph/indianopenmaps").create_git_commit.assert_not_called()


@pytest.mark.parametrize("problem", ["dirty_listing", "staged_listing", "ahead", "branch", "remote"])
def test_git_preflight_fails_before_uploads(saved_plan, git_checkout, authenticated, problem):
    _, release = authenticated
    listing = git_checkout / publish.ROUTE_FILE
    if problem in ("dirty_listing", "staged_listing"):
        listing.write_text('{"user edit": true}')
        if problem == "staged_listing":
            git(git_checkout, "add", publish.ROUTE_FILE)
    elif problem == "ahead":
        git(git_checkout, "commit", "--allow-empty", "-m", "Unpushed work")
    elif problem == "branch":
        git(git_checkout, "checkout", "-b", "other")
    else:
        git(git_checkout, "remote", "set-url", "origin", "https://github.com/other/repo.git")
    before = listing.read_bytes()
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    release.upload_asset.assert_not_called()
    release.update_release.assert_not_called()
    assert listing.read_bytes() == before


def test_preview_hash_detects_same_size_asset_change_before_auth(saved_plan, git_checkout, monkeypatch):
    api = Mock(side_effect=AssertionError("Unexpected authentication"))
    monkeypatch.setattr(cli, "get_github_api", api)
    plan = json.loads(saved_plan.read_text())
    Path(plan["uploads"][0]["path"]).write_bytes(b"tampered asset")
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    assert "asset changed since preview" in result.output
    api.assert_not_called()


@pytest.mark.parametrize("args", [["--owner", "another"], ["--no-uploads"], ["--description", "Changed"]])
def test_plan_disallows_metadata_overrides(saved_plan, git_checkout, monkeypatch, args):
    monkeypatch.setattr(cli, "get_github_api", Mock(side_effect=AssertionError("Unexpected authentication")))
    result = execute(saved_plan, git_checkout, args=args)
    assert result.exit_code == 2
    assert "--plan cannot be combined" in result.output


def test_partial_upload_retry_skips_completed_files(saved_plan, git_checkout, authenticated):
    _, release = authenticated
    upload = release.upload_asset.side_effect

    def interrupted(**kwargs):
        if release.upload_asset.call_count == 2:
            raise RuntimeError("connection lost")
        return upload(**kwargs)

    release.upload_asset.side_effect = interrupted
    first = execute(saved_plan, git_checkout)
    assert first.exit_code != 0
    release.update_release.assert_not_called()
    release.upload_asset.side_effect = upload
    second = execute(saved_plan, git_checkout)
    assert second.exit_code == 0, second.output
    assert release.upload_asset.call_count == 4  # one success, one failure, two remaining files
    assert release.update_release.call_count == 1


def test_push_failure_retries_the_same_local_commit(saved_plan, git_checkout, authenticated, monkeypatch):
    _, release = authenticated
    run_git = publish.LocalRoutes.git

    def fail_push(self, *args):
        if args[0] == "push":
            raise click.ClickException("test push rejected")
        return run_git(self, *args)

    monkeypatch.setattr(publish.LocalRoutes, "git", fail_push)
    first = execute(saved_plan, git_checkout)
    assert first.exit_code != 0
    head = git(git_checkout, "rev-parse", "HEAD")
    assert head != git(git_checkout, "rev-parse", "origin/main")
    monkeypatch.setattr(publish.LocalRoutes, "git", run_git)
    second = execute(saved_plan, git_checkout)
    assert second.exit_code == 0, second.output
    assert git(git_checkout, "rev-parse", "HEAD") == head == git(git_checkout, "rev-parse", "origin/main")
    assert release.upload_asset.call_count == 3
    assert release.update_release.call_count == 1


def test_concurrent_local_commit_is_not_pushed(saved_plan, git_checkout, authenticated, monkeypatch):
    run_git = publish.LocalRoutes.git
    original_remote = git(git_checkout, "rev-parse", "origin/main")

    def concurrent_commit(self, *args):
        if args[0] == "commit":
            git(git_checkout, "commit", "--allow-empty", "-m", "Concurrent unreviewed work")
        return run_git(self, *args)

    monkeypatch.setattr(publish.LocalRoutes, "git", concurrent_commit)
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    assert "history changed during publication" in result.output
    assert git(git_checkout, "rev-parse", "origin/main") == original_remote


@pytest.mark.parametrize("advance_during_upload", [False, True])
def test_upstream_is_fast_forwarded_without_losing_routes(
    saved_plan, git_checkout, authenticated, tmp_path, advance_during_upload
):
    import subprocess

    _, release = authenticated
    other = tmp_path / "other-checkout"
    remote = git(git_checkout, "remote", "get-url", "origin")
    subprocess.run(["git", "clone", remote, str(other)], check=True, capture_output=True)
    git(other, "config", "user.name", "Another publisher")
    git(other, "config", "user.email", "other@example.com")
    git(other, "config", "commit.gpgsign", "false")
    git(other, "config", "core.hooksPath", str(tmp_path / "no-hooks"))

    def advance():
        listing = other / publish.ROUTE_FILE
        data = json.loads(listing.read_text())
        data["/someone-else/"] = {"name": "Another dataset"}
        listing.write_text(json.dumps(data, indent=2))
        git(other, "add", publish.ROUTE_FILE)
        git(other, "commit", "-m", "Another route")
        git(other, "push", "origin", "main")

    if advance_during_upload:
        upload = release.upload_asset.side_effect

        def upload_and_advance(**kwargs):
            if release.upload_asset.call_count == 1:
                advance()
            return upload(**kwargs)

        release.upload_asset.side_effect = upload_and_advance
    else:
        advance()
    result = execute(saved_plan, git_checkout)
    assert result.exit_code == 0, result.output
    routes = json.loads((git_checkout / publish.ROUTE_FILE).read_text())
    assert set(routes) == {"/existing/", "/someone-else/", "/plots/test/"}
    assert git(git_checkout, "rev-parse", "HEAD") == git(git_checkout, "rev-parse", "origin/main")


def test_plan_dry_run_stays_offline_and_old_plans_are_rejected(saved_plan, git_checkout, monkeypatch):
    api = Mock(side_effect=AssertionError("Unexpected network/authentication"))
    monkeypatch.setattr(cli, "get_github_api", api)
    result = execute(saved_plan, git_checkout, args=["--dry-run"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == json.loads(saved_plan.read_text())
    plan = json.loads(saved_plan.read_text())
    del plan["schema_version"]
    saved_plan.write_text(json.dumps(plan))
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    assert "generate a new --dry-run preview" in result.output
    api.assert_not_called()


def test_conflicting_existing_asset_fails_before_any_write(saved_plan, git_checkout, authenticated):
    _, release = authenticated
    plan = json.loads(saved_plan.read_text())
    asset = plan["uploads"][-1]
    remote = release.upload_asset(path=asset["path"], name=asset["name"])
    remote.raw_data["digest"] = "sha256:" + "0" * 64
    release.upload_asset.reset_mock()
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    assert "different content" in result.output
    release.upload_asset.assert_not_called()
    release.update_release.assert_not_called()


def test_no_uploads_requires_all_assets(saved_plan, git_checkout, authenticated):
    _, release = authenticated
    plan = json.loads(saved_plan.read_text())
    plan["skip_uploads"] = True
    saved_plan.write_text(json.dumps(plan))
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    assert "already uploaded asset" in result.output
    release.upload_asset.assert_not_called()


def test_existing_asset_without_digest_is_hashed(monkeypatch):
    import hashlib
    from types import SimpleNamespace

    data = b"old asset content"
    remote = SimpleNamespace(
        state="uploaded", size=len(data), raw_data={}, browser_download_url="https://example.com/file"
    )
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.iter_content.return_value = [data]
    monkeypatch.setattr(publish.requests, "get", Mock(return_value=response))
    publish.verify_asset(
        remote, {"name": "old.pmtiles", "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    )
    response.raise_for_status.assert_called_once()


def test_failed_live_check_can_resume_without_republishing(saved_plan, git_checkout, authenticated, monkeypatch):
    from iomaps.commands import admin_verify

    _, release = authenticated
    monkeypatch.setattr(admin_verify, "choose_sample_tile", Mock(return_value={"data": b"tile"}))
    monkeypatch.setattr(admin_verify, "verify_live", Mock(side_effect=click.ClickException("test deployment timeout")))
    args = ["--plan", str(saved_plan), "--checkout", str(git_checkout)]
    first = CliRunner().invoke(cli.make_release, args)
    assert first.exit_code != 0
    head = git(git_checkout, "rev-parse", "HEAD")
    receipt = json.loads(saved_plan.with_suffix(".receipt.json").read_text())
    assert receipt["stage"] == "published" and receipt["github_verified"]
    monkeypatch.setattr(admin_verify, "verify_live", Mock(return_value={"status": "verified"}))
    second = CliRunner().invoke(cli.make_release, args)
    assert second.exit_code == 0, second.output
    assert git(git_checkout, "rev-parse", "HEAD") == head
    assert release.upload_asset.call_count == 3
    receipt = json.loads(saved_plan.with_suffix(".receipt.json").read_text())
    assert receipt["status"] == "published and verified live"


def test_notes_update_preserves_other_sections():
    body = "Intro\n\n### Other\nKeep this.\n\n### Dataset\nOld metadata.\n\n### Last\nAlso keep.\n"
    result = publish.merge_release_notes(body, "\n### Dataset\nNew metadata.\n")
    assert result == body.replace("Old metadata.", "New metadata.")
    assert publish.merge_release_notes(result, "\n### Dataset\nNew metadata.\n") == result
    with pytest.raises(click.ClickException, match="duplicate"):
        publish.merge_release_notes(body + "\n### Dataset\nDuplicate\n", "\n### Dataset\nNew metadata.\n")


def test_multiline_notes_and_legacy_section_migration():
    legacy = "Intro\n\n### Dataset\nOld metadata.\n\n### Last\nKeep.\n"
    first = publish.wrap_release_notes("/test/", "### Dataset\nNew metadata.\n\n### Subheading\nDetails.\n")
    body = publish.merge_release_notes(legacy, first)
    assert "Old metadata" not in body
    assert body.endswith("### Last\nKeep.\n")
    assert publish.merge_release_notes(body, first) == body
    second = publish.wrap_release_notes("/test/", "### Dataset\nChanged.\n\n### New subheading\nNew details.\n")
    updated = publish.merge_release_notes(body, second)
    assert "### Subheading" not in updated
    assert updated.endswith("### Last\nKeep.\n")
    assert publish.merge_release_notes(updated, second) == updated


def test_commit_failure_resumes_only_its_own_listing_edit(saved_plan, git_checkout, authenticated, monkeypatch):
    _, release = authenticated
    run_git = publish.LocalRoutes.git

    def fail_commit(self, *args):
        if args[0] == "commit":
            raise click.ClickException("test commit interrupted")
        return run_git(self, *args)

    monkeypatch.setattr(publish.LocalRoutes, "git", fail_commit)
    assert execute(saved_plan, git_checkout).exit_code != 0
    assert git(git_checkout, "status", "--porcelain", "--", publish.ROUTE_FILE)
    monkeypatch.setattr(publish.LocalRoutes, "git", run_git)
    result = execute(saved_plan, git_checkout)
    assert result.exit_code == 0, result.output
    assert git(git_checkout, "rev-parse", "HEAD") == git(git_checkout, "rev-parse", "origin/main")
    assert release.upload_asset.call_count == 3


def test_retry_rejects_user_changes_to_interrupted_listing(saved_plan, git_checkout, authenticated, monkeypatch):
    run_git = publish.LocalRoutes.git

    def fail_commit(self, *args):
        if args[0] == "commit":
            raise click.ClickException("test commit interrupted")
        return run_git(self, *args)

    monkeypatch.setattr(publish.LocalRoutes, "git", fail_commit)
    assert execute(saved_plan, git_checkout).exit_code != 0
    listing = git_checkout / publish.ROUTE_FILE
    listing.write_text('{"user edit": true}')
    monkeypatch.setattr(publish.LocalRoutes, "git", run_git)
    result = execute(saved_plan, git_checkout)
    assert result.exit_code != 0
    assert "has local edits" in result.output
    assert listing.read_text() == '{"user edit": true}'


def test_remote_assets_then_local_routes_without_dataset_files(saved_plan, git_checkout, authenticated, monkeypatch):
    from iomaps.commands import admin_verify

    api, release = authenticated
    before = git(git_checkout, "rev-parse", "HEAD")
    sample = {"z": 4, "x": 11, "y": 6, "data": b"tile", "layers": [{"id": "Test_Plots"}]}
    monkeypatch.setattr(admin_verify, "choose_sample_tile", Mock(return_value=sample))
    live = Mock(return_value={"status": "verified"})
    monkeypatch.setattr(admin_verify, "verify_live", live)
    # The asset host has no Git checkout or Git credentials.
    result = CliRunner().invoke(cli.make_release, ["--plan", str(saved_plan), "--phase", "assets"])
    assert result.exit_code == 0, result.output
    assert git(git_checkout, "rev-parse", "HEAD") == before
    assert release.upload_asset.call_count == 3
    assert all(call.args[0] != "ramSeraph/indianopenmaps" for call in api.get_repo.call_args_list)
    live.assert_not_called()
    receipt = json.loads(saved_plan.with_suffix(".receipt.json").read_text())
    assert receipt["assets_verified"] is True
    assert receipt["stage"] == "assets_complete"
    assert "data" not in receipt["sample_tile"]
    for asset in json.loads(saved_plan.read_text())["uploads"]:
        Path(asset["path"]).unlink()
    monkeypatch.setattr(admin_verify, "choose_sample_tile", Mock(side_effect=AssertionError("Local dataset needed")))
    args = ["--plan", str(saved_plan), "--phase", "routes", "--checkout", str(git_checkout)]
    for _ in range(2):
        result = CliRunner().invoke(cli.make_release, args)
        assert result.exit_code == 0, result.output
    assert git(git_checkout, "rev-parse", "HEAD") != before
    assert git(git_checkout, "rev-parse", "HEAD") == git(git_checkout, "rev-parse", "origin/main")
    assert release.upload_asset.call_count == 3
    assert release.update_release.call_count == 1
    assert live.call_args.args[2] == receipt["sample_tile"]


@pytest.mark.parametrize("problem", ["missing_receipt", "bad_notes", "bad_digest", "missing_asset", "missing_sample"])
def test_local_route_phase_rechecks_handoff_before_committing(
    saved_plan, git_checkout, authenticated, problem, monkeypatch
):
    from iomaps.commands import admin_verify

    _, release = authenticated
    monkeypatch.setattr(
        admin_verify,
        "choose_sample_tile",
        Mock(return_value={"z": 4, "x": 11, "y": 6, "data": b"tile", "layers": [{"id": "Test"}]}),
    )
    result = CliRunner().invoke(cli.make_release, ["--plan", str(saved_plan), "--phase", "assets"])
    assert result.exit_code == 0, result.output
    receipt_path = saved_plan.with_suffix(".receipt.json")
    if problem == "missing_receipt":
        receipt_path.unlink()
    elif problem == "bad_notes":
        release.body = "Changed by someone else"
    elif problem == "bad_digest":
        release.get_assets()[0].raw_data["digest"] = "sha256:" + "0" * 64
    elif problem == "missing_asset":
        assets = release.get_assets()[1:]
        release.get_assets.side_effect = lambda: assets
    else:
        receipt = json.loads(receipt_path.read_text())
        receipt.pop("sample_tile")
        receipt_path.write_text(json.dumps(receipt))
    before = git(git_checkout, "rev-parse", "HEAD")
    result = CliRunner().invoke(
        cli.make_release, ["--plan", str(saved_plan), "--phase", "routes", "--checkout", str(git_checkout)]
    )
    assert result.exit_code != 0
    assert git(git_checkout, "rev-parse", "HEAD") == before
    assert release.upload_asset.call_count == 3
    assert release.update_release.call_count == 1
