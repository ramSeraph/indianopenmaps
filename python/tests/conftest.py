import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest


def git(path, *args):
    return subprocess.check_output(["git", "-C", str(path), *args], text=True, stderr=subprocess.PIPE).strip()


@pytest.fixture
def git_checkout(tmp_path, monkeypatch):
    from iomaps.commands import admin_publish

    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "--bare", "--initial-branch=main", str(remote)], check=True, capture_output=True)
    checkout = tmp_path / "checkout"
    subprocess.run(["git", "init", "--initial-branch=main", str(checkout)], check=True, capture_output=True)
    git(checkout, "config", "user.name", "Test publisher")
    git(checkout, "config", "user.email", "publisher@example.com")
    git(checkout, "config", "commit.gpgsign", "false")
    git(checkout, "config", "core.hooksPath", str(tmp_path / "no-hooks"))
    listing = checkout / admin_publish.ROUTE_FILE
    listing.parent.mkdir(parents=True)
    listing.write_text(json.dumps({"/existing/": {"name": "Keep this", "handlertype": "pmtiles"}}, indent=2))
    (checkout / "unrelated.txt").write_text("original\n")
    git(checkout, "add", ".")
    git(checkout, "commit", "-m", "Initial routes")
    git(checkout, "remote", "add", "origin", str(remote))
    git(checkout, "push", "-u", "origin", "main")
    normalize = admin_publish.github_remote
    monkeypatch.setattr(
        admin_publish,
        "github_remote",
        lambda url: "ramSeraph/indianopenmaps" if url == str(remote) else normalize(url),
    )
    return checkout


@pytest.fixture
def release_api(git_checkout):
    from iomaps.commands.admin_publish import ROUTE_FILE

    assets = {}
    release = Mock()
    release.name = "Urban Land Use"
    release.body = "Existing release notes\n"
    release.html_url = "https://github.com/ramSeraph/indian_land_features/releases/tag/urban-land-use"
    release.get_assets.side_effect = lambda: list(assets.values())

    def upload(path, name, **kwargs):
        assert name not in assets, "Attempted duplicate upload"
        data = Path(path).read_bytes()
        asset = SimpleNamespace(
            name=name,
            state="uploaded",
            size=len(data),
            raw_data={"digest": "sha256:" + hashlib.sha256(data).hexdigest()},
            browser_download_url=f"https://github.com/example/{name}",
        )
        assets[name] = asset
        return asset

    release.upload_asset.side_effect = upload
    release.update_release.side_effect = lambda name, body: setattr(release, "body", body)
    data_repo = Mock()
    data_repo.get_release.return_value = release
    code_repo = Mock()
    remote = git(git_checkout, "remote", "get-url", "origin")
    code_repo.get_contents.side_effect = lambda path, ref: SimpleNamespace(
        decoded_content=git(remote, "show", f"{ref}:{ROUTE_FILE}").encode()
    )
    api = Mock()
    api.get_user.return_value.login = "ramSeraph"
    api.get_repo.side_effect = {
        "ramSeraph/indian_land_features": data_repo,
        "ramSeraph/indianopenmaps": code_repo,
    }.__getitem__
    return api, release
