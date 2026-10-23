"""Execute a reviewed publication plan using GitHub releases and a local Git checkout."""

import hashlib
import json
import re
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import click
import requests

ROUTE_FILE = "worker/src/routes/listing.json"
PLAN_VERSION = 1


def file_digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def describe_asset(path):
    path = Path(path).resolve()
    return {"name": path.name, "path": str(path), "bytes": path.stat().st_size, "sha256": file_digest(path)}


def wrap_release_notes(route, section):
    """Stable, invisible boundaries allow arbitrary Markdown inside a dataset section."""
    key = hashlib.sha256(route.encode()).hexdigest()
    return f"\n<!-- iomaps:{key} -->\n{section.strip()}\n<!-- /iomaps:{key} -->\n"


def validate_assets(plan, check_files=True):
    """Validate an upload manifest, including files already staged on GitHub."""
    if not isinstance(plan["uploads"], list) or not plan["uploads"]:
        raise ValueError("the plan must list all required assets, including with --no-uploads")
    names, paths = set(), set()
    for asset in plan["uploads"]:
        name, path = asset["name"], Path(asset["path"])
        if name in names or path in paths or name != path.name or not path.is_absolute():
            raise ValueError("asset names/paths must be unique absolute paths with matching filenames")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", name) or name in (".", ".."):
            raise ValueError(f"invalid asset name: {name}")
        if not isinstance(asset["bytes"], int) or asset["bytes"] <= 0:
            raise ValueError(f"invalid asset size: {name}")
        if not re.fullmatch(r"[0-9a-f]{64}", asset["sha256"]):
            raise ValueError(f"invalid SHA-256: {name}")
        if not isinstance(asset.get("remote_only", False), bool):
            raise ValueError(f"invalid remote_only flag: {name}")
        if check_files and not asset.get("remote_only"):
            if path.stat().st_size != asset["bytes"] or file_digest(path) != asset["sha256"]:
                raise ValueError(f"asset changed since preview: {name}; generate and review a new preview")
        names.add(name)
        paths.add(path)
    return names


def validate_plan(plan, check_files=True):
    """Reject incomplete/old plans and changed files before any remote operation."""
    try:
        if plan.get("schema_version") != PLAN_VERSION:
            raise ValueError("generate a new --dry-run preview; this plan has no supported schema version")
        owner, repo = plan["data_repository"].split("/")
        if not all(re.fullmatch(r"[\w.-]+", part) for part in (owner, repo)):
            raise ValueError("invalid data repository")
        if not isinstance(plan["release"], str) or not plan["release"].strip():
            raise ValueError("missing release")
        names = validate_assets(plan, check_files)
        if plan.get("kind") == "uploads":
            if any(key in plan for key in ("route_entry", "release_notes_markdown")):
                raise ValueError("an uploads-only plan cannot contain routes or release notes")
            return
        if plan["route_repository"] != f"{owner}/indianopenmaps":
            raise ValueError("route repository must be <owner>/indianopenmaps")
        if plan["route_branch"] != "main" or plan["route_file"] != ROUTE_FILE:
            raise ValueError("only main and worker/src/routes/listing.json are supported")
        for key in ("release", "route_commit_message", "release_notes_markdown", "release_license_markdown"):
            if not isinstance(plan[key], str) or not plan[key].strip():
                raise ValueError(f"missing {key}")
        if not isinstance(plan["skip_uploads"], bool):
            raise ValueError("skip_uploads must be a boolean")
        if len(plan["route_entry"]) != 1:
            raise ValueError("exactly one route entry is required")
        route, entry = next(iter(plan["route_entry"].items()))
        if not re.fullmatch(r"/(?:[\w.-]+/)+", route):
            raise ValueError("invalid route path")
        if entry["handlertype"] not in ("pmtiles", "mosaic") or not entry["name"] or not entry["category"]:
            raise ValueError("invalid route entry")
        prefix = f"https://github.com/{owner}/{repo}/releases/download/{plan['release']}/"
        if entry["url"] not in {prefix + name for name in names}:
            raise ValueError("route URL must reference one of the planned release assets")
        if not any(name.endswith(".pmtiles") for name in names):
            raise ValueError("no PMTiles assets in the plan")
        if not re.search(r"(?m)^### ", plan["release_notes_markdown"]):
            raise ValueError("release notes must start with a dataset heading")
    except (KeyError, TypeError, ValueError, OSError, AttributeError) as exc:
        raise click.ClickException(f"Invalid publication plan: {exc}") from exc


def merge_release_notes(body, section):
    """Replace this dataset's section, leaving other datasets and release prose alone."""
    marker = re.match(r"\s*(<!-- iomaps:([0-9a-f]{64}) -->)", section)
    if marker:
        start_marker, key = marker.groups()
        end_marker = f"<!-- /iomaps:{key} -->"
        if not section.strip().endswith(end_marker):
            raise click.ClickException("Invalid dataset notes boundary in plan")
        starts, ends = body.count(start_marker), body.count(end_marker)
        if starts or ends:
            if starts != 1 or ends != 1 or body.index(start_marker) > body.index(end_marker):
                raise click.ClickException("The release has duplicate or incomplete dataset notes boundaries")
            start, end = body.index(start_marker), body.index(end_marker) + len(end_marker)
            return body[:start] + section.strip() + body[end:]
    heading = next(line for line in section.splitlines() if line.startswith("### "))
    matches = list(re.finditer(rf"(?m)^{re.escape(heading)}[ \t]*$", body))
    if len(matches) > 1:
        raise click.ClickException("The release already has duplicate dataset sections; resolve them before retrying")
    if not matches:
        return body + "\n\n" + section
    start = matches[0].start()
    following = re.search(r"(?m)^### ", body[matches[0].end() :])
    end = matches[0].end() + following.start() if following else len(body)
    if body[start:end].strip() == section.strip():
        return body
    return body[:start] + section.strip() + ("\n\n" if following else "\n") + body[end:]


def github_remote(url):
    if url.startswith("git@github.com:"):
        return url.removeprefix("git@github.com:").removesuffix(".git").rstrip("/")
    parsed = urlparse(url)
    if parsed.hostname == "github.com" and parsed.scheme in ("ssh", "https"):
        return parsed.path.strip("/").removesuffix(".git")
    return None


class LocalRoutes:
    def __init__(self, checkout, plan, receipt, save):
        self.root = Path(checkout).resolve()
        self.plan, self.receipt, self.save = plan, receipt, save
        self.root = Path(self.git("rev-parse", "--show-toplevel"))

    def git(self, *args):
        result = subprocess.run(["git", "-C", str(self.root), *args], capture_output=True, text=True)
        if result.returncode:
            raise click.ClickException(f"git {args[0]} failed: {result.stderr.strip() or result.stdout.strip()}")
        return result.stdout.strip()

    def check_clean_listing(self):
        if self.git("status", "--porcelain", "--", ROUTE_FILE):
            # An interrupted attempt may have written this exact plan but not committed it.
            if self.receipt.get("route_worktree_base") == self.git("rev-parse", "HEAD"):
                committed = json.loads(self.git("show", f"HEAD:{ROUTE_FILE}"))
                expected = {**committed, **self.plan["route_entry"]}
                try:
                    staged = json.loads(self.git("show", f":{ROUTE_FILE}"))
                    if json.loads((self.root / ROUTE_FILE).read_text()) == expected and staged in (committed, expected):
                        return
                except (ValueError, OSError):
                    pass
            raise click.ClickException(f"{ROUTE_FILE} has local edits; resolve them before publishing")

    def is_pending_publication(self):
        """Only resume the single, exact route commit recorded by this plan's receipt."""
        head = self.git("rev-parse", "HEAD")
        recorded_commit = self.receipt.get("route_sha") == head
        interrupted_commit = self.receipt.get("route_worktree_base") == self.git("rev-parse", "HEAD^")
        if not recorded_commit and not interrupted_commit:
            return False
        if self.git("diff-tree", "--no-commit-id", "--name-only", "-r", head) != ROUTE_FILE:
            return False
        if self.git("log", "-1", "--format=%B") != self.plan["route_commit_message"]:
            return False
        before = json.loads(self.git("show", f"HEAD^:{ROUTE_FILE}"))
        after = json.loads(self.git("show", f"HEAD:{ROUTE_FILE}"))
        return after == {**before, **self.plan["route_entry"]}

    def preflight(self):
        if self.git("symbolic-ref", "--short", "HEAD") != "main":
            raise click.ClickException("Publish from the local main branch")
        remote = github_remote(self.git("remote", "get-url", "--all", "origin"))
        push_remote = github_remote(self.git("remote", "get-url", "--push", "--all", "origin"))
        if any(
            value is None or value.lower() != self.plan["route_repository"].lower() for value in (remote, push_remote)
        ):
            raise click.ClickException("origin fetch and push URLs must match the plan's route repository")
        for marker in ("MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD", "rebase-merge", "rebase-apply"):
            path = Path(self.git("rev-parse", "--git-path", marker))
            if (path if path.is_absolute() else self.root / path).exists():
                raise click.ClickException("Finish the in-progress Git operation before publishing")
        if self.git("diff", "--name-only", "--diff-filter=U"):
            raise click.ClickException("Resolve Git conflicts before publishing")
        self.check_clean_listing()
        click.echo("Fetching origin and checking local history...")
        self.git("fetch", "origin")
        ahead, behind = map(int, self.git("rev-list", "--left-right", "--count", "HEAD...origin/main").split())
        if ahead and not (ahead == 1 and behind == 0 and self.is_pending_publication()):
            raise click.ClickException(
                "Local main has unpushed or divergent commits; sync/push those before publishing"
            )
        if behind:
            self.git("merge", "--ff-only", "origin/main")
        self.check_clean_listing()
        # Fail before uploads if the listing or Git author configuration is unusable.
        json.loads((self.root / ROUTE_FILE).read_text())
        self.git("var", "GIT_AUTHOR_IDENT")
        self.git("var", "GIT_COMMITTER_IDENT")

    def publish(self):
        # Recheck after uploads: another publisher may have advanced main in the meantime.
        self.preflight()
        path = self.root / ROUTE_FILE
        original = path.read_text()
        routes = json.loads(original)
        updated = {**routes, **self.plan["route_entry"]}
        head_routes = json.loads(self.git("show", f"HEAD:{ROUTE_FILE}"))
        if updated != head_routes:
            self.receipt.update(route_worktree_base=self.git("rev-parse", "HEAD"), stage="route_prepared")
            self.save()
            if updated != routes:
                path.write_text(json.dumps(updated, indent=2) + ("\n" if original.endswith("\n") else ""))
            click.echo("Committing only the route listing...")
            self.git("commit", "--only", "-m", self.plan["route_commit_message"], "--", ROUTE_FILE)
            # Check hooks did not change the intended content or commit other paths.
            if (
                json.loads(self.git("show", f"HEAD:{ROUTE_FILE}")) != updated
                or self.git("diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD") != ROUTE_FILE
            ):
                raise click.ClickException("Route commit differs from the plan; inspect it before pushing")
        sha = self.git("rev-parse", "HEAD")
        self.receipt.update(route_sha=sha, stage="route_committed")
        self.receipt.pop("route_worktree_base", None)
        self.save()
        self.check_clean_listing()
        if sha != self.git("rev-parse", "origin/main"):
            ahead, behind = map(int, self.git("rev-list", "--left-right", "--count", "HEAD...origin/main").split())
            if ahead != 1 or behind or not self.is_pending_publication():
                raise click.ClickException("Git history changed during publication; inspect it before pushing")
            click.echo("Pushing the route commit...")
            # Pin the reviewed commit even if another process changes HEAD just before push.
            self.git("push", "origin", f"{sha}:refs/heads/main")
        return sha


def verify_asset(remote, expected):
    if remote.state != "uploaded" or remote.size != expected["bytes"]:
        raise click.ClickException(f"Existing asset is incomplete or has a different size: {expected['name']}")
    digest = remote.raw_data.get("digest")
    if not digest:
        # Older GitHub assets may not expose a digest; verify their bytes before skipping them.
        sha, size = hashlib.sha256(), 0
        with requests.get(remote.browser_download_url, stream=True, timeout=30) as response:
            response.raise_for_status()
            for chunk in response.iter_content(1024 * 1024):
                size += len(chunk)
                if size > expected["bytes"]:
                    raise click.ClickException(f"Downloaded asset is larger than expected: {expected['name']}")
                sha.update(chunk)
        if size != expected["bytes"]:
            raise click.ClickException(f"Downloaded asset is incomplete: {expected['name']}")
        digest = "sha256:" + sha.hexdigest()
    if digest != "sha256:" + expected["sha256"]:
        raise click.ClickException(
            f"Existing asset has different content: {expected['name']}; it will not be overwritten"
        )


def release_assets(release):
    return {asset.name: asset for asset in release.get_assets()}


def publish_plan(plan, api, checkout, receipt_path, verify=True, timeout=300, phase="all"):
    """Run resumable publication; the caller validates the plan before authentication."""
    from .admin_verify import choose_sample_tile, sample_manifest, validate_sample_manifest, verify_live

    if phase not in ("all", "assets", "routes", "uploads"):
        raise click.ClickException(f"Unknown publication phase: {phase}")
    if (phase == "uploads") != (plan.get("kind") == "uploads"):
        raise click.ClickException("An uploads-only plan requires --phase uploads, and vice versa")

    fingerprint = hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
    receipt_path = Path(receipt_path).resolve()
    receipt = {"plan_sha256": fingerprint, "status": "in_progress", "stage": "preflight"}
    if receipt_path.exists():
        try:
            previous = json.loads(receipt_path.read_text())
        except (ValueError, OSError) as exc:
            raise click.ClickException(f"Cannot read receipt: {receipt_path}") from exc
        if previous.get("plan_sha256") != fingerprint:
            raise click.ClickException("Receipt belongs to another plan; choose a different --receipt path")
        receipt.update(previous, status="in_progress")
        receipt.pop("error", None)

    def save():
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = receipt_path.with_name(receipt_path.name + ".tmp")
        temporary.write_text(json.dumps(receipt, indent=2) + "\n")
        temporary.replace(receipt_path)

    if phase == "routes" and not receipt.get("assets_verified"):
        raise click.ClickException("--phase routes requires the successful receipt from --phase assets")
    routes = None if phase in ("assets", "uploads") else LocalRoutes(checkout, plan, receipt, save)
    protected = {Path(a["path"]).resolve() for a in plan["uploads"]}
    if routes:
        protected.add(routes.root / ROUTE_FILE)
    if receipt_path in protected or (routes and receipt_path.is_relative_to(routes.root / ".git")):
        raise click.ClickException("Receipt path must not overwrite an asset, the route listing, or Git metadata")
    try:
        if routes:
            routes.preflight()
        sample = None
        if phase == "routes":
            if verify:
                sample = receipt.get("sample_tile")
                validate_sample_manifest(sample)
        elif verify and phase != "uploads":
            sample = sample_manifest(choose_sample_tile(plan))
            receipt["sample_tile"] = sample
        data_repo = api.get_repo(plan["data_repository"])
        release = data_repo.get_release(plan["release"])
        existing = release_assets(release)
        # Inspect every existing asset and notes section before the first write.
        for asset in plan["uploads"]:
            if asset["name"] in existing:
                verify_asset(existing[asset["name"]], asset)
            elif plan.get("skip_uploads") or phase == "routes" or asset.get("remote_only"):
                raise click.ClickException(f"This phase requires an already uploaded asset: {asset['name']}")
            elif phase == "uploads":
                path = Path(asset["path"])
                if path.stat().st_size != asset["bytes"] or file_digest(path) != asset["sha256"]:
                    raise click.ClickException(f"Asset changed since preview: {asset['name']}")
        if phase != "uploads":
            merge_release_notes(release.body or "", plan["release_notes_markdown"])
        if phase != "routes":
            receipt.update(stage="assets", release_url=release.html_url)
            save()
            for asset in plan["uploads"]:
                if asset["name"] in existing:
                    click.echo(f"Verified existing asset: {asset['name']}")
                else:
                    click.echo(f"Uploading {asset['name']} ({asset['bytes']:,} bytes)...")
                    uploaded = release.upload_asset(
                        path=asset["path"],
                        name=asset["name"],
                        content_type="image/png"
                        if asset["name"].endswith(".coverage.png")
                        else "application/octet-stream",
                    )
                    verify_asset(uploaded, asset)
                receipt.setdefault("verified_assets", {})[asset["name"]] = asset["sha256"]
                save()
            if phase == "uploads":
                # Verify every asset again from a fresh release response before allowing cleanup.
                existing = release_assets(data_repo.get_release(plan["release"]))
                for asset in plan["uploads"]:
                    if asset["name"] not in existing:
                        raise click.ClickException(f"Missing uploaded asset: {asset['name']}")
                    verify_asset(existing[asset["name"]], asset)
                receipt.update(status="uploads verified; publication pending", stage="uploads_complete")
                save()
                click.echo(f"{receipt['status']}. Receipt: {receipt_path}")
                return receipt
            # Refresh so we preserve notes written by others while our uploads ran.
            release = data_repo.get_release(plan["release"])
            body = release.body or ""
            updated = merge_release_notes(body, plan["release_notes_markdown"])
            if updated != body:
                click.echo("Updating the dataset's release notes section...")
                release.update_release(release.name, updated)
            receipt.update(stage="release_notes")
            save()
        # Both phases verify the release; routes never trusts a receipt alone.
        verify_release(plan, data_repo)
        receipt["assets_verified"] = True
        if phase == "assets":
            receipt.update(status="assets published; route pending", stage="assets_complete")
            save()
            click.echo(f"{receipt['status']}. Copy the plan and receipt to the route checkout: {receipt_path}")
            return receipt
        sha = routes.publish()
        receipt.update(
            stage="published",
            status="published",
            route_commit=(f"https://github.com/{plan['route_repository']}/commit/{sha}"),
        )
        save()
        # Verify actual remote state even when deployment polling was explicitly skipped.
        verify_release(plan, data_repo)
        remote = api.get_repo(plan["route_repository"]).get_contents(ROUTE_FILE, ref="main")
        remote_routes = json.loads(remote.decoded_content)
        route, entry = next(iter(plan["route_entry"].items()))
        if remote_routes.get(route) != entry:
            raise click.ClickException("Remote route differs from the plan")
        receipt["github_verified"] = True
        save()
        if verify:
            receipt["live_verification"] = verify_live(plan, sha, sample, timeout)
            receipt["status"] = "published and verified live"
        else:
            receipt["live_verification"] = {"status": "skipped"}
        receipt["stage"] = "complete"
        save()
        click.echo(f"{receipt['status']}. Receipt: {receipt_path}")
        return receipt
    except (Exception, KeyboardInterrupt) as exc:
        receipt.update(status="incomplete", error=str(exc) or "Interrupted")
        save()
        if isinstance(exc, (click.ClickException, KeyboardInterrupt)):
            raise
        raise click.ClickException(f"Publication stopped: {exc}. Resume the same plan; see {receipt_path}") from exc


def verify_release(plan, data_repo):
    release = data_repo.get_release(plan["release"])
    remote_assets = release_assets(release)
    for asset in plan["uploads"]:
        if asset["name"] not in remote_assets:
            raise click.ClickException(f"Missing uploaded asset: {asset['name']}")
        verify_asset(remote_assets[asset["name"]], asset)
    if merge_release_notes(release.body or "", plan["release_notes_markdown"]) != (release.body or ""):
        raise click.ClickException("Remote release notes differ from the plan")
