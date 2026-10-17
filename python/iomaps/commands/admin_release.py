import base64
import json
import os
import re
import textwrap
from pathlib import Path

import click
import questionary
from github import Auth, Github, InputGitTreeElement

known_repos = [
    "indian_admin_boundaries",
    "indian_water_features",
    "indian_transport",
    "indian_communications",
    "indian_facilities",
    "indian_cadastrals",
    "indian_land_features",
    "india_natural_disasters",
    "indian_buildings",
    "indian_power_infra",
    "indian_industries",
]

gh_api = None
DEFAULT_LICENSE = (
    "[CC0 1.0 but attribute datameet and the original government source where possible]"
    "(https://github.com/ramSeraph/indianopenmaps/blob/main/DATA_LICENSE.md)"
)


def get_github_api():
    global gh_api

    if gh_api is not None:
        return gh_api

    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        # Support running from either the checkout root or a subdirectory.
        for directory in (Path.cwd(), *Path.cwd().parents):
            if directory == Path.cwd() or (directory / ".git").exists():
                for relative_path in (".secrets/gh_token.txt", "utils/token.txt"):
                    token_file = directory / relative_path
                    if token_file.is_file():
                        token = token_file.read_text().strip()
                        if token:
                            break
            if token or (directory / ".git").exists():
                break
    if not token:
        raise click.ClickException(
            "GitHub authentication not found; set GH_TOKEN or GITHUB_TOKEN, "
            "or use .secrets/gh_token.txt in the checkout"
        )
    auth = Auth.Token(token)
    gh_api = Github(auth=auth)
    return gh_api


def md_to_html(text):
    """Convert markdown links to HTML <a> tags, then linkify remaining bare URLs."""
    # Convert [text](url) markdown links
    html = re.sub(r"\[([^\]]+)\]\((https?://[^)]+)\)", r'<a href="\2">\1</a>', text)
    # Linkify bare URLs not already inside <a> tags
    parts = re.split(r"(<a [^>]*>.*?</a>)", html)
    return "".join(
        part if part.startswith("<a ") else re.sub(r'(https?://[^\s,)<"]+)', r'<a href="\1">\1</a>', part)
        for part in parts
    )


def resolve_text_option(text, file_path):
    """Resolve a text value from direct text or a file path."""
    if text and file_path:
        raise click.UsageError("Cannot specify both text and file for the same option")
    if file_path:
        return Path(file_path).read_text().strip()
    return text


def get_releases_for_repo(repo_name, owner=None):
    """Get available releases for a specific repository.

    Args:
        repo: The selected repository name

    Returns:
        List of available releases for the repository
    """
    api = get_github_api()
    owner = owner or api.get_user().login
    repo = api.get_repo(f"{owner}/{repo_name}")

    releases = repo.get_releases()
    return [r.tag_name for r in releases]


def find_related_files(base_path: Path) -> dict:
    """Find all related files for a given base path.

    Returns dict with keys: 7z, pmtiles, mosaic, parquet, parquet_meta
    Each value is a list of Path objects.
    """
    base_name = base_path.stem
    if base_name.endswith(".geojsonl"):
        base_name = base_name[:-9]
    elif base_name.endswith(".pmtiles"):
        base_name = base_name[:-8]
    elif base_name.endswith(".parquet"):
        base_name = base_name[:-8]

    parent_dir = base_path.parent

    files = {
        "7z": [],
        "pmtiles": [],
        "mosaic": [],
        "parquet": [],
        "parquet_meta": [],
        "analysis": [],
    }

    # Find 7z files (single or split)
    single_7z = parent_dir / f"{base_name}.geojsonl.7z"
    if single_7z.exists():
        files["7z"].append(single_7z)
    else:
        # Look for split 7z files
        files["7z"] = sorted(parent_dir.glob(f"{base_name}.geojsonl.7z.*"))

    # Find pmtiles files (single or split)
    single_pmtiles = parent_dir / f"{base_name}.pmtiles"
    if single_pmtiles.exists():
        files["pmtiles"].append(single_pmtiles)
    else:
        # Look for split pmtiles files (partXXXX pattern)
        files["pmtiles"] = sorted(parent_dir.glob(f"{base_name}-part*.pmtiles"))
        # Look for mosaic.json
        mosaic_file = parent_dir / f"{base_name}.mosaic.json"
        if mosaic_file.exists():
            files["mosaic"].append(mosaic_file)

    # Find parquet files (single or split)
    single_parquet = parent_dir / f"{base_name}.parquet"
    if single_parquet.exists():
        files["parquet"].append(single_parquet)
    else:
        # Look for split parquet files
        files["parquet"] = sorted(parent_dir.glob(f"{base_name}.*.parquet"))
        # Look for parquet meta.json
        parquet_meta = parent_dir / f"{base_name}.parquet.meta.json"
        if parquet_meta.exists():
            files["parquet_meta"].append(parquet_meta)

    # Find analysis.json file
    analysis_file = parent_dir / f"{base_name}.analysis.json"
    if analysis_file.exists():
        files["analysis"] = [analysis_file]

    return files


def build_route_entry(
    owner,
    repo_name,
    release,
    file_pmtiles,
    description,
    category,
    is_mosaic=False,
    is_partitioned_parquet=False,
    promoteid=None,
    license_str=None,
    notes_html=None,
):
    """Build the exact route payload shared by preview and publication."""
    entry = {
        "handlertype": "mosaic" if is_mosaic else "pmtiles",
        "category": list(category),
        "name": description,
        "url": f"https://github.com/{owner}/{repo_name}/releases/download/{release}/{file_pmtiles.name}",
    }
    if is_partitioned_parquet:
        entry["partitioned_parquet"] = True
    if license_str:
        entry["license"] = license_str
    if notes_html:
        entry["notes"] = notes_html
    if promoteid:
        entry["promoteid"] = promoteid
    return entry


def update_routes_file(
    repo_name,
    release,
    file_pmtiles,
    route,
    description,
    category,
    is_mosaic=False,
    is_partitioned_parquet=False,
    promoteid=None,
    license_str=None,
    notes_html=None,
    owner=None,
):
    code_repo_name = "indianopenmaps"
    branch = "main"
    file_path = "worker/src/routes/listing.json"
    gh = get_github_api()
    owner = owner or gh.get_user().login
    github_repo = gh.get_repo(f"{owner}/{code_repo_name}")

    base_ref = github_repo.get_git_ref(f"heads/{branch}")
    base_sha = base_ref.object.sha
    base_tree = github_repo.get_git_tree(base_sha)

    file_content = github_repo.get_contents(file_path, ref=branch)

    # Decode content if it's base64 encoded
    if isinstance(file_content.content, bytes):
        file_content = base64.b64decode(file_content.content).decode("utf-8")
    else:
        file_content = base64.b64decode(file_content.content.encode("utf-8")).decode("utf-8")

    commit_msg = f"[ROUTE ADDITION] {file_pmtiles.name}"
    route_data = json.loads(file_content)

    entry = build_route_entry(
        owner,
        repo_name,
        release,
        file_pmtiles,
        description,
        category,
        is_mosaic,
        is_partitioned_parquet,
        promoteid,
        license_str,
        notes_html,
    )
    route_data.update({route: entry})

    content = json.dumps(route_data, indent=2)

    # Create blob with new content
    blob = github_repo.create_git_blob(content, "utf-8")
    element = InputGitTreeElement(path=file_path, mode="100644", type="blob", sha=blob.sha)

    # Create new tree with the updated file
    new_tree = github_repo.create_git_tree([element], base_tree)

    # Create commit
    parent = github_repo.get_git_commit(base_sha)
    commit = github_repo.create_git_commit(commit_msg, new_tree, [parent])

    base_ref.edit(commit.sha)


def upload_file(repo_name, release, file, owner=None):
    gh = get_github_api()
    owner = owner or gh.get_user().login
    github_repo = gh.get_repo(f"{owner}/{repo_name}")
    release_obj = github_repo.get_release(release)
    release_obj.upload_asset(path=str(file), content_type="application/octet-stream", name=file.name)


def build_release_notes(
    owner, repo_name, release, description, files_7z, route, source, source_url, license_str=None, notes_md=None
):
    """Build the exact Markdown appended to the existing release."""
    # Build the header with file links
    if len(files_7z) == 1:
        # Single file
        file_7z = files_7z[0]
        header = f"[{file_7z.name}](https://github.com/{owner}/{repo_name}/releases/download/{release}/{file_7z.name})"
    else:
        # Split files - get base name and create part links
        base_name = files_7z[0].name.rsplit(".", 1)[0]  # Remove .001 etc
        parts = []
        for i, f in enumerate(files_7z, 1):
            url = f"https://github.com/{owner}/{repo_name}/releases/download/{release}/{f.name}"
            parts.append(f"[Part {i}]({url})")
        header = f"{base_name}: {', '.join(parts)}"

    license_line = f"\n    - License: {license_str or DEFAULT_LICENSE}"

    notes_line = f"\n    - Notes: {notes_md}" if notes_md else ""

    to_append = f"""
    ### {header}
    - Description: {description}
    - Source: {source} - {source_url}{license_line}{notes_line}
    - Tiles - https://indianopenmaps.com{route}{{z}}/{{x}}/{{y}}.pbf - [view](https://indianopenmaps.com{route}view)
    """

    return textwrap.dedent(to_append)


def update_release_doc(
    repo_name, release, description, files_7z, route, source, source_url, license_str=None, notes_md=None, owner=None
):
    gh = get_github_api()
    owner = owner or gh.get_user().login
    github_repo = gh.get_repo(f"{owner}/{repo_name}")
    release_obj = github_repo.get_release(release)
    body = release_obj.raw_data["body"] or ""
    to_append = build_release_notes(
        owner, repo_name, release, description, files_7z, route, source, source_url, license_str, notes_md
    )

    new_body = body + "\n\n" + to_append
    release_obj.update_release(release_obj.name, new_body)


class QuestionaryOption(click.Option):
    def __init__(self, param_decls=None, **attrs):
        click.Option.__init__(self, param_decls, **attrs)
        if not isinstance(self.type, click.Choice):
            raise Exception("ChoiceOption type arg must be click.Choice")

    def prompt_for_value(self, ctx):
        val = questionary.select(self.prompt, choices=self.type.choices).unsafe_ask()
        return val


class DynamicReleaseOption(click.Option):
    def prompt_for_value(self, ctx):
        if ctx.params.get("dry_run"):
            raise click.UsageError("--release is required with --dry-run; no GitHub lookup is made")
        repo = ctx.params.get("repo")
        if not repo:
            raise click.UsageError("Repository must be selected before release")
        choices = get_releases_for_repo(repo, owner=ctx.params.get("owner"))
        return questionary.select(self.prompt, choices=choices).unsafe_ask()


@click.command("make-release")
@click.option(
    "--dry-run",
    is_flag=True,
    is_eager=True,
    help="Print the exact release plan as JSON without authentication, network access, or GitHub writes",
)
@click.option(
    "--owner",
    type=str,
    is_eager=True,
    help="GitHub owner of both repositories; required for --dry-run, otherwise defaults to the authenticated user",
)
@click.option(
    "--repo",
    type=click.Choice(known_repos),
    prompt="Repository",
    help="Repository to upload to",
    show_choices=False,
    cls=QuestionaryOption,
)
@click.option(
    "--release",
    type=str,
    prompt="Release",
    help="Release in repo to upload to",
    show_choices=False,
    cls=DynamicReleaseOption,
)
@click.option("--base-file", type=str, help="GeoJSONL, .geojsonl.7z, or .pmtiles file to discover related assets from")
@click.option("--description", type=str, help="description")
@click.option("--source", type=str, help="source")
@click.option("--source-url", type=str, help="source url")
@click.option("--route", type=str, help="tile route")
@click.option("--category", type=str, help="category", multiple=True)
@click.option(
    "--no-uploads",
    is_flag=True,
    default=False,
    help="Skip asset uploads but still update GitHub release notes and routes; use --dry-run for a preview",
)
@click.option(
    "--license",
    "license_str",
    type=str,
    default=None,
    help="license text (markdown links converted to HTML for routes)",
)
@click.option(
    "--license-file", type=click.Path(exists=True), default=None, help="file containing license text (markdown)"
)
@click.option(
    "--notes", "notes_str", type=str, default=None, help="notes text (markdown links converted to HTML for routes)"
)
@click.option("--notes-file", type=click.Path(exists=True), default=None, help="file containing notes text (markdown)")
@click.option("--promoteid", type=str, help="override promoteid for vector tiles")
def make_release(
    dry_run,
    owner,
    repo,
    release,
    base_file,
    description,
    source,
    source_url,
    route,
    category,
    no_uploads,
    license_str,
    license_file,
    notes_str,
    notes_file,
    promoteid,
):
    """Upload prepared assets and update release metadata."""

    if dry_run and not owner:
        raise click.UsageError("--owner is required with --dry-run; no GitHub lookup is made")
    if base_file is None:
        raise click.UsageError("--base-file is required")
    for option, value in (("description", description), ("source", source), ("source-url", source_url)):
        if not value or not value.strip():
            raise click.UsageError(f"--{option} is required")
    if not route or not route.startswith("/") or not route.endswith("/"):
        raise click.UsageError("--route must start and end with /")
    if not category:
        raise click.UsageError("At least one --category is required")

    # Resolve notes and license from text or file
    notes_md = resolve_text_option(notes_str, notes_file)
    license_md = resolve_text_option(license_str, license_file)

    # Convert markdown links to HTML for listing.json (routes)
    notes_html = md_to_html(notes_md) if notes_md else None
    license_html = md_to_html(license_md) if license_md else None

    base_path = Path(base_file).resolve()
    if not base_path.exists():
        raise click.UsageError(f"{base_file} does not exist")

    # Find all related files
    related = find_related_files(base_path)

    # Validate we have the minimum required files
    if not related["7z"]:
        raise click.UsageError("No 7z files found")
    if not related["pmtiles"] and not related["mosaic"]:
        raise click.UsageError("No pmtiles files found")

    # Determine if this is a mosaic (split pmtiles)
    is_mosaic = len(related["mosaic"]) > 0

    # Determine if parquet is partitioned
    is_partitioned_parquet = len(related["parquet_meta"]) > 0

    # Get the main file for route registration
    if is_mosaic:
        route_file = related["mosaic"][0]
    else:
        route_file = related["pmtiles"][0]

    # Read promoteid from analysis file if not provided via CLI
    warnings = []
    if not promoteid and related["analysis"]:
        analysis_file = related["analysis"][0]
        try:
            with open(analysis_file) as f:
                analysis_data = json.load(f)
            picked_id = analysis_data.get("picked_id", {})
            promoteid = picked_id.get("field")
            if not promoteid:
                available_attrs = list(analysis_data.get("analysis", {}).get("fields", {}))
                warnings.append(f"Unable to pick a promoteid. Available attributes: {available_attrs}")
        except (OSError, ValueError) as e:
            warnings.append(f"Failed to read analysis file: {e}")

    owner = owner or get_github_api().get_user().login
    upload_files = [f for kind in ("7z", "pmtiles", "mosaic", "parquet", "parquet_meta") for f in related[kind]]
    if dry_run:
        entry = build_route_entry(
            owner,
            repo,
            release,
            route_file,
            description,
            category,
            is_mosaic,
            is_partitioned_parquet,
            promoteid,
            license_html,
            notes_html,
        )
        plan = {
            "dry_run": True,
            "data_repository": f"{owner}/{repo}",
            "release": release,
            "release_url": f"https://github.com/{owner}/{repo}/releases/tag/{release}",
            "uploads": [
                {"name": f.name, "path": str(f), "bytes": f.stat().st_size} for f in upload_files if not no_uploads
            ],
            "route_repository": f"{owner}/indianopenmaps",
            "route_branch": "main",
            "route_file": "worker/src/routes/listing.json",
            "route_commit_message": f"[ROUTE ADDITION] {route_file.name}",
            "route_entry": {route: entry},
            "release_license_markdown": license_md or DEFAULT_LICENSE,
            "release_notes_markdown": build_release_notes(
                owner, repo, release, description, related["7z"], route, source, source_url, license_md, notes_md
            ),
            "warnings": warnings,
        }
        click.echo(json.dumps(plan, indent=2))
        return

    print(f"Release: {owner}/{repo} / {release}")
    print(f"Route: {route} (promoteid: {promoteid})")
    for warning in warnings:
        click.echo(f"Warning: {warning}", err=True)
    if not no_uploads:
        for f in upload_files:
            print(f"uploading file {f}")
            upload_file(repo, release, f, owner=owner)

    # append to release doc (uses original markdown for GitHub rendering)
    print("updating release notes")
    update_release_doc(
        repo,
        release,
        description,
        related["7z"],
        route,
        source,
        source_url,
        license_str=license_md,
        notes_md=notes_md,
        owner=owner,
    )
    # update route (uses HTML-converted links for listing.json)
    print("updating routes file")
    update_routes_file(
        repo,
        release,
        route_file,
        route,
        description,
        category,
        is_mosaic,
        is_partitioned_parquet,
        promoteid,
        license_str=license_html,
        notes_html=notes_html,
        owner=owner,
    )


if __name__ == "__main__":
    make_release()
