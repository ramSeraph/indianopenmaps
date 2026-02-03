#!/usr/bin/env -S uv run 
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "click",
#     "pygithub",
#     "questionary",
# ]
# ///
import json
import base64
import textwrap
from pathlib import Path

import click
import questionary
from github import Auth, Github, InputGitTreeElement


known_repos = [
    "indian_admin_boundaries",
    "indian_water_features",
    "indian_railways",
    "indian_roads",
    "indian_communications",
    "indian_facilities",
    "indian_cadastrals",
    "indian_land_features",
    "india_natural_disasters",
    "indian_buildings",
    "indian_power_infra",
]

gh_api = None
def get_github_api():
    global gh_api

    if gh_api is not None:
        return gh_api

    token_file = Path(__file__).parent / 'token.txt'
    token = token_file.read_text().strip()
    auth = Auth.Token(token)
    gh_api = Github(auth=auth)
    return gh_api


def get_releases_for_repo(repo_name):
    """Get available releases for a specific repository.
    
    Args:
        repo: The selected repository name
        
    Returns:
        List of available releases for the repository
    """
    api = get_github_api()
    user = api.get_user()
    repo = api.get_repo(f"{user.login}/{repo_name}")

    releases = repo.get_releases()
    return [r.tag_name for r in releases]


def find_related_files(base_path: Path) -> dict:
    """Find all related files for a given base path.
    
    Returns dict with keys: 7z, pmtiles, mosaic, parquet, parquet_meta
    Each value is a list of Path objects.
    """
    base_name = base_path.stem
    if base_name.endswith('.geojsonl'):
        base_name = base_name[:-9]
    elif base_name.endswith('.pmtiles'):
        base_name = base_name[:-8]
    elif base_name.endswith('.parquet'):
        base_name = base_name[:-8]
    
    parent_dir = base_path.parent
    
    files = {
        '7z': [],
        'pmtiles': [],
        'mosaic': [],
        'parquet': [],
        'parquet_meta': [],
    }
    
    # Find 7z files (single or split)
    single_7z = parent_dir / f"{base_name}.geojsonl.7z"
    if single_7z.exists():
        files['7z'].append(single_7z)
    else:
        # Look for split 7z files
        files['7z'] = sorted(parent_dir.glob(f"{base_name}.geojsonl.7z.*"))
    
    # Find pmtiles files (single or split)
    single_pmtiles = parent_dir / f"{base_name}.pmtiles"
    if single_pmtiles.exists():
        files['pmtiles'].append(single_pmtiles)
    else:
        # Look for split pmtiles files (partXXXX pattern)
        files['pmtiles'] = sorted(parent_dir.glob(f"{base_name}-part*.pmtiles"))
        # Look for mosaic.json
        mosaic_file = parent_dir / f"{base_name}.mosaic.json"
        if mosaic_file.exists():
            files['mosaic'].append(mosaic_file)
    
    # Find parquet files (single or split)
    single_parquet = parent_dir / f"{base_name}.parquet"
    if single_parquet.exists():
        files['parquet'].append(single_parquet)
    else:
        # Look for split parquet files
        files['parquet'] = sorted(parent_dir.glob(f"{base_name}.*.parquet"))
        # Look for parquet meta.json
        parquet_meta = parent_dir / f"{base_name}.parquet.meta.json"
        if parquet_meta.exists():
            files['parquet_meta'].append(parquet_meta)
    
    return files


def update_routes_file(repo_name, release, file_pmtiles, route, description, category, is_mosaic=False, is_partitioned_parquet=False):
    code_repo_name = 'indianopenmaps'
    branch = 'main' 
    file_path = 'server/routes.json'
    gh = get_github_api()
    user = gh.get_user()
    github_repo = gh.get_repo(f"{user.login}/{code_repo_name}")
    
    base_ref = github_repo.get_git_ref(f"heads/{branch}")
    base_sha = base_ref.object.sha
    base_tree = github_repo.get_git_tree(base_sha)
    
    file_content = github_repo.get_contents(file_path, ref=branch)
    
    # Decode content if it's base64 encoded
    if isinstance(file_content.content, bytes):
        file_content = base64.b64decode(file_content.content).decode('utf-8')
    else:
        file_content = base64.b64decode(file_content.content.encode('utf-8')).decode('utf-8')
    
    commit_msg = f'[ROUTE ADDITION] {file_pmtiles.name}'
    route_data = json.loads(file_content)
    
    handler_type = 'mosaic' if is_mosaic else 'pmtiles'
    entry = {
        'handlertype': handler_type,
        'category': category,
        'name': description,
        'url': f'https://github.com/{user.login}/{repo_name}/releases/download/{release}/{file_pmtiles.name}'
    }
    if is_partitioned_parquet:
        entry['partitioned_parquet'] = True
    route_data.update({route: entry})

    content = json.dumps(route_data, indent=2)


    # Create blob with new content
    blob = github_repo.create_git_blob(content, "utf-8")
    element = InputGitTreeElement(
        path=file_path,
        mode='100644',
        type='blob',
        sha=blob.sha
    )
    
    # Create new tree with the updated file
    new_tree = github_repo.create_git_tree([element], base_tree)
    
    # Create commit
    parent = github_repo.get_git_commit(base_sha)
    commit = github_repo.create_git_commit(
        commit_msg,
        new_tree,
        [parent]
    )
    
    base_ref.edit(commit.sha)

def upload_file(repo_name, release, file):
    gh = get_github_api()
    user = gh.get_user()
    github_repo = gh.get_repo(f"{user.login}/{repo_name}")
    release_obj = github_repo.get_release(release)
    release_obj.upload_asset(
        path=str(file),
        content_type='application/octet-stream',
        name=file.name
    )

def update_release_doc(repo_name, release, description, files_7z, route, source, source_url):
    gh = get_github_api()
    user = gh.get_user()
    github_repo = gh.get_repo(f"{user.login}/{repo_name}")
    release_obj = github_repo.get_release(release)
    body = release_obj.raw_data['body']

    # Build the header with file links
    if len(files_7z) == 1:
        # Single file
        file_7z = files_7z[0]
        header = f"[{file_7z.name}](https://github.com/{user.login}/{repo_name}/releases/download/{release}/{file_7z.name})"
    else:
        # Split files - get base name and create part links
        base_name = files_7z[0].name.rsplit('.', 1)[0]  # Remove .001 etc
        parts = []
        for i, f in enumerate(files_7z, 1):
            url = f"https://github.com/{user.login}/{repo_name}/releases/download/{release}/{f.name}"
            parts.append(f"[Part {i}]({url})")
        header = f"{base_name}: {', '.join(parts)}"

    to_append = f"""
    ### {header}
    - Description: {description}
    - Source: {source} - {source_url}
    - License: [CC0 1.0 but attribute datameet and the original government source where possible](https://github.com/ramSeraph/indianopenmaps/blob/main/DATA_LICENSE.md)
    - Tiles - https://indianopenmaps.fly.dev{route}{{z}}/{{x}}/{{y}}.pbf - [view](https://indianopenmaps.fly.dev{route}view)
    """

    to_append = textwrap.dedent(to_append)

    new_body = body + '\n\n' + to_append
    release_obj.update_release(release_obj.title, new_body)


 
class QuestionaryOption(click.Option):

    def __init__(self, param_decls=None, **attrs):
        click.Option.__init__(self, param_decls, **attrs)
        if not isinstance(self.type, click.Choice):
            raise Exception('ChoiceOption type arg must be click.Choice')

    def prompt_for_value(self, ctx):
        val = questionary.select(self.prompt, choices=self.type.choices).unsafe_ask()
        return val

class RepositoryReleases(click.Choice):
    def __init__(self):
        self.choices = []  # Initialize with empty choices
        self.choices_filled = False
        super().__init__(self.choices)
        
    def update_choices(self, repo: str):
        if self.choices_filled:
            return
        self.choices = tuple(get_releases_for_repo(repo))
        self.choices_filled = True


class DynamicReleaseOption(QuestionaryOption):
    def prompt_for_value(self, ctx):
        # Get the repository value from the context
        repo = ctx.params.get('repo')
        if not repo:
            raise click.UsageError('Repository must be selected before release')

        # Update choices based on selected repository
        if isinstance(self.type, RepositoryReleases):
            self.type.update_choices(repo)

        return super().prompt_for_value(ctx)

    def consume_value(self, ctx, opts):
        repo = ctx.params.get('repo')
        if not repo:
            raise click.UsageError('Repository must be selected before release')

        if isinstance(self.type, RepositoryReleases):
            self.type.update_choices(repo)

        return super().consume_value(ctx, opts)



@click.command()
@click.option('--repo', type=click.Choice(known_repos), prompt='Repository', help='Repository to upload to', show_choices=False, cls=QuestionaryOption)
@click.option('--release', type=RepositoryReleases(), prompt='Release', help='Release in repo to upload to', show_choices=False, cls=DynamicReleaseOption)
@click.option('--base-file', type=str, help='base file (geojsonl.7z or pmtiles) to find related files from')
@click.option('--description', type=str, help='description')
@click.option('--source', type=str, help='source')
@click.option('--source-url', type=str, help='source url')
@click.option('--route', type=str, help='tile route')
@click.option('--category', type=str, help='category', multiple=True)
@click.option('--no-uploads', is_flag=True, default=False)
def main(repo, release, base_file, description, source, source_url, route, category, no_uploads):
    
    if base_file is None:
        raise click.UsageError('--base-file is required')
    
    base_path = Path(base_file)
    if not base_path.exists():
        raise click.UsageError(f'{base_file} does not exist')
    
    # Find all related files
    related = find_related_files(base_path)
    
    # Validate we have the minimum required files
    if not related['7z']:
        raise click.UsageError('No 7z files found')
    if not related['pmtiles'] and not related['mosaic']:
        raise click.UsageError('No pmtiles files found')
    
    # Determine if this is a mosaic (split pmtiles)
    is_mosaic = len(related['mosaic']) > 0
    
    # Determine if parquet is partitioned
    is_partitioned_parquet = len(related['parquet_meta']) > 0
    
    # Get the main file for route registration
    if is_mosaic:
        route_file = related['mosaic'][0]
    else:
        route_file = related['pmtiles'][0]
    
    if not no_uploads:
        # Upload all 7z files
        for f in related['7z']:
            print(f'uploading file {f}')
            upload_file(repo, release, f)
        
        # Upload all pmtiles files
        for f in related['pmtiles']:
            print(f'uploading file {f}')
            upload_file(repo, release, f)
        
        # Upload mosaic.json if present
        for f in related['mosaic']:
            print(f'uploading file {f}')
            upload_file(repo, release, f)
        
        # Upload all parquet files
        for f in related['parquet']:
            print(f'uploading file {f}')
            upload_file(repo, release, f)
        
        # Upload parquet meta.json if present
        for f in related['parquet_meta']:
            print(f'uploading file {f}')
            upload_file(repo, release, f)
    
    # Print summary
    print(f"\nFiles to upload:")
    print(f"  7z: {[f.name for f in related['7z']]}")
    print(f"  pmtiles: {[f.name for f in related['pmtiles']]}")
    print(f"  mosaic: {[f.name for f in related['mosaic']]}")
    print(f"  parquet: {[f.name for f in related['parquet']]}")
    print(f"  parquet_meta: {[f.name for f in related['parquet_meta']]}")
    print(f"  Route file: {route_file.name} (mosaic: {is_mosaic}, partitioned_parquet: {is_partitioned_parquet})")
    
    # append to release doc
    print('updating release notes')
    update_release_doc(repo, release, description, related['7z'], route, source, source_url)
    # update route
    print('updating routes file')
    update_routes_file(repo, release, route_file, route, description, category, is_mosaic, is_partitioned_parquet)


if __name__ == "__main__":
    main()
