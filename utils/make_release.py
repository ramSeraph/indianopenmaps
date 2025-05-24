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


def update_routes_file(repo_name, release, file_pmtiles, route, description, category):
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
    route_data.update({route: {
        'handlertype': 'pmtiles',
        'category': category,
        'name': description,
        'url': f'https://github.com/{user.login}/{repo_name}/releases/download/{release}/{file_pmtiles.name}'
    }})

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

def update_release_doc(repo_name, release, description, file_7z, route, source, source_url):
    gh = get_github_api()
    user = gh.get_user()
    github_repo = gh.get_repo(f"{user.login}/{repo_name}")
    release_obj = github_repo.get_release(release)
    body = release_obj.raw_data['body']

    to_append = f"""
    ### [{file_7z.name}](https://github.com/{user.login}/{repo_name}/releases/download/{release}/{file_7z.name})
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
@click.option('--file-7z', type=str, help='the zip file to upload')
@click.option('--file-pmtiles', type=str, help='the pmtiles file to upload')
@click.option('--description', type=str, help='description')
@click.option('--source', type=str, help='source')
@click.option('--source-url', type=str, help='source url')
@click.option('--route', type=str, help='tile route')
@click.option('--category', type=str, help='category', multiple=True)
@click.option('--no-uploads', is_flag=True, default=False)
def main(repo, release, file_7z, file_pmtiles, description, source, source_url, route, category, no_uploads):

    
    if file_7z is not None and not file_7z.endswith('.geojsonl.7z'):
        raise Exception(f'{file_7z} does not end with .geojsonl.7z')

    if file_pmtiles is not None and not file_pmtiles.endswith('.pmtiles'):
        raise Exception(f'{file_pmtiles} does not end with .pmtiles')

    if file_7z is not None and file_pmtiles is None:
        file_pmtiles = file_7z.replace('.geojsonl.7z', '.pmtiles')

    if file_pmtiles is not None and file_7z is None:
        file_7z = file_pmtiles.replace('.pmtiles', '.geojsonl.7z')

    file_7z = Path(file_7z)
    file_pmtiles = Path(file_pmtiles)

    if not file_7z.exists():
        raise Exception(f'{file_7z} missing')

    if not file_pmtiles.exists():
        raise Exception(f'{file_pmtiles} missing')

    if not no_uploads:
        # upload files
        print(f'uploading file {file_7z}')
        upload_file(repo, release, file_7z)
        print(f'uploading file {file_pmtiles}')
        upload_file(repo, release, file_pmtiles)
    # append to release doc
    print('updating release notes')
    update_release_doc(repo, release, description, file_7z, route, source, source_url)
    # update route
    print('updating routes file')
    update_routes_file(repo, release, file_pmtiles, route, description, category)


if __name__ == "__main__":
    main()
