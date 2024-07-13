import re
from pathlib import Path
from github import Auth, Github

#body = Path('body.txt').read_text()
#print(body)
#
#lines = body.split('\n')
#for line in lines:
#    line_new = line.replace('https://github.com/ramSeraph/indian_admin_boundaries/blob/main/LICENSE', 'https://github.com/ramSeraph/indianopenmaps/blob/main/DATA_LICENSE.md')
#    #    line_new = re.sub(r'^(.*) (https://indianopenmaps.fly.dev/.+/)(\{z\}/\{x\}/\{y\}.pbf)', r'\g<1> \g<2>\g<3> - [view](\g<2>view)', line)
#    print(line_new)
#exit(0)

def add_view_links(body):
    lines_new = []
    lines = body.split('\n')
    for line in lines:
        line_new = re.sub(r'^(.*) (https://indianopenmaps.fly.dev/.+/)(\{z\}/\{x\}/\{y\}.pbf)', r'\g<1> \g<2>\g<3> - [view](\g<2>view)', line)
        lines_new.append(line_new)
    body_new = '\n'.join(lines_new)
    return body_new

def fix_license_link(body):
    lines_new = []
    lines = body.split('\n')
    for line in lines:
        line_new = line.replace('https://github.com/ramSeraph/indian_admin_boundaries/blob/main/LICENSE', 'https://github.com/ramSeraph/indianopenmaps/blob/main/DATA_LICENSE.md')
        lines_new.append(line_new)
    body_new = '\n'.join(lines_new)
    return body_new

def add_data_links(body, url_prefix):
    lines_new = []
    lines = body.split('\n')
    lines = [ line.strip() for line in lines ]
    for line in lines:
        if line.endswith('.7z:') or line.endswith('.zip:') or line.endswith('.7z'):
            if not line.endswith('.7z'):
                line = line[:-1]
            parts = line.split(' ')
            parts = [ p for p in parts if p != '']
            if len(parts) != 1:
                parts[-1] = f'[{parts[-1]}]({url_prefix}/{parts[-1]})'
                line_new = ' '.join(parts)
            else:
                line_new = f'### [{line}]({url_prefix}/{line})'
            lines_new.append(line_new)
        else:
            lines_new.append(line)
    body_new = '\n'.join(lines_new)
    return body_new


token = Path('token.txt').read_text().strip()

repos = [
    'indian_admin_boundaries',
    'indian_water_features',
    'indian_railways',
    'indian_roads',
    'indian_communications',
    'indian_facilities',
    #'ms_buildings_india',
    #'overture_places_india',
]

auth = Auth.Token(token)
g = Github(auth=auth)
user = g.get_user()
user.login
#print(user.login)

already_done = set()
if Path('done.txt').exists():
    lines = Path('done.txt').read_text().split('\n')
    for line in lines:
        parts = line.strip().split(',')
        if len(parts) != 2:
            continue
        already_done.add((parts[0],parts[1]))


for repo_name in repos:
    repo = g.get_repo(f"{user.login}/{repo_name}")
    print(repo)
    print(repo.name)
    releases = repo.get_releases()
    for release in releases:
        if (repo.name, release.tag_name) in already_done:
            continue
        body = release.raw_data['body']
        #Path('body.txt').write_text(body)
        #print('old:')
        #print(body)
        #exit(0)
        #body_new = add_view_links(body)
        #body_new = fix_license_link(body)
        release_prefix_url = f'https://github.com/{user.login}/{repo_name}/releases/download/{release.tag_name}'
        body_new = add_data_links(body, release_prefix_url)
        #print('new:')
        #print(body_new)
        if body_new == body:
            continue
        release.update_release(release.title, body_new)
        with open('done.txt', 'a') as f:
            f.write(f'{repo.name},{release.tag_name}\n')

