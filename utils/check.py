import json
from pathlib import Path

script_dir = Path(__file__).parent

routes_file = script_dir / '..' / 'server' / 'routes.json'

data = json.loads(routes_file.read_text())

for p, info in data.items():
    if not p.startswith('/'):
        print(f'{p} missing / infront')

