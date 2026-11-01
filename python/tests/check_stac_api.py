"""Run the independent STAC API validator without downloading imagery assets.

uv run --with stac-api-validator python tests/check_stac_api.py URL COLLECTION
Asset reads are tested separately through GDAL STACIT and iomaps extraction.
"""

import argparse
import json
from dataclasses import fields

import requests
from shapely.geometry import mapping, shape
from stac_api_validator.validations import QueryConfig, validate_api

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("url")
parser.add_argument("collection")
parser.add_argument("--pagination", action="store_true")
args = parser.parse_args()
response = requests.get(args.url + "/search", params={"collections": args.collection, "limit": 1}, timeout=60)
response.raise_for_status()
geometry = mapping(shape(response.json()["features"][0]["geometry"]).representative_point())
warnings, errors = validate_api(
    root_url=args.url,
    ccs_to_validate=["core", "collections", "features", "item-search"],
    collection=args.collection,
    geometry=json.dumps(geometry),
    auth_bearer_token=None,
    auth_query_parameter=None,
    fields_nested_property=None,
    validate_pagination=args.pagination,
    query_config=QueryConfig(**{field.name: None for field in fields(QueryConfig)}),
    transaction_collection=None,
    headers=None,
    open_assets_urls=False,
)
print(
    json.dumps(
        {"url": args.url, "collection": args.collection, "warnings": list(warnings), "errors": list(errors)}, indent=2
    )
)
raise SystemExit(1 if errors else 0)
