"""STAC-specific documents and checks for the shared publication pipeline."""

import json
import re
from pathlib import Path

import click
import pyarrow.parquet as pq

STAC_FILE = "worker/src/routes/stac_catalog.json"


def merge_catalog(catalog, plan):
    entry = next(iter(plan["route_entry"].values()))
    collection_id = entry["collection"]["id"]
    links = list(catalog["links"])
    matches = [
        i
        for i, link in enumerate(links)
        if link.get("rel") == "child"
        and (link.get("collection", {}).get("id") or link.get("href", "").removeprefix("./").rstrip("/"))
        == collection_id
    ]
    if len(matches) > 1:
        raise ValueError(f"Duplicate STAC collection: {collection_id}")
    if matches:
        links[matches[0]] = entry
    else:
        links.append(entry)
    return {**catalog, "links": links}


def validate_entry(plan, names, prefix):
    route, entry = next(iter(plan["route_entry"].items()))
    collection = entry["collection"]
    collection_id = collection["id"]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", collection_id):
        raise ValueError("invalid STAC collection ID")
    if route != f"/stac/collections/{collection_id}/" or entry["href"] != f"./{collection_id}":
        raise ValueError("STAC route and collection ID differ")
    if collection["type"] != "Collection" or not all(
        collection.get(k) for k in ("title", "description", "license", "extent")
    ):
        raise ValueError("incomplete STAC collection metadata")
    if entry["rel"] != "child" or entry["geoparquet"] not in {prefix + n for n in names if n.endswith(".parquet")}:
        raise ValueError("STAC index must reference a planned Parquet asset")
    sample = plan["stac_sample"]
    if not isinstance(sample["count"], int) or sample["count"] <= 0 or not sample["ids"] or len(sample["bbox"]) != 4:
        raise ValueError("invalid STAC verification sample")
    if collection["item_count"] != sample["count"]:
        raise ValueError("STAC count differs from the verification sample")


def build_stac_plan(*, owner, repo, release, index, collection_path, license_md, notes_md=None, skip_uploads=False):
    from .admin_publish import PLAN_VERSION, describe_asset, validate_plan, wrap_release_notes

    index, collection_path = Path(index).resolve(), Path(collection_path).resolve()
    report_path = index.with_suffix(".report.json")
    if report_path.exists() and json.loads(report_path.read_text()).get("failures"):
        raise click.UsageError("The latest preparation failed; resolve its report before publishing this index")
    collection = json.loads(collection_path.read_text())
    parquet = pq.ParquetFile(index)
    if b"geo" not in (parquet.schema_arrow.metadata or {}):
        raise click.UsageError("STAC publication requires a GeoParquet index with geo metadata")
    rows = parquet.read(columns=["id", "bbox", "assets"]).to_pylist()
    if not rows or len({r["id"] for r in rows}) != len(rows) or len(rows) != collection.get("item_count"):
        raise click.UsageError("Index IDs/count do not match the prepared collection")
    collection_id = collection["id"]
    route = f"/stac/collections/{collection_id}/"
    prefix = f"https://github.com/{owner}/{repo}/releases/download/{release}/"
    entry = {
        "rel": "child",
        "type": "application/json",
        "href": f"./{collection_id}",
        "title": collection["title"],
        "geoparquet": prefix + index.name,
        "collection": collection,
    }
    bbox = rows[0]["bbox"]
    if isinstance(bbox, dict):
        bbox = [bbox[k] for k in ("xmin", "ymin", "xmax", "ymax")]
    ids = list(dict.fromkeys([rows[0]["id"], rows[min(1000, len(rows) - 1)]["id"], rows[-1]["id"]]))
    notes = f"""### STAC: {collection["title"]}
- {collection["description"]}
- Items: {len(rows):,}
- Index: [{index.name}]({prefix}{index.name})
- Collection metadata: [{collection_path.name}]({prefix}{collection_path.name})
- API: https://indianopenmaps.com{route.rstrip("/")}
- License: {license_md}
"""
    if notes_md:
        notes += f"- Notes: {notes_md}\n"
    plan = {
        "schema_version": PLAN_VERSION,
        "kind": "stac",
        "dry_run": True,
        "data_repository": f"{owner}/{repo}",
        "release": release,
        "uploads": [describe_asset(index), describe_asset(collection_path)],
        "skip_uploads": skip_uploads,
        "route_repository": f"{owner}/indianopenmaps",
        "route_branch": "main",
        "route_file": STAC_FILE,
        "route_commit_message": f"[STAC COLLECTION] {collection_id}",
        "route_entry": {route: entry},
        "release_license_markdown": license_md,
        "release_notes_markdown": wrap_release_notes(route, notes),
        "stac_sample": {"count": len(rows), "ids": ids, "bbox": bbox, "assets": rows[0]["assets"]},
    }
    validate_plan(plan)
    return plan


def verify_stac_live(plan, get):
    """Check collection discovery, full count, late IDs, spatial search and assets."""
    route, entry = next(iter(plan["route_entry"].items()))
    base = "https://indianopenmaps.com"
    url = base + route.rstrip("/")
    sample = plan["stac_sample"]
    collection = get(url).json()
    if (
        collection.get("item_count") != sample["count"]
        or collection.get("assets", {}).get("index", {}).get("href") != entry["geoparquet"]
    ):
        raise ValueError("live STAC collection differs from the plan")
    page = get(url + "/items?limit=1").json()
    if page.get("numberMatched") != sample["count"]:
        raise ValueError("live STAC item count differs from the index")
    from urllib.parse import quote

    for item_id in sample["ids"]:
        item = get(url + "/items/" + quote(item_id, safe="")).json()
        if item.get("id") != item_id or item.get("collection") != collection["id"]:
            raise ValueError("live STAC item lookup failed")
        if item_id == sample["ids"][0] and item.get("assets") != sample["assets"]:
            raise ValueError("live STAC item assets differ from the index")
    bbox = ",".join(map(str, sample["bbox"]))
    if not get(url + "/items?limit=1&bbox=" + bbox).json().get("features"):
        raise ValueError("live STAC bbox search returned no matching sheets")
    return {
        "status": "verified",
        "collection_url": url,
        "item_count": sample["count"],
        "sample_ids": sample["ids"],
        "bbox_matches": True,
        "assets_match": True,
    }
