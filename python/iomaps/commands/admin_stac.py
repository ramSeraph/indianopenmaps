"""Prepare a complete, resumable STAC GeoParquet index of published rasters."""

import csv
import hashlib
import io
import json
import logging
import mimetypes
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

import click
import requests
from shapely.geometry import mapping, shape

logger = logging.getLogger(__name__)


def read_text(location):
    if location.startswith(("https://", "http://")):
        response = requests.get(location, timeout=60)
        response.raise_for_status()
        return response.text
    return Path(location).read_text()


def read_listing(location):
    rows = list(csv.DictReader(io.StringIO(read_text(location))))
    if not rows or not {"name", "url"}.issubset(rows[0]):
        raise ValueError("listing must contain name and url columns and at least one row")
    return rows


def raster_metadata(row, cache, refresh=False):
    """Read headers only; successful reads survive interrupted preparation runs."""
    identity = {key: row.get(key) for key in ("url", "size", "sha256")}
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    cached = cache / f"{key}.json"
    if cached.exists() and not refresh:
        return json.loads(cached.read_text())
    url = row["url"]
    source = "/vsicurl/" + url if url.startswith(("https://", "http://")) else url
    result = subprocess.run(
        [
            "gdalinfo",
            "-json",
            "--config",
            "GDAL_DISABLE_READDIR_ON_OPEN",
            "EMPTY_DIR",
            "--config",
            "GDAL_HTTP_TIMEOUT",
            "30",
            "--config",
            "GDAL_HTTP_MAX_RETRY",
            "3",
            source,
        ],
        capture_output=True,
        text=True,
        timeout=180,
        check=True,
    )
    data = json.loads(result.stdout)
    if not data.get("coordinateSystem") or not data.get("wgs84Extent"):
        raise ValueError(f"{row['name']}: missing georeferencing or WGS84 footprint")
    # Do not retain unrelated GDAL statistics or embedded source XML.
    metadata = {k: data[k] for k in ("size", "geoTransform", "wgs84Extent", "bands", "stac", "metadata") if k in data}
    temporary = cached.with_suffix(".tmp")
    temporary.write_text(json.dumps(metadata))
    temporary.replace(cached)
    return metadata


def parse_datetime(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("datetime must include a timezone")
    return parsed


def reusable_metadata(row, item):
    """Reuse explicitly trusted metadata only for the exact same sheet asset URL."""
    if not item:
        return None
    asset = next((a for a in item["assets"].values() if a["href"] == row["url"]), None)
    props = item["properties"]
    transform, size = props.get("proj:transform"), props.get("proj:shape")
    if not asset or not size or not transform or not (props.get("proj:code") or props.get("proj:epsg")):
        return None
    a, b, c, d, e, f = transform[:6]
    return {
        "size": list(reversed(size)),
        "geoTransform": [c, a, b, f, d, e],
        "wgs84Extent": item["geometry"],
        "stac": props,
        "metadata": {"IMAGE_STRUCTURE": {"LAYOUT": "COG" if "cloud-optimized" in asset.get("type", "") else ""}},
    }


def make_item(row, metadata, collection_id, timestamp, footprint=None, original=None):
    import pystac

    geometry = shape(footprint["geometry"] if footprint else metadata["wgs84Extent"])
    if geometry.is_empty or not geometry.is_valid or geometry.geom_type not in ("Polygon", "MultiPolygon"):
        raise ValueError(f"{row['name']}: invalid footprint; source geometry was not repaired")
    west, south, east, north = geometry.bounds
    if not (-180 <= west <= east <= 180 and -90 <= south <= north <= 90):
        raise ValueError(f"{row['name']}: footprint is outside WGS84 bounds")
    props = {"iomaps:source_properties": footprint.get("properties", {})} if footprint else {}
    item = pystac.Item(Path(row["name"]).stem, mapping(geometry), list(geometry.bounds), timestamp, props)
    item.collection_id = collection_id
    item.add_link(pystac.Link("collection", f"https://indianopenmaps.com/stac/collections/{collection_id}"))
    epsg = metadata.get("stac", {}).get("proj:epsg")
    code = metadata.get("stac", {}).get("proj:code") or (f"EPSG:{epsg}" if epsg else None)
    if code:
        item.stac_extensions.append("https://stac-extensions.github.io/projection/v2.0.0/schema.json")
        item.properties["proj:code"] = code
        item.properties["proj:shape"] = list(reversed(metadata["size"]))
        t = metadata["geoTransform"]
        item.properties["proj:transform"] = [t[1], t[2], t[0], t[4], t[5], t[3], 0, 0, 1]
    cog = metadata.get("metadata", {}).get("IMAGE_STRUCTURE", {}).get("LAYOUT") == "COG"
    item.add_asset(
        "cog" if cog else "data",
        pystac.Asset(
            row["url"],
            media_type=pystac.MediaType.COG if cog else pystac.MediaType.GEOTIFF,
            roles=["data"],
        ),
    )
    if original:
        item.add_asset(
            "original",
            pystac.Asset(
                original["url"],
                roles=["source"],
                media_type=mimetypes.guess_type(original["name"])[0] or "application/octet-stream",
            ),
        )
    return item


def prepare_stac(
    *,
    rows,
    originals,
    footprints,
    output,
    collection_id,
    title,
    description,
    license_id,
    timestamp,
    datetime_description,
    source_url,
    cache,
    workers=4,
    refresh=False,
    reuse_items=None,
):
    import pyarrow.fs  # noqa: F401 -- stac-geoparquet 0.8 needs this module initialized.
    import pystac
    from stac_geoparquet.arrow import parse_stac_items_to_arrow, to_parquet

    tiffs = [r for r in rows if r["name"].lower().endswith((".tif", ".tiff"))]
    ids = [Path(r["name"]).stem for r in tiffs]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("TIFF listing is empty or contains duplicate sheet IDs")
    if footprints is not None and set(ids) - footprints.keys():
        raise ValueError(f"Missing footprints for {len(set(ids) - footprints.keys())} TIFFs")
    cache.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    items, failures = {}, []
    reused = {}
    for row in tiffs:
        item_id = Path(row["name"]).stem
        item = (reuse_items or {}).get(item_id)
        metadata = reusable_metadata(row, item)
        if metadata and not refresh:
            reused[item_id] = metadata

    def read_metadata(row):
        return reused.get(Path(row["name"]).stem) or raster_metadata(row, cache, refresh)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(read_metadata, row): row for row in tiffs}
        for count, future in enumerate(as_completed(futures), 1):
            row = futures[future]
            item_id = Path(row["name"]).stem
            try:
                items[item_id] = make_item(
                    row,
                    future.result(),
                    collection_id,
                    timestamp,
                    footprints.get(item_id) if footprints is not None else None,
                    originals.get(item_id),
                )
            except Exception as exc:
                failures.append({"id": item_id, "url": row["url"], "error": str(exc)})
                logger.error("%s: %s", item_id, exc)
            if count % 100 == 0 or count == len(tiffs):
                logger.info("Read %s/%s sheets (%s failures)", count, len(tiffs), len(failures))
    report = {
        "expected": len(tiffs),
        "prepared": len(items),
        "reused_headers": len(reused),
        "failures": failures,
        "missing_originals": sorted(set(ids) - originals.keys()) if originals else [],
        "datetime": timestamp.isoformat(),
        "datetime_description": datetime_description,
    }
    output.with_suffix(".report.json").write_text(json.dumps(report, indent=2) + "\n")
    if failures:
        raise ValueError(
            f"{len(failures)} sheets failed; no index written. Fix failures and rerun using the same cache."
        )
    ordered = [items[key] for key in sorted(items)]
    extent = pystac.Extent.from_items(ordered)
    collection = pystac.Collection(collection_id, description, extent, title=title, license=license_id)
    collection.extra_fields["iomaps:datetime_description"] = datetime_description
    collection.extra_fields["item_count"] = len(ordered)
    collection.add_link(pystac.Link("derived_from", source_url, title="Source"))
    collection.add_asset(
        "index", pystac.Asset(output.name, media_type="application/vnd.apache.parquet", roles=["metadata"])
    )
    collection_dict = collection.to_dict()
    temporary = output.with_suffix(".tmp.parquet")
    to_parquet(
        parse_stac_items_to_arrow(ordered, drop_invalid_properties=False),
        temporary,
        collections={collection_id: collection_dict},
        compression="snappy",
    )
    temporary.replace(output)
    collection_path = output.with_suffix(".collection.json")
    collection_path.write_text(json.dumps(collection_dict, indent=2) + "\n")
    logger.info("Prepared %s items: %s and %s", len(ordered), output, collection_path)
    return report


@click.command("stac")
@click.option("--listing", help="Local/HTTP CSV with name,url and optional size/sha256 columns")
@click.option("--repo", help="GitHub owner/repository (uses the release's listing_files.csv)")
@click.option("--release", help="Existing georeferenced release tag")
@click.option("--original-listing", help="Local/HTTP CSV of original sheets, matched by basename")
@click.option("--original-release", help="Original release tag in --repo")
@click.option("--bounds", help="Existing WGS84 GeoJSON footprints, matched by feature id or properties.id")
@click.option("--collection-id", required=True)
@click.option("--title", required=True)
@click.option("--description", required=True)
@click.option("--license", "license_id", required=True, help="SPDX identifier, proprietary, or other; never inferred")
@click.option("--datetime", "timestamp", required=True, help="Explicit timezone-qualified date for these items")
@click.option(
    "--datetime-description", required=True, help="What the date represents, e.g. map edition or derivative publication"
)
@click.option("--source-url", required=True)
@click.option("--output", required=True, type=click.Path(dir_okay=False, path_type=Path))
@click.option("--cache-dir", type=click.Path(file_okay=False, path_type=Path))
@click.option("--workers", default=4, show_default=True, type=click.IntRange(1, 16))
@click.option("--refresh", is_flag=True, help="Refresh cached TIFF headers")
@click.option(
    "--reuse-index",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Trust header metadata in an existing index for unchanged asset URLs; --refresh bypasses it",
)
def stac(
    listing,
    repo,
    release,
    original_listing,
    original_release,
    bounds,
    collection_id,
    title,
    description,
    license_id,
    timestamp,
    datetime_description,
    source_url,
    output,
    cache_dir,
    workers,
    refresh,
    reuse_index,
):
    """Prepare a STAC index and collection metadata locally, without publishing."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", collection_id):
        raise click.UsageError("Invalid collection ID")
    if output.suffix != ".parquet":
        raise click.UsageError("--output must end in .parquet")
    if bool(repo) != bool(release) or (not listing and not repo) or (listing and repo):
        raise click.UsageError("Choose --listing or both --repo and --release")
    if original_listing and original_release or original_release and not repo:
        raise click.UsageError("Choose --original-listing or --original-release with --repo")
    if repo and not re.fullmatch(r"[\w.-]+/[\w.-]+", repo):
        raise click.UsageError("--repo must be owner/repository")
    prefix = f"https://github.com/{repo}/releases/download/"
    if repo:
        listing = f"{prefix}{quote(release, safe='')}/listing_files.csv"
        bounds = bounds or f"{prefix}{quote(release, safe='')}/bounds.geojson"
    if original_release:
        original_listing = f"{prefix}{quote(original_release, safe='')}/listing_files.csv"
    try:
        originals = {}
        for row in read_listing(original_listing) if original_listing else []:
            key = Path(row["name"]).stem
            if key in originals:
                raise ValueError(f"Duplicate original sheet ID: {key}")
            originals[key] = row
        footprints = None
        if bounds:
            footprints = {}
            for feature in json.loads(read_text(bounds))["features"]:
                key = str(feature.get("id") or feature.get("properties", {}).get("id", ""))
                if not key or key in footprints:
                    raise ValueError(f"Missing or duplicate footprint ID: {key}")
                footprints[key] = feature
        prepare_stac(
            rows=read_listing(listing),
            originals=originals,
            footprints=footprints,
            output=output,
            collection_id=collection_id,
            title=title,
            description=description,
            license_id=license_id,
            timestamp=parse_datetime(timestamp),
            datetime_description=datetime_description,
            source_url=source_url,
            cache=cache_dir or output.parent / "stac-cache",
            workers=workers,
            refresh=refresh,
            reuse_items=_read_reuse_index(reuse_index, collection_id) if reuse_index else None,
        )
    except (ValueError, OSError, requests.RequestException) as exc:
        raise click.ClickException(str(exc)) from exc


def _read_reuse_index(path, collection_id):
    from iomaps.core.stac_client import RasterCatalog

    catalog = RasterCatalog(indexes={collection_id: str(path)})
    items = list(catalog.search())
    if len({item["id"] for item in items}) != len(items):
        raise ValueError("Reuse index contains duplicate IDs")
    return {item["id"]: item for item in items}
