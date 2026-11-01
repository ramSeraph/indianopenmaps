"""STAC discovery and windowed raster extraction for Python and CLI users."""

import hashlib
import json
import math
import os
import shutil
import subprocess
import tempfile
from datetime import UTC, timedelta
from datetime import datetime as DateTime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pyarrow.parquet as pq
import requests
from filelock import FileLock
from platformdirs import user_cache_path
from shapely import from_wkb
from shapely.geometry import box, mapping, shape

DEFAULT_CATALOG_URL = (
    "https://raw.githubusercontent.com/ramSeraph/indianopenmaps/main/worker/src/routes/stac_catalog.json"
)
MAX_INDEX_BYTES = 16 * 1024 * 1024


def time_range(value):
    parts = value.split("/") if "/" in value else [value, value]
    if len(parts) != 2:
        raise ValueError("datetime must be an ISO date/timestamp or interval")

    def parse(part, end):
        if part in ("", ".."):
            return DateTime.max.replace(tzinfo=UTC) if end else DateTime.min.replace(tzinfo=UTC)
        date = DateTime.fromisoformat(part.replace("Z", "+00:00"))
        if date.tzinfo is None:
            date = date.replace(tzinfo=UTC)
        if end and len(part) == 10:
            date += timedelta(days=1, microseconds=-1)
        return date

    start, end = parse(parts[0], False), parse(parts[1], True)
    if start > end:
        raise ValueError("datetime interval is reversed")
    return start, end


def row_to_item(row, collection, index_url):
    reserved = {
        "type",
        "id",
        "geometry",
        "bbox",
        "properties",
        "assets",
        "links",
        "collection",
        "stac_version",
        "stac_extensions",
    }
    props = {**(row.get("properties") or {}), **{k: v for k, v in row.items() if k not in reserved}}
    bbox = row.get("bbox")
    if isinstance(bbox, dict):
        bbox = [bbox[k] for k in ("xmin", "ymin", "xmax", "ymax")]
    geometry = row.get("geometry")
    if isinstance(geometry, bytes):
        geometry = mapping(from_wkb(geometry))
    elif isinstance(geometry, str):
        geometry = json.loads(geometry)
    elif geometry is None and bbox:
        geometry = mapping(box(*bbox))
    assets = {k: {**v, "href": urljoin(index_url, v["href"])} for k, v in (row.get("assets") or {}).items() if v}
    extensions = list(row.get("stac_extensions") or [])
    if props.get("proj:epsg") and not any("/projection/" in url for url in extensions):
        extensions.append("https://stac-extensions.github.io/projection/v1.1.0/schema.json")
    result = {
        "type": "Feature",
        "id": row["id"],
        "collection": collection,
        "stac_version": row.get("stac_version", "1.1.0"),
        "stac_extensions": extensions,
        "geometry": geometry,
        "bbox": bbox,
        "properties": props,
        "assets": assets,
        "links": row.get("links") or [],
    }
    return json.loads(json.dumps(result, default=lambda v: v.isoformat() if isinstance(v, DateTime) else str(v)))


def parse_bounds(bounds):
    values = [float(v) for v in bounds.split(",")] if isinstance(bounds, str) else list(bounds)
    if len(values) != 4 or not all(math.isfinite(v) for v in values):
        raise ValueError("Bounds must be four finite WGS84 coordinates: west,south,east,north")
    west, south, east, north = values
    if not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise ValueError("Bounds must have west < east and south < north within WGS84; split dateline-crossing areas")
    return values


class RasterCatalog:
    """Discover STAC rasters and extract their native data without downloading whole sheets.

    Extracted overlaps are resolved in ascending item-ID order (later sheets win).
    Missing values use the source's nodata/mask; no RGB rendering is applied to DEMs.
    """

    def __init__(
        self, url=DEFAULT_CATALOG_URL, *, indexes=None, timeout=60, cache_dir=None, refresh=False, offline=False
    ):
        """Read a GitHub catalog file or explicit {collection_id: release_index_url}.

        No STAC HTTP API is contacted. Indexes are downloaded once and cached by URL;
        refresh=True fetches them again. All spatial searches run locally.
        """
        self.timeout = timeout
        self.cache_dir = Path(cache_dir) if cache_dir else user_cache_path("iomaps") / "stac"
        self.refresh = refresh
        self.offline = offline
        if offline and refresh:
            raise ValueError("offline and refresh cannot be used together")
        self._paths = {}
        if indexes is not None:
            self.entries = {key: {"geoparquet": str(value), "title": key} for key, value in indexes.items()}
        else:
            if str(url).startswith(("https://", "http://")):
                path = self._cached_download(
                    str(url), ".catalog.json", lambda path: json.loads(path.read_text()), 2 * 1024 * 1024
                )
                catalog = json.loads(path.read_text())
            else:
                catalog = json.loads(Path(url).read_text())
            self.entries = {
                (entry.get("collection", {}).get("id") or entry["href"].removeprefix("./").rstrip("/")): entry
                for entry in catalog["links"]
                if entry.get("rel") == "child" and entry.get("geoparquet")
            }

    def _cached_download(self, url, suffix, validate, limit):
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self.cache_dir / (hashlib.sha256(url.encode()).hexdigest() + suffix)
        with FileLock(str(path) + ".lock"):
            if not self.refresh and path.exists():
                return path
            if self.offline:
                raise ValueError(f"Metadata is not cached for offline use: {url}")
            with requests.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(dir=self.cache_dir, delete=False) as stream:
                    temporary = Path(stream.name)
                    try:
                        size = 0
                        for chunk in response.iter_content(1024 * 1024):
                            size += len(chunk)
                            if size > limit:
                                raise ValueError(f"Metadata exceeds the {limit // (1024 * 1024)} MiB limit")
                            stream.write(chunk)
                        stream.flush()
                        validate(temporary)
                        temporary.replace(path)
                    finally:
                        temporary.unlink(missing_ok=True)
        return path

    def _index_path(self, collection):
        if collection not in self.entries:
            raise ValueError(f"Unknown collection: {collection}")
        if collection in self._paths:
            return self._paths[collection]
        url = self.entries[collection]["geoparquet"]
        if not url.startswith(("https://", "http://")):
            path = Path(url)
        else:
            path = self._cached_download(url, ".parquet", pq.ParquetFile, MAX_INDEX_BYTES)
        self._paths[collection] = path
        return path

    def collections(self):
        result = []
        for collection_id, entry in self.entries.items():
            metadata = entry.get("collection")
            if not metadata:
                parquet = pq.ParquetFile(self._index_path(collection_id))
                embedded = json.loads((parquet.schema_arrow.metadata or {}).get(b"stac-geoparquet", b"{}"))
                candidates = embedded.get("collections", {})
                metadata = candidates.get(collection_id)
                if not metadata and len(candidates) == 1:
                    metadata = next(iter(candidates.values()))
                metadata = metadata or {
                    "id": collection_id,
                    "title": entry.get("title", collection_id),
                    "description": entry.get("title", collection_id),
                    "type": "Collection",
                    "item_count": parquet.metadata.num_rows,
                }
            result.append(
                {
                    **metadata,
                    "id": collection_id,
                    "assets": {
                        **metadata.get("assets", {}),
                        "index": {"href": entry["geoparquet"], "type": "application/vnd.apache.parquet"},
                    },
                }
            )
        return result

    def search(self, *, collections=None, bounds=None, datetime=None, ids=None, max_items=None):
        if max_items is not None and (not isinstance(max_items, int) or max_items < 1):
            raise ValueError("max_items must be a positive integer")
        if isinstance(collections, str):
            collections = [collections]
        if isinstance(ids, str):
            ids = [ids]
        region = box(*parse_bounds(bounds)) if bounds is not None else None
        interval = time_range(datetime) if datetime else None
        returned = 0
        for collection_id in collections if collections is not None else self.entries:
            path = self._index_path(collection_id)
            for batch in pq.ParquetFile(path).iter_batches(batch_size=256):
                for row in batch.to_pylist():
                    item = row_to_item(row, collection_id, self.entries[collection_id]["geoparquet"])
                    if ids is not None and item["id"] not in ids:
                        continue
                    if region is not None and (not item["geometry"] or not region.intersects(shape(item["geometry"]))):
                        continue
                    if interval:
                        p = item["properties"]
                        date = p.get("datetime")
                        if not date and not (p.get("start_datetime") and p.get("end_datetime")):
                            continue
                        start, end = time_range(date or f"{p['start_datetime']}/{p['end_datetime']}")
                        if end < interval[0] or start > interval[1]:
                            continue
                    yield item
                    returned += 1
                    if max_items is not None and returned >= max_items:
                        return

    def item(self, collection, item_id):
        item = next(self.search(collections=[collection], ids=[item_id], max_items=1), None)
        if item is None:
            raise ValueError(f"Item not found: {collection}/{item_id}")
        return item

    def extract(
        self,
        *,
        collection,
        bounds,
        output,
        asset=None,
        datetime=None,
        crs=None,
        resolution=None,
        resampling="near",
        max_sheets=1000,
        overwrite=False,
    ):
        """Search, mosaic and crop matching COG windows into a local Cloud-Optimized GeoTIFF.

        Requires gdalwarp on PATH. Bounds are WGS84; resolution is in output CRS units.
        By default GDAL preserves the first sheet's CRS and chooses native resolution.
        A source whose data or mask cannot be read aborts the operation. Existing output
        is preserved unless overwrite=True and the replacement completes successfully.
        """
        bounds = parse_bounds(bounds)
        if self.offline:
            raise ValueError("Raster extraction requires network access; offline mode supports metadata discovery only")
        output = Path(output)
        if output.exists() and not overwrite:
            raise FileExistsError(f"Output exists: {output}; use overwrite=True to replace it")
        if output.suffix.lower() not in (".tif", ".tiff"):
            raise ValueError("Raster output must end in .tif or .tiff")
        if not shutil.which("gdalwarp"):
            raise ValueError("Raster extraction requires gdalwarp (install GDAL)")
        if max_sheets < 1:
            raise ValueError("max_sheets must be positive")
        if resolution is not None and (not math.isfinite(resolution) or resolution <= 0):
            raise ValueError("resolution must be positive and finite")
        if resampling not in ("near", "bilinear", "cubic", "average", "mode"):
            raise ValueError("Unsupported resampling method")
        items = sorted(
            self.search(collections=[collection], bounds=bounds, datetime=datetime, max_items=max_sheets + 1),
            key=lambda item: item["id"],
        )
        if not items:
            raise ValueError("No raster sheets intersect these bounds")
        if len(items) > max_sheets:
            raise ValueError(f"More than {max_sheets} sheets match; narrow the bounds or raise max_sheets")
        sources = []
        for item in items:
            key = asset or next((k for k in ("cog", "data") if k in item["assets"]), None)
            if key not in item["assets"]:
                raise ValueError(f"{item['id']} has no {asset or 'cog/data'} asset")
            candidate = item["assets"][key]
            if candidate.get("type") and "tiff" not in candidate["type"].lower():
                raise ValueError(f"{item['id']}/{key} is not a GeoTIFF asset")
            href = candidate["href"]
            if urlparse(href).scheme not in ("http", "https"):
                raise ValueError(f"{item['id']}/{key}: expected an HTTP(S) GeoTIFF")
            sources.append("/vsicurl/" + href)
        output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="iomaps-raster-", dir=output.parent) as temporary:
            result_path = Path(temporary) / "result.tif"
            command = [
                "gdalwarp",
                "--config",
                "GDAL_DISABLE_READDIR_ON_OPEN",
                "EMPTY_DIR",
                "--config",
                "GDAL_HTTP_TIMEOUT",
                str(self.timeout),
                "--config",
                "GDAL_HTTP_MAX_RETRY",
                "3",
                "--config",
                "GDAL_CACHEMAX",
                "128",
                "-wm",
                "256",
                "-of",
                "COG",
                "-dstalpha",
                "-co",
                "COMPRESS=DEFLATE",
                "-co",
                "BIGTIFF=IF_SAFER",
                "-r",
                resampling,
                "-te_srs",
                "EPSG:4326",
                "-te",
                *map(str, bounds),
            ]
            if crs:
                command.extend(["-t_srs", crs])
            if resolution is not None:
                command.extend(["-tr", str(resolution), str(resolution)])
            command.extend([*sources, str(result_path)])
            process = subprocess.run(command, capture_output=True, text=True)
            if process.returncode:
                raise RuntimeError(f"Raster extraction failed: {process.stderr.strip()}")
            if overwrite:
                result_path.replace(output)
            else:
                # Atomic creation: never replace a file another process created while we worked.
                os.link(result_path, output)
        return {"output": str(output), "collection": collection, "bounds": bounds, "item_ids": [i["id"] for i in items]}
