"""Offline bbox lookup with explicitly refreshed, per-catalog coverage caches.

Remote artifacts remain standalone PNGs. A local SQLite download cache makes
interrupted builds resumable; one atomically replaced file contains both the
source registry and the dense cell-to-source bitmask. Readers mmap that same
file descriptor, so a concurrent refresh cannot mix two index generations.
"""

import hashlib
import io
import json
import mmap
import os
import sqlite3
import struct
import tempfile
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import closing
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

import numpy as np
import requests
from filelock import FileLock, Timeout
from PIL import Image
from platformdirs import user_cache_path

from .coverage import INDIA_BOUNDS
from .source_assets import coverage_url

MAGIC = b"IOMAPS1\0"
MAX_RESOURCE = 32 * 1024 * 1024
MAX_INDEX = 512 * 1024 * 1024


class CoverageIndexError(ValueError):
    """An index cannot be safely built or queried."""


def query_bounds(bounds):
    try:
        bounds = tuple(float(v) for v in bounds)
        west, south, east, north = bounds
    except (ValueError, TypeError) as exc:
        raise CoverageIndexError("Bounds must be west,south,east,north") from exc
    if not all(np.isfinite(bounds)) or not (-180 <= west < east <= 180 and -90 <= south < north <= 90):
        raise CoverageIndexError("Bounds must be finite, ordered WGS84 coordinates without antimeridian wrapping")
    iw, ins, ie, inn = INDIA_BOUNDS
    clipped = (max(west, iw), max(south, ins), min(east, ie), min(north, inn))
    if clipped[0] > clipped[2] or clipped[1] > clipped[3]:
        raise CoverageIndexError("Bbox is outside the supported India bounds (67,5,98,38)")
    return bounds, clipped


def decode_png(data):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(data)) as image:
                width, height = image.size
                if image.format != "PNG" or image.mode != "1" or width != height or not 1 <= width <= 10000:
                    raise CoverageIndexError(
                        "Coverage must be a square 1-bit grayscale PNG, at most 10000 pixels per side"
                    )
                image.verify()
            with Image.open(io.BytesIO(data)) as image:
                return np.asarray(image, dtype=np.bool_)
    except (OSError, SyntaxError, ValueError, Image.DecompressionBombError) as exc:
        raise CoverageIndexError(f"Invalid coverage PNG: {exc}") from exc


def parse_catalog(data):
    try:
        routes = json.loads(data)
        if not isinstance(routes, dict):
            raise ValueError("expected an object mapping routes to sources")
        sources = []
        for route, config in routes.items():
            if not isinstance(config, dict):
                raise ValueError(f"invalid source at {route}")
            if config.get("type") == "raster":
                continue
            categories = config.get("category") or []
            if isinstance(categories, str):
                categories = [categories]
            if not isinstance(categories, list) or not all(isinstance(c, str) for c in categories):
                raise ValueError(f"invalid categories at {route}")
            if any(not isinstance(config.get(key, ""), str) for key in ("url", "coverage_url", "name")):
                raise ValueError(f"invalid name or URL at {route}")
            sources.append({**config, "route": route, "category": categories})
        return sources
    except (ValueError, TypeError) as exc:
        raise CoverageIndexError(f"Invalid source catalog: {exc}") from exc


class Downloads:
    def __init__(self, path):
        self.path = path
        self.local = threading.local()
        self.sessions = []
        self.lock = threading.Lock()
        with closing(sqlite3.connect(path)) as db, db:
            db.execute("CREATE TABLE IF NOT EXISTS resources (url TEXT PRIMARY KEY, body BLOB, etag TEXT)")

    def close(self):
        for session in self.sessions:
            session.close()

    def cached(self, url):
        with closing(sqlite3.connect(self.path, timeout=30)) as db:
            return db.execute("SELECT body, etag FROM resources WHERE url = ?", (url,)).fetchone()

    def save(self, url, body, etag):
        # The original PNG bytes and their ETag commit together. No decoded
        # cells live in SQLite; lookup never opens this download database.
        with closing(sqlite3.connect(self.path, timeout=30)) as db, db:
            if body is None:
                db.execute("DELETE FROM resources WHERE url = ?", (url,))
            else:
                db.execute("INSERT OR REPLACE INTO resources VALUES (?, ?, ?)", (url, body, etag))

    def session(self):
        if not hasattr(self.local, "session"):
            self.local.session = requests.Session()
            with self.lock:
                self.sessions.append(self.local.session)
        return self.local.session

    def fetch(self, url, validate, *, refresh, missing_ok=False):
        cached = self.cached(url)
        if cached:
            try:
                validate(cached[0])
            except ValueError:
                cached = None
        if cached and not refresh:
            return cached[0]
        parsed = urlsplit(url)
        if parsed.scheme == "file" and parsed.netloc in ("", "localhost"):
            try:
                with Path(unquote(parsed.path)).open("rb") as stream:
                    data = stream.read(MAX_RESOURCE + 1)
            except FileNotFoundError:
                if not missing_ok:
                    raise CoverageIndexError("Catalog file does not exist") from None
                self.save(url, None, None)
                return None
            if len(data) > MAX_RESOURCE:
                raise CoverageIndexError("Resource exceeds the 32 MiB size limit")
            validate(data)
            self.save(url, data, None)
            return data
        if parsed.scheme not in ("http", "https"):
            raise CoverageIndexError("Catalog and coverage URLs must use HTTP, HTTPS, or local file URLs")
        headers = {"If-None-Match": cached[1]} if cached and cached[1] else {}
        for attempt in range(3):
            delay = 0.5 * 2**attempt
            try:
                with self.session().get(url, headers=headers, timeout=(10, 30), stream=True) as response:
                    status = response.status_code
                    if status == 304:
                        if not cached:
                            raise CoverageIndexError("Server returned 304 without cached content")
                        self.save(url, cached[0], response.headers.get("ETag", cached[1]))
                        return cached[0]
                    if status in (404, 410) and missing_ok:
                        self.save(url, None, None)
                        return None
                    retry = status in (429, 500, 502, 503, 504) or (status == 403 and "Retry-After" in response.headers)
                    if status == 200:
                        data = bytearray()
                        for chunk in response.iter_content(64 * 1024):
                            data.extend(chunk)
                            if len(data) > MAX_RESOURCE:
                                raise CoverageIndexError("Resource exceeds the 32 MiB size limit")
                        data = bytes(data)
                        validate(data)
                        self.save(url, data, response.headers.get("ETag"))
                        return data
                    if not retry:
                        raise CoverageIndexError(f"Download returned HTTP {status}")
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = max(delay, float(retry_after))
                        except ValueError:
                            try:
                                delay = max(
                                    delay, (parsedate_to_datetime(retry_after) - datetime.now(UTC)).total_seconds()
                                )
                            except (ValueError, TypeError, OverflowError):
                                pass
                    error = f"Download returned HTTP {status}"
            except requests.RequestException:
                error = "Network request failed or timed out"
            if attempt == 2 or delay > 60:
                raise CoverageIndexError(f"{error}; retry the update later")
            time.sleep(delay)
        raise AssertionError("unreachable")


class CoverageIndex:
    def __init__(self, catalog_url, cache_dir=None):
        self.catalog_url = catalog_url
        parent = Path(cache_dir) if cache_dir else user_cache_path("iomaps")
        self.directory = parent / hashlib.sha256(catalog_url.encode()).hexdigest()[:24]
        self.path = self.directory / "index.bin"

    def ensure(self, *, force_update=False, workers=8, progress=None):
        if self.path.exists() and not force_update:
            return
        if not 1 <= workers <= 32:
            raise CoverageIndexError("Workers must be between 1 and 32")
        self.directory.mkdir(parents=True, exist_ok=True)
        try:
            with FileLock(self.directory / "update.lock", timeout=0):
                if self.path.exists() and not force_update:
                    return
                downloads = Downloads(self.directory / "downloads.sqlite3")
                try:
                    self._build(downloads, force_update, workers, progress)
                finally:
                    downloads.close()
        except Timeout:
            raise CoverageIndexError("Another process is updating this cache; retry when it finishes") from None

    def _build(self, downloads, refresh, workers, progress):
        if progress:
            progress("Refreshing coverage cache..." if refresh else "Building coverage cache...")
        data = downloads.fetch(self.catalog_url, parse_catalog, refresh=refresh)
        sources = parse_catalog(data)
        records = []
        for source in sources:
            records.append(
                {
                    "route": source["route"],
                    "name": source.get("name") or source["route"],
                    "category": source["category"],
                    "coverage_url": coverage_url(source, self.catalog_url),
                }
            )
        urls = list(dict.fromkeys(s["coverage_url"] for s in records if s["coverage_url"]))
        dimensions, errors = {}, []

        def download(url):
            body = downloads.fetch(url, decode_png, refresh=refresh, missing_ok=True)
            return decode_png(body).shape[0] if body is not None else None

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(download, url): url for url in urls}
            for done, future in enumerate(as_completed(futures), 1):
                url = futures[future]
                try:
                    dimensions[url] = future.result()
                except (ValueError, OSError, sqlite3.Error) as exc:
                    route = next(s["route"] for s in records if s["coverage_url"] == url)
                    errors.append(f"{route}: {exc}")
                if progress and (done % 25 == 0 or done == len(urls)):
                    progress(f"Coverage downloads: {done}/{len(urls)}")
        if errors:
            raise CoverageIndexError("Cache update failed; previous index preserved. " + "; ".join(errors[:5]))
        sizes = {n for n in dimensions.values() if n is not None}
        if len(sizes) > 1:
            raise CoverageIndexError(
                "Coverage PNG dimensions differ; rebuild them on the same grid. Previous index preserved"
            )
        size = next(iter(sizes), 1000)
        indexed = []
        for record in records:
            url = record["coverage_url"]
            if url and dimensions[url] is not None:
                record["bit"] = len(indexed)
                indexed.append(record)
            else:
                record["reason"] = "Coverage PNG missing" if url else "No coverage URL could be derived"
        stride = (len(indexed) + 7) // 8
        if size * size * stride > MAX_INDEX:
            raise CoverageIndexError("Combined index would exceed 512 MiB; use a coarser coverage grid")
        header = {
            "version": 1,
            "catalog_url": self.catalog_url,
            "cached_at": datetime.now(UTC).isoformat(),
            "grid_size": size,
            "sources": records,
        }
        metadata = json.dumps(header, separators=(",", ":")).encode()
        if len(metadata) > MAX_RESOURCE:
            raise CoverageIndexError("Source registry is too large")
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(dir=self.directory, prefix=".index-", delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(MAGIC + struct.pack("<I", len(metadata)) + metadata)
                offset = stream.tell()
                stream.truncate(offset + size * size * stride)
                stream.flush()
                if stride:
                    with mmap.mmap(stream.fileno(), 0) as mapping:
                        matrix = np.ndarray((size, size, stride), dtype=np.uint8, buffer=mapping, offset=offset)
                        try:
                            for byte in range(stride):
                                plane = np.zeros((size, size), dtype=np.uint8)
                                for bit, record in enumerate(indexed[byte * 8 : (byte + 1) * 8]):
                                    body, _ = downloads.cached(record["coverage_url"])
                                    plane |= decode_png(body).astype(np.uint8) << bit
                                matrix[:, :, byte] = plane
                            mapping.flush()
                        finally:
                            del matrix
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
        finally:
            if temporary:
                temporary.unlink(missing_ok=True)

    def lookup(self, bounds):
        bounds, clipped = query_bounds(bounds)
        try:
            with self.path.open("rb") as stream:
                if stream.read(8) != MAGIC:
                    raise ValueError("invalid signature")
                length = struct.unpack("<I", stream.read(4))[0]
                if length > MAX_RESOURCE:
                    raise ValueError("oversized registry")
                header = json.loads(stream.read(length))
                offset = stream.tell()
                size, records = header["grid_size"], header["sources"]
                if header["version"] != 1 or header["catalog_url"] != self.catalog_url or not 1 <= size <= 10000:
                    raise ValueError("unsupported registry")
                indexed = [s for s in records if "bit" in s]
                if [s["bit"] for s in indexed] != list(range(len(indexed))):
                    raise ValueError("invalid source bits")
                stride = (len(indexed) + 7) // 8
                if os.fstat(stream.fileno()).st_size != offset + size * size * stride:
                    raise ValueError("incorrect index length")
                matches = []
                if stride:
                    west, south, east, north = INDIA_BOUNDS
                    x = np.linspace(west, east, size + 1)
                    y = np.linspace(south, north, size + 1)
                    c0 = max(0, int(np.searchsorted(x, clipped[0], side="left")) - 1)
                    c1 = min(size, int(np.searchsorted(x, clipped[2], side="right")))
                    s0 = max(0, int(np.searchsorted(y, clipped[1], side="left")) - 1)
                    s1 = min(size, int(np.searchsorted(y, clipped[3], side="right")))
                    with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_READ) as mapping:
                        matrix = np.ndarray((size, size, stride), dtype=np.uint8, buffer=mapping, offset=offset)
                        try:
                            bits = np.bitwise_or.reduce(matrix[size - s1 : size - s0, c0:c1, :], axis=(0, 1), initial=0)
                            occupied = np.unpackbits(bits, bitorder="little")[: len(indexed)]
                            matches = [s for s, present in zip(indexed, occupied, strict=True) if present]
                        finally:
                            del matrix

                def public(record):
                    return {k: v for k, v in record.items() if k != "bit"}

                return {
                    "bounds": list(bounds),
                    "effective_bounds": list(clipped),
                    "cached_at": header["cached_at"],
                    "grid": {"bounds": list(INDIA_BOUNDS), "width": size, "height": size},
                    "coverage": {
                        "total_sources": len(records),
                        "indexed_sources": len(indexed),
                        "unknown_sources": len(records) - len(indexed),
                        "complete": len(indexed) == len(records),
                    },
                    "matches": [public(s) for s in matches],
                    "unknown": [public(s) for s in records if "bit" not in s],
                }
        except (OSError, ValueError, KeyError, TypeError, struct.error) as exc:
            raise CoverageIndexError(f"Cannot read coverage cache; use --force-update to rebuild it: {exc}") from exc
