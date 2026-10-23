import io
import json
import threading
import time

import numpy as np
import pytest
import requests
from click.testing import CliRunner
from filelock import FileLock
from PIL import Image

from iomaps.commands.search import search
from iomaps.core import coverage_index as ci
from iomaps.core.coverage import INDIA_BOUNDS
from iomaps.core.source_assets import coverage_url, get_parquet_info_from_source


def png(cells=(), size=4, mode="1"):
    with Image.new(mode, (size, size)) as image:
        for x, y in cells:
            image.putpixel((x, y), 255)
        stream = io.BytesIO()
        image.save(stream, format="PNG")
        return stream.getvalue()


def catalog(count=2):
    return {
        f"/s{i}/": {"name": f"Source {i}", "url": f"https://data.test/S{i}.pmtiles", "category": ["plots"]}
        for i in range(count)
    }


class Response:
    def __init__(self, status, body=b"", headers=None):
        self.status_code, self.body, self.headers = status, body, headers or {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def iter_content(self, size):
        yield self.body


class Server:
    def __init__(self, monkeypatch, count=2):
        self.routes = catalog(count)
        self.url = "https://catalog.test/api/routes"
        self.bodies = {self.url: json.dumps(self.routes).encode()}
        self.etags = {self.url: '"catalog-v1"'}
        for i in range(count):
            url = f"https://data.test/S{i}.coverage.png"
            self.bodies[url] = png([(i % 4, i % 4)])
            self.etags[url] = f'"png-{i}-v1"'
        self.calls, self.responses, self.failures = [], [], {}
        self.lock = threading.Lock()
        self.active = self.maximum = 0
        self.delay = 0
        self.barrier = None
        monkeypatch.setattr(ci.requests, "Session", lambda: self)

    def get(self, url, headers, **kwargs):
        with self.lock:
            self.calls.append((url, headers.copy()))
            self.active += 1
            self.maximum = max(self.maximum, self.active)
        try:
            if url != self.url:
                if self.barrier:
                    self.barrier.wait(timeout=10)
                if self.delay:
                    time.sleep(self.delay)
            if url in self.failures:
                failure = self.failures[url]
                if isinstance(failure, Exception):
                    raise failure
                if isinstance(failure, list):
                    failure = failure.pop(0)
                if failure is not None:
                    return failure
            body = self.bodies.get(url)
            etag = self.etags.get(url)
            if body is None:
                response = Response(404)
            elif etag and headers.get("If-None-Match") == etag:
                response = Response(304, headers={"ETag": etag})
            else:
                response = Response(200, body, {"ETag": etag} if etag else {})
            self.responses.append(response.status_code)
            return response
        finally:
            with self.lock:
                self.active -= 1

    def close(self):
        pass


def routes(result, key="matches"):
    return [s["route"] for s in result[key]]


@pytest.mark.parametrize("suffix", [".parquet", ".pmtiles", ".mosaic.json", ".parquet.meta.json"])
def test_common_dataset_basename_and_override(suffix):
    source = {"url": f"https://host/a.parquet/Dataset%20Name{suffix}?token=test", "partitioned_parquet": True}
    assert coverage_url(source) == "https://host/a.parquet/Dataset%20Name.coverage.png?token=test"
    assert get_parquet_info_from_source(source)["meta_url"].endswith("Dataset%20Name.parquet.meta.json?token=test")
    source["coverage_url"] = "special.png"
    assert coverage_url(source, "file:///tmp/catalog.json") == "file:///tmp/special.png"
    source["type"] = "raster"
    assert coverage_url(source) is None


def test_query_matches_closed_cells_and_north_first_storage(tmp_path):
    # One source per cell, including more than one byte of source bits.
    data = {}
    for i in range(16):
        name = f"S{i}.coverage.png"
        (tmp_path / name).write_bytes(png([(i % 4, i // 4)]))
        data[f"/s{i}/"] = {"coverage_url": name}
    path = tmp_path / "routes.json"
    path.write_text(json.dumps(data))
    index = ci.CoverageIndex(path.as_uri(), tmp_path / "cache")
    index.ensure()
    x = np.linspace(67, 98, 5)
    y = np.linspace(5, 38, 5)
    queries = [(67.1, 37.1, 67.2, 37.2), INDIA_BOUNDS, (66, 4, 68, 6), (98, 5, 99, 6)]
    queries += [(x[a], y[b], x[a + 1], y[b + 1]) for a in range(4) for b in range(4)]
    for bounds in queries:
        expected = []
        for i in range(16):
            col, row = i % 4, 3 - i // 4
            if x[col] <= bounds[2] and x[col + 1] >= bounds[0] and y[row] <= bounds[3] and y[row + 1] >= bounds[1]:
                expected.append(f"/s{i}/")
        assert routes(index.lookup(bounds)) == expected


def test_raw_png_blob_and_warm_lookup_never_open_sqlite_or_network(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    expected = index.lookup(INDIA_BOUNDS)
    with ci.closing(ci.sqlite3.connect(index.directory / "downloads.sqlite3")) as db:
        row = db.execute(
            "SELECT body, etag FROM resources WHERE url = ?", ("https://data.test/S0.coverage.png",)
        ).fetchone()
    assert row == (server.bodies["https://data.test/S0.coverage.png"], '"png-0-v1"')

    def forbidden(*args, **kwargs):
        raise AssertionError("Warm lookup touched network or SQLite")

    monkeypatch.setattr(ci.requests, "Session", forbidden)
    monkeypatch.setattr(ci.sqlite3, "connect", forbidden)
    index.ensure()
    assert index.lookup(INDIA_BOUNDS) == expected


def test_parallel_downloads_are_bounded_and_deduplicated(tmp_path, monkeypatch):
    server = Server(monkeypatch, count=8)
    server.barrier = threading.Barrier(4)
    server.routes["/alias/"] = server.routes["/s0/"]
    server.bodies[server.url] = json.dumps(server.routes).encode()
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure(workers=4)
    assert server.maximum == 4
    assert len(server.calls) == 9
    assert len(routes(index.lookup(INDIA_BOUNDS))) == 9


def test_explicit_update_uses_etags_and_replaces_removed_changed_sources(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    server.calls.clear()
    server.responses.clear()
    index.ensure(force_update=True)
    assert server.responses == [304, 304, 304]
    assert all("If-None-Match" in headers for _, headers in server.calls)
    server.routes = {"/s1/": server.routes["/s1/"]}
    server.bodies[server.url] = json.dumps(server.routes).encode()
    server.etags[server.url] = '"catalog-v2"'
    server.bodies["https://data.test/S1.coverage.png"] = png([(3, 3)])
    del server.etags["https://data.test/S1.coverage.png"]
    index.ensure(force_update=True)
    assert routes(index.lookup(INDIA_BOUNDS)) == ["/s1/"]
    assert not routes(index.lookup((67, 37, 68, 38)))
    assert routes(index.lookup((97, 5, 98, 6))) == ["/s1/"]
    server.calls.clear()
    index.ensure(force_update=True)
    assert dict(server.calls)["https://data.test/S1.coverage.png"] == {}


def test_failed_refresh_preserves_active_generation_and_partial_bootstrap_resumes(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    bad = "https://data.test/S1.coverage.png"
    server.bodies[bad] = b"not a PNG"
    with pytest.raises(ci.CoverageIndexError, match="Invalid coverage PNG"):
        index.ensure()
    assert not index.path.exists()
    server.calls.clear()
    server.bodies[bad] = png([(3, 3)])
    index.ensure()
    assert [url for url, _ in server.calls] == [bad]
    original = index.path.read_bytes()
    server.bodies[bad] = b"broken replacement"
    server.etags[bad] = '"new"'
    with pytest.raises(ci.CoverageIndexError):
        index.ensure(force_update=True)
    assert index.path.read_bytes() == original
    assert len(routes(index.lookup(INDIA_BOUNDS))) == 2


def test_missing_coverage_is_unknown_but_empty_coverage_is_known(tmp_path, monkeypatch):
    server = Server(monkeypatch, count=3)
    server.bodies["https://data.test/S0.coverage.png"] = png()
    del server.bodies["https://data.test/S1.coverage.png"]
    server.routes["/s2/"]["url"] = "https://data.test/unsupported"
    server.bodies[server.url] = json.dumps(server.routes).encode()
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    result = index.lookup(INDIA_BOUNDS)
    assert result["matches"] == []
    assert routes(result, "unknown") == ["/s1/", "/s2/"]
    assert result["coverage"] == {"total_sources": 3, "indexed_sources": 1, "unknown_sources": 2, "complete": False}


def test_all_unknown_and_empty_catalog(tmp_path, monkeypatch):
    server = Server(monkeypatch, count=1)
    server.bodies.pop("https://data.test/S0.coverage.png")
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    assert routes(index.lookup(INDIA_BOUNDS), "unknown") == ["/s0/"]
    server.bodies[server.url] = b"{}"
    server.etags[server.url] = "empty"
    index.ensure(force_update=True)
    assert index.lookup(INDIA_BOUNDS)["coverage"]["total_sources"] == 0


def test_bad_dimensions_depth_and_truncated_cache_require_explicit_repair(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    before = index.path.read_bytes()
    url = "https://data.test/S1.coverage.png"
    server.bodies[url] = png(size=5)
    server.etags[url] = "size5"
    with pytest.raises(ci.CoverageIndexError, match="dimensions differ"):
        index.ensure(force_update=True)
    assert index.path.read_bytes() == before
    with pytest.raises(ci.CoverageIndexError, match="1-bit"):
        ci.decode_png(png(mode="L"))
    server.bodies[url] = png()
    server.etags[url] = "fixed"
    index.path.write_bytes(before[:-1])
    count = len(server.calls)
    index.ensure()
    with pytest.raises(ci.CoverageIndexError, match="force-update"):
        index.lookup(INDIA_BOUNDS)
    assert len(server.calls) == count
    index.ensure(force_update=True)
    assert index.lookup(INDIA_BOUNDS)["coverage"]["complete"]


def test_retries_honor_retry_after_and_have_a_limit(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    sleeps = []
    monkeypatch.setattr(ci.time, "sleep", sleeps.append)
    server.failures[server.url] = [Response(429, headers={"Retry-After": "2"}), None]
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    assert sleeps == [2]
    original = index.path.read_bytes()
    server.failures[server.url] = requests.Timeout()
    server.calls.clear()
    with pytest.raises(ci.CoverageIndexError, match="timed out"):
        index.ensure(force_update=True)
    assert len(server.calls) == 3
    assert index.path.read_bytes() == original
    server.failures[server.url] = Response(429, headers={"Retry-After": "120"})
    server.calls.clear()
    with pytest.raises(ci.CoverageIndexError, match="retry the update later"):
        index.ensure(force_update=True)
    assert len(server.calls) == 1


def test_refresh_lock_does_not_block_cached_readers(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    with FileLock(index.directory / "update.lock", timeout=0):
        index.ensure()
        assert routes(index.lookup(INDIA_BOUNDS))
        with pytest.raises(ci.CoverageIndexError, match="Another process"):
            index.ensure(force_update=True)


def test_cli_local_catalog_offline_filters_unknown_and_isolated_cache(tmp_path, monkeypatch):
    path = tmp_path / "routes.json"
    path.write_text(
        json.dumps(
            {
                "/plots/": {"name": "Land parcels", "coverage_url": "plots.png", "category": ["plots"]},
                "/missing/": {"name": "Unavailable", "coverage_url": "missing.png", "category": ["plots"]},
                "/raster/": {"type": "raster"},
            }
        )
    )
    image = tmp_path / "plots.png"
    image.write_bytes(png([(0, 0)]))
    monkeypatch.setenv("IOMAPS_CACHE_DIR", str(tmp_path / "cache"))
    args = ["--bounds", "66,37,68,39", "--routes-file", str(path), "--json", "--category", "plots"]
    result = CliRunner().invoke(search, args)
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert routes(data) == ["/plots/"]
    assert routes(data, "unknown") == ["/missing/"]
    assert data["effective_bounds"] == [67, 37, 68, 38]
    assert "clipped" in result.stderr
    assert "index" not in data["matches"][0]
    image.unlink()
    path.unlink()
    offline = CliRunner().invoke(search, args)
    assert offline.exit_code == 0, offline.output
    assert json.loads(offline.stdout) == data
    filtered = CliRunner().invoke(search, [*args, "--search", "parcels"])
    assert json.loads(filtered.stdout)["unknown"] == []
    other = tmp_path / "other.json"
    other.write_text("{}")
    isolated = CliRunner().invoke(search, ["--bounds", "67,5,98,38", "--routes-file", str(other), "--json"])
    assert json.loads(isolated.stdout)["coverage"]["total_sources"] == 0


@pytest.mark.parametrize("bounds", ["0,0,1,1", "nan,5,98,38", "98,5,67,38", "1,2,3", "67,5,181,38"])
def test_cli_bad_bounds_fail_before_download(tmp_path, monkeypatch, bounds):
    def forbidden(*args, **kwargs):
        raise AssertionError("Invalid query reached the cache")

    monkeypatch.setattr(ci.CoverageIndex, "ensure", forbidden)
    result = CliRunner().invoke(search, ["--bounds", bounds, "--cache-dir", str(tmp_path)])
    assert result.exit_code == 1
    assert not list(tmp_path.iterdir())
