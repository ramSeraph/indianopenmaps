import io
import json
import struct
import threading
import time
from functools import partial

import numpy as np
import pytest
import requests
from click.testing import CliRunner
from filelock import FileLock
from PIL import Image

from iomaps.commands.search import search
from iomaps.commands.sources import filter_sources
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


def select_routes(*selected):
    return lambda sources: [s for s in sources if s["route"] in selected]


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
    assert result["coverage"] == {
        "total_sources": 3,
        "indexed_sources": 1,
        "unknown_sources": 2,
        "pending_sources": 0,
        "complete": False,
    }


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
    with pytest.raises(ci.CoverageIndexError, match="force-update"):
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


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        (["--search", "SOI Water Channel Polygons"], [0]),
        (["--search", "polygoms"], [0, 1]),
        (["--search", "/water/soi/channels/"], [0]),
        (["--category", "WATER"], [0]),
        (["--category", "water", "--category", "forests"], [0, 1]),
        (["--category", "water", "--search", "Polygons"], [0]),
        (["--search", "does-not-exist"], []),
    ],
)
def test_cli_filters_limit_downloads_before_spatial_lookup(tmp_path, monkeypatch, filters, expected):
    server = Server(monkeypatch, count=3)
    for i, (name, category) in enumerate(
        [("SOI Water Channel Polygons", "water"), ("Forest Polygons", "forests"), ("Buildings", "buildings")]
    ):
        server.routes[f"/s{i}/"].update(name=name, category=[category])
    server.routes = {
        "/water/soi/channels/": server.routes["/s0/"],
        "/s1/": server.routes["/s1/"],
        "/s2/": server.routes["/s2/"],
    }
    server.bodies[server.url] = json.dumps(server.routes).encode()
    args = ["--bounds", "75.08,12.10,75.24,12.28", "--routes-url", server.url, "--cache-dir", str(tmp_path), "--json"]
    result = CliRunner().invoke(search, [*args, *filters])
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert {url for url, _ in server.calls} == {server.url, *(f"https://data.test/S{i}.coverage.png" for i in expected)}
    assert data["coverage"]["indexed_sources"] == len(expected)
    assert data["coverage"]["pending_sources"] == 3 - len(expected)
    assert data["unknown"] == []

    # Even a partial or empty selection remains offline in a fresh CLI instance.
    def forbidden(*args, **kwargs):
        raise AssertionError("Cached filtered search touched network or SQLite")

    with monkeypatch.context() as offline:
        offline.setattr(ci.requests, "Session", forbidden)
        offline.setattr(ci.sqlite3, "connect", forbidden)
        again = CliRunner().invoke(search, [*args, *filters])
        assert again.exit_code == 0, again.output
        assert json.loads(again.stdout) == data

    server.calls.clear()
    args[1] = "67,5,98,38"
    broader = CliRunner().invoke(search, args)
    assert broader.exit_code == 0, broader.output
    data = json.loads(broader.stdout)
    assert routes(data) == [*server.routes]
    assert data["coverage"]["complete"]
    assert data["coverage"]["pending_sources"] == 0
    assert {url for url, _ in server.calls} == {
        f"https://data.test/S{i}.coverage.png" for i in range(3) if i not in expected
    }


def test_incremental_source_bits_and_aliases_reuse_existing_coverage(tmp_path, monkeypatch):
    server = Server(monkeypatch, count=10)
    server.routes["/alias/"] = {**server.routes["/s9/"], "category": ["alias"]}
    server.bodies[server.url] = json.dumps(server.routes).encode()
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure(source_filter=partial(filter_sources, categories=["alias"]))
    assert routes(index.lookup(INDIA_BOUNDS)) == ["/s9/", "/alias/"]
    assert index.lookup(INDIA_BOUNDS)["coverage"]["pending_sources"] == 9
    assert all(s["pending"] and s["reason"] == "Coverage not fetched" for s in index.lookup(INDIA_BOUNDS)["unknown"])
    server.calls.clear()
    index.ensure()
    assert len(server.calls) == 9
    assert "https://data.test/S9.coverage.png" not in dict(server.calls)
    assert routes(index.lookup(INDIA_BOUNDS)) == [*server.routes]
    # Existing bits move across byte boundaries as earlier catalog sources load.
    assert routes(index.lookup((75, 29, 76, 30))) == ["/s1/", "/s5/", "/s9/", "/alias/"]


def test_known_missing_png_is_not_retried_when_extending(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    missing = "https://data.test/S0.coverage.png"
    del server.bodies[missing]
    index = ci.CoverageIndex(server.url, tmp_path)
    select = select_routes("/s0/")
    index.ensure(source_filter=select)
    assert index.lookup(INDIA_BOUNDS)["coverage"]["pending_sources"] == 1
    server.calls.clear()
    index.ensure(source_filter=select)
    assert not server.calls
    index.ensure()
    assert [url for url, _ in server.calls] == ["https://data.test/S1.coverage.png"]
    assert routes(index.lookup(INDIA_BOUNDS), "unknown") == ["/s0/"]
    server.bodies[missing] = png([(0, 0)])
    index.ensure(force_update=True, source_filter=select)
    assert index.lookup(INDIA_BOUNDS)["coverage"]["complete"]


def test_filtered_refresh_tracks_new_removed_and_changed_sources(tmp_path, monkeypatch):
    server = Server(monkeypatch, count=3)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    server.calls.clear()
    del server.routes["/s1/"]
    server.routes["/s2/"]["coverage_url"] = "https://data.test/replacement.png"
    server.bodies["https://data.test/replacement.png"] = png([(3, 3)])
    server.routes["/new/"] = {"coverage_url": "https://data.test/new.png"}
    server.bodies["https://data.test/new.png"] = png([(3, 3)])
    server.routes["/alias/"] = server.routes["/s0/"]
    server.bodies[server.url] = json.dumps(server.routes).encode()
    server.etags[server.url] = "catalog-v2"
    index.ensure(force_update=True, source_filter=select_routes("/s0/"))
    assert set(dict(server.calls)) == {server.url, "https://data.test/S0.coverage.png"}
    assert dict(server.calls)["https://data.test/S0.coverage.png"] == {"If-None-Match": '"png-0-v1"'}
    result = index.lookup(INDIA_BOUNDS)
    assert routes(result) == ["/s0/", "/alias/"]
    assert routes(result, "unknown") == ["/s2/", "/new/"]
    assert result["coverage"]["pending_sources"] == 2
    server.calls.clear()
    index.ensure()
    assert set(dict(server.calls)) == {"https://data.test/replacement.png", "https://data.test/new.png"}
    assert routes(index.lookup(INDIA_BOUNDS)) == [*server.routes]


def test_filtered_refresh_preserves_unselected_bits_and_missing_checks(tmp_path, monkeypatch):
    server = Server(monkeypatch, count=3)
    del server.bodies["https://data.test/S2.coverage.png"]
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    server.bodies["https://data.test/S0.coverage.png"] = png([(3, 3)])
    server.etags["https://data.test/S0.coverage.png"] = "new-png"
    server.calls.clear()
    index.ensure(force_update=True, source_filter=select_routes("/s0/"))
    assert set(dict(server.calls)) == {server.url, "https://data.test/S0.coverage.png"}
    assert routes(index.lookup((97, 5, 98, 6))) == ["/s0/"]
    assert routes(index.lookup((75, 29, 76, 30))) == ["/s1/"]
    assert routes(index.lookup(INDIA_BOUNDS), "unknown") == ["/s2/"]


@pytest.mark.parametrize("bad_body", [b"invalid PNG", png(size=5)])
def test_failed_extension_preserves_partial_index_and_resumes(tmp_path, monkeypatch, bad_body):
    server = Server(monkeypatch, count=3)
    index = ci.CoverageIndex(server.url, tmp_path)
    select = select_routes("/s0/")
    index.ensure(source_filter=select)
    original = index.path.read_bytes()
    bad = "https://data.test/S2.coverage.png"
    server.bodies[bad] = bad_body
    with pytest.raises(ci.CoverageIndexError):
        index.ensure()
    assert index.path.read_bytes() == original
    server.calls.clear()
    index.ensure(source_filter=select)
    assert not server.calls
    if bad_body.startswith(b"invalid"):
        # Successful downloads from a failed extension are reused on retry.
        server.bodies[bad] = png([(2, 2)])
        index.ensure()
        assert [url for url, _ in server.calls] == [bad]
        assert index.lookup(INDIA_BOUNDS)["coverage"]["complete"]


def test_extension_after_failed_refresh_uses_active_catalog_and_bits(tmp_path, monkeypatch):
    server = Server(monkeypatch, count=3)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure(source_filter=lambda sources: sources[:2])
    original = index.path.read_bytes()
    server.routes = {"/s0/": server.routes["/s0/"], "/new/": {"coverage_url": "https://data.test/invalid.png"}}
    server.bodies[server.url] = json.dumps(server.routes).encode()
    server.etags[server.url] = "new-catalog"
    server.bodies["https://data.test/invalid.png"] = b"invalid PNG"
    server.bodies["https://data.test/S0.coverage.png"] = png([(3, 3)])
    server.etags["https://data.test/S0.coverage.png"] = "new-png"
    with pytest.raises(ci.CoverageIndexError):
        index.ensure(force_update=True)
    assert index.path.read_bytes() == original
    server.calls.clear()
    index.ensure()
    assert [url for url, _ in server.calls] == ["https://data.test/S2.coverage.png"]
    assert routes(index.lookup(INDIA_BOUNDS)) == ["/s0/", "/s1/", "/s2/"]
    assert routes(index.lookup((67, 37, 68, 38))) == ["/s0/"]
    assert routes(index.lookup((97, 5, 98, 6))) == []


def test_failed_index_write_preserves_cache_and_reuses_downloads(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure(source_filter=select_routes("/s0/"))
    original = index.path.read_bytes()

    def fail_write(*args):
        raise OSError("Disk write failed")

    with monkeypatch.context() as failure:
        failure.setattr(ci.os, "fsync", fail_write)
        with pytest.raises(OSError, match="Disk write failed"):
            index.ensure()
    assert index.path.read_bytes() == original
    assert not list(index.directory.glob(".index-*"))
    server.calls.clear()
    index.ensure()
    assert not server.calls
    assert index.lookup(INDIA_BOUNDS)["coverage"]["complete"]


def test_partial_cache_readers_work_during_update_lock(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    select = select_routes("/s0/")
    index.ensure(source_filter=select)
    with FileLock(index.directory / "update.lock", timeout=0):
        index.ensure(source_filter=select)
        assert routes(index.lookup(INDIA_BOUNDS)) == ["/s0/"]
        with pytest.raises(ci.CoverageIndexError, match="Another process"):
            index.ensure()


def test_legacy_full_index_remains_usable_offline(tmp_path, monkeypatch):
    server = Server(monkeypatch)
    index = ci.CoverageIndex(server.url, tmp_path)
    index.ensure()
    blob = index.path.read_bytes()
    length = struct.unpack("<I", blob[8:12])[0]
    header = json.loads(blob[12 : 12 + length])
    header["version"] = 1
    metadata = json.dumps(header).encode()
    index.path.write_bytes(ci.MAGIC + struct.pack("<I", len(metadata)) + metadata + blob[12 + length :])

    def forbidden(*args, **kwargs):
        raise AssertionError("Legacy index touched network or SQLite")

    monkeypatch.setattr(ci.requests, "Session", forbidden)
    monkeypatch.setattr(ci.sqlite3, "connect", forbidden)
    index.ensure(source_filter=select_routes("/s0/"))
    index.ensure()
    assert routes(index.lookup(INDIA_BOUNDS)) == ["/s0/", "/s1/"]
