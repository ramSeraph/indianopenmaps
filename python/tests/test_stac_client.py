import json
import shutil
import subprocess
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from shapely.geometry import Polygon, box

from iomaps.commands.stac import stac
from iomaps.core import stac_client
from iomaps.core.stac_client import RasterCatalog


def write_index(path, *, href="https://github.com/example/releases/download/maps/a.tif", geometries=None):
    rows = []
    for index, geometry in enumerate(geometries or [box(0, 0, 4, 4)]):
        rows.append(
            {
                "id": f"sheet-{index}",
                "geometry": geometry.wkb,
                "bbox": list(geometry.bounds),
                "datetime": datetime(2025, 1, index + 1, tzinfo=UTC),
                "proj:code": "EPSG:4326",
                "assets": {"cog": {"href": href, "type": "image/tiff; application=geotiff; profile=cloud-optimized"}},
            }
        )
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def test_direct_index_download_cache_and_refresh_never_call_site(tmp_path, monkeypatch):
    index = write_index(tmp_path / "index.parquet")
    url = "https://github.com/example/releases/download/maps/collection.parquet"
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.iter_content.side_effect = lambda size: iter([index.read_bytes()])
    get = Mock(return_value=response)
    monkeypatch.setattr(stac_client.requests, "get", get)
    options = {"indexes": {"maps": url}, "cache_dir": tmp_path / "cache"}
    catalog = RasterCatalog(**options)
    assert catalog.item("maps", "sheet-0")["properties"]["proj:code"] == "EPSG:4326"
    assert len(list(catalog.search())) == 1
    assert len(list(RasterCatalog(**options).search())) == 1
    assert get.call_count == 1
    list(RasterCatalog(**options, refresh=True).search())
    assert get.call_count == 2
    assert all(call.args[0] == url for call in get.call_args_list)
    response.iter_content.side_effect = lambda size: iter([b"bad parquet"])
    with pytest.raises(pa.ArrowInvalid):
        list(RasterCatalog(**options, refresh=True).search())
    assert len(list(RasterCatalog(**options).search())) == 1, "failed refresh preserves the good cache"


def test_search_uses_footprints_holes_dates_and_all_ids(tmp_path, monkeypatch):
    monkeypatch.setattr(stac_client.requests, "get", Mock(side_effect=AssertionError("Unexpected HTTP")))
    triangle = Polygon([(0, 0), (4, 0), (0, 4)])
    hole = Polygon(box(10, 10, 14, 14).exterior.coords, [box(11, 11, 13, 13).exterior.coords])
    index = write_index(tmp_path / "index.parquet", geometries=[triangle, hole])
    catalog = RasterCatalog(indexes={"maps": str(index)})
    assert list(catalog.search(bounds=[3, 3, 3.5, 3.5])) == []
    assert list(catalog.search(bounds=[11.5, 11.5, 12, 12])) == []
    assert [i["id"] for i in catalog.search(datetime="2025-01-02")] == ["sheet-1"]
    assert catalog.item("maps", "sheet-1")["geometry"]["type"] == "Polygon"
    assert len(list(catalog.search(collections="maps", max_items=1))) == 1
    with pytest.raises(ValueError, match="positive"):
        list(catalog.search(max_items=0))
    with pytest.raises(ValueError, match="not found"):
        catalog.item("maps", "missing")
    result = CliRunner().invoke(stac, ["--index", str(index), "--collection-id", "maps", "search", "--json"])
    assert result.exit_code == 0, result.output
    assert len(json.loads(result.output)["features"]) == 2


def test_catalog_and_collection_metadata_cache_support_offline_use(tmp_path, monkeypatch):
    index = write_index(tmp_path / "index.parquet")
    index_url = "https://github.com/example/releases/download/maps/collection.parquet"
    catalog_url = "https://raw.githubusercontent.com/example/main/catalog.json"
    collection = {"id": "maps", "title": "Cached maps", "item_count": 1}
    payloads = {
        catalog_url: json.dumps(
            {"links": [{"rel": "child", "href": "./maps", "geoparquet": index_url, "collection": collection}]}
        ).encode(),
        index_url: index.read_bytes(),
    }

    def response(url, **kwargs):
        result = Mock()
        result.__enter__ = Mock(return_value=result)
        result.__exit__ = Mock(return_value=False)
        result.iter_content.return_value = iter([payloads[url]])
        return result

    get = Mock(side_effect=response)
    monkeypatch.setattr(stac_client.requests, "get", get)
    options = {"url": catalog_url, "cache_dir": tmp_path / "cache"}
    warm = RasterCatalog(**options)
    assert warm.collections()[0]["title"] == "Cached maps"
    assert len(list(warm.search())) == 1
    assert get.call_count == 2
    get.reset_mock(side_effect=True)
    get.side_effect = AssertionError("Offline discovery made a network request")
    offline = RasterCatalog(**options, offline=True)
    assert offline.collections()[0]["title"] == "Cached maps"
    assert len(list(offline.search())) == 1
    assert len(list(RasterCatalog(**options).search())) == 1, "normal warm reads also avoid HTTP"
    get.assert_not_called()
    with pytest.raises(ValueError, match="not cached"):
        list(
            RasterCatalog(indexes={"uncached": index_url + "?new"}, cache_dir=tmp_path / "cache", offline=True).search()
        )
    with pytest.raises(ValueError, match="cannot be used together"):
        RasterCatalog(**options, offline=True, refresh=True)
    with pytest.raises(ValueError, match="network access"):
        offline.extract(collection="maps", bounds=[0, 0, 1, 1], output=tmp_path / "offline.tif")
    get.side_effect = response
    list(RasterCatalog(**options, refresh=True).search())
    assert get.call_count == 2


@pytest.fixture
def range_server(tmp_path):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_HEAD(self):
            self.serve(False)

        def do_GET(self):
            self.serve(True)

        def serve(self, body):
            file = tmp_path / self.path.lstrip("/")
            if not file.is_file():
                self.send_error(404)
                return
            size = file.stat().st_size
            start, end = 0, size - 1
            requested = self.headers.get("Range")
            if requested:
                start, end = map(int, requested.removeprefix("bytes=").split("-"))
                end = min(end, size - 1)
            self.send_response(206 if requested else 200)
            self.send_header("Content-Length", str(end - start + 1))
            self.send_header("Accept-Ranges", "bytes")
            if requested:
                self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
            self.end_headers()
            if body:
                calls.append((self.path, requested, end - start + 1))
                with file.open("rb") as stream:
                    stream.seek(start)
                    self.wfile.write(stream.read(end - start + 1))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def gdal(*args):
    return subprocess.run(list(map(str, args)), check=True, capture_output=True, text=True).stdout


@pytest.mark.skipif(not shutil.which("gdalwarp"), reason="GDAL CLI required")
def test_extract_reads_windows_preserves_float_values_and_masks_gaps(tmp_path, range_server):
    base, calls = range_server
    raw, cog = tmp_path / "raw.tif", tmp_path / "source.tif"
    gdal(
        "gdal_create",
        "-outsize",
        2048,
        2048,
        "-a_ullr",
        0,
        4,
        4,
        0,
        "-a_srs",
        "EPSG:4326",
        "-ot",
        "Float32",
        "-burn",
        42.5,
        "-a_nodata",
        -9999,
        raw,
    )
    gdal("gdal_translate", "-of", "COG", "-co", "COMPRESS=NONE", raw, cog)
    index = write_index(tmp_path / "index.parquet", href=base + "/source.tif")
    catalog = RasterCatalog(indexes={"maps": str(index)})
    output = tmp_path / "crop.tif"
    result = catalog.extract(
        collection="maps", bounds=[-0.125, 1, 0.125, 1.25], output=output, resolution=1 / 512, crs="EPSG:4326"
    )
    assert result["item_ids"] == ["sheet-0"]
    info = json.loads(gdal("gdalinfo", "-json", output))
    assert info["size"] == [128, 128]
    assert info["bands"][0]["type"] == "Float32"
    assert info["metadata"]["IMAGE_STRUCTURE"]["LAYOUT"] == "COG"
    assert float(gdal("gdallocationinfo", "-valonly", "-b", 1, output, 96, 64)) == 42.5
    assert float(gdal("gdallocationinfo", "-valonly", "-b", 2, output, 32, 64)) == 0
    assert float(gdal("gdallocationinfo", "-valonly", "-b", 2, output, 96, 64)) > 0
    reads = [call for call in calls if call[0] == "/source.tif"]
    assert reads and all(call[1] for call in reads), "COGs must use HTTP range reads"
    assert sum(call[2] for call in reads) < cog.stat().st_size / 2
    previous = output.read_bytes()
    with pytest.raises(FileExistsError):
        catalog.extract(collection="maps", bounds=[0, 1, 1, 2], output=output)
    missing = write_index(tmp_path / "missing.parquet", href=base + "/missing.tif")
    with pytest.raises(RuntimeError, match="failed"):
        RasterCatalog(indexes={"maps": str(missing)}).extract(
            collection="maps", bounds=[0, 1, 1, 2], output=output, overwrite=True
        )
    assert output.read_bytes() == previous
