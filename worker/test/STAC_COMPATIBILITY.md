# STAC client compatibility

The API implements STAC API 1.0 Core, Collections, OGC API Features and Item Search.
Items/collections use STAC 1.1. Fields, Sort, Query and Filter extensions are not
advertised. Unsupported search parameters return 400 rather than being ignored.

Checks on 2026-11-02 used the real 50k release index in workerd:

| Client | Version | Checked |
| --- | --- | --- |
| PySTAC Client | 0.9.1 | Discovery, GET/POST pagination of all 4,449 legacy items without duplicates, late item lookup, bbox/datetime filtering, schema validation |
| Rustac | 0.9.19 | Complete paginated search: 4,449 unique items |
| GDAL CLI | 3.11.4 | STACIT opens a search result and `gdalwarp` extracts a 233 × 256 pixel COG from release imagery |
| STAC Browser | 5.1.0 | Catalog/collection discovery, item metadata, original/COG assets and actual sheet rendering via standard XYZ links |
| stac-api-validator | 0.6.8 | Core, Collections, Features, Item Search and pagination: zero errors |

The validator reports optional recommendations about summaries, link titles and
lowercase identifiers. Sheet identifiers retain their source spelling. Its fixed
20–21° test bbox has no data here. These are warnings, not conformance failures.

The rebuilt 50k index contains 4,517 items, including the 68 previously missing
sheets, and uses supplied footprints rather than replacing them with rectangles.
Its datetime explicitly represents catalog indexing, not map acquisition.

## Repeat the checks

Run a local workerd server against downloaded indexes (from `worker/`):

```bash
node test/helpers/stac_server.mjs india-topo-50k=/absolute/path/to/collection.parquet
```

Run the independent client matrix and validator from the repository root:

```bash
STAC_TEST_URL=http://127.0.0.1:8791/stac python/.venv/bin/pytest python/tests/test_stac_compatibility.py -q
cd python
uv run --with stac-api-validator python tests/check_stac_api.py \
  http://127.0.0.1:8791/stac india-topo-50k --pagination
uv run --with rustac rustac search http://127.0.0.1:8791/stac results.json \
  --collections india-topo-50k --limit 317
```

Use `https://indianopenmaps.com/stac` in place of the local URL to test deployment.
The validator script checks metadata/schema and API behavior without opening every
imagery asset. The separate GDAL test checks real asset access:

```bash
gdalwarp --config GDAL_DISABLE_READDIR_ON_OPEN EMPTY_DIR \
  -te_srs EPSG:4326 -te 68.8 24.3 68.81 24.31 -dstalpha -of COG \
  'STACIT:"https://indianopenmaps.com/stac/search?collections=india-topo-50k&bbox=68.8,24.3,68.81,24.31&limit=1":asset=cog' \
  crop.tif
```

GDAL STACIT defaults to a maximum of 1,000 items; use `-oo MAX_ITEMS=0` with tools
accepting open options when a full large mosaic is intended. This is a client-side
limit, independent of API pagination.

For STAC Browser, open the API root, choose a collection, and open a sheet. Items
with an EPSG:3857 COG advertise a `rel=xyz`, `type=image/png` link using the
[Web Map Links extension](https://github.com/stac-extensions/web-map-links).
STAC Browser 5.1.0 consumes it without a custom tile-server configuration. During
local testing the browser's tile requests were forwarded to the existing deployed
COG renderer; catalog/item requests went to workerd. Outside-sheet tile requests
return 404, and covered tiles return PNGs. The existing renderer supports north-up
EPSG:3857 byte imagery; other rasters remain downloadable/extractable as COGs.

The Python `iomaps.stac.RasterCatalog` is a separate direct-release client. It
reads cached Parquet metadata locally and uses GDAL HTTP ranges for extraction;
it does not depend on this STAC HTTP service. Its regression tests cover cache
refresh/offline behavior, geometry holes, numeric elevation values, uncovered
pixels, ranged reads, and preservation of existing output after errors.

See [GDAL STACIT documentation](https://gdal.org/en/stable/drivers/raster/stacit.html)
and [STAC Browser mapping options](https://github.com/radiantearth/stac-browser/blob/v5.1.0/docs/options.md).
