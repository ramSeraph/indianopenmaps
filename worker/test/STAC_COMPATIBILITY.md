# STAC client compatibility

The API implements STAC API 1.0 Core, Collections, OGC API Features and Item Search.
Items/collections use STAC 1.1. Fields, Sort, Query and Filter extensions are not
advertised. Unsupported search parameters return 400 rather than being ignored.

## Direct GitHub access without the HTTP API

The release indexes are STAC GeoParquet, not a proprietary index format. Clients
that support it can query the same published metadata locally and follow its
GitHub GeoTIFF asset URLs. The Indian Open Maps HTTP API is optional for this path.
See the [tested Rustac command, index discovery and client comparison](../../python/README.md#search-release-indexes-with-other-stac-clients).

On 2026-11-02, Rustac **0.9.19** queried the published
[CartoDEM index](https://github.com/ramSeraph/indian_land_features/releases/download/cartodem-30m-v3r1/Bhuvan_CartoDEM_v3r1_EGM2008.stac.parquet)
directly over HTTPS using its DuckDB implementation. The WGS84 bbox
`77.6,13.0,77.61,13.01` returned `cdnd43r` and `cdnd43x`, with collection IDs and
their GitHub COG URLs intact. This independently verifies direct-index search;
it does not exercise the HTTP API or raster pixel extraction.

Clients expecting STAC API `/search` still need the HTTP service. A Parquet URL
cannot replace an API root in PySTAC Client. A browsable static STAC JSON catalog
would require linked item JSON files in addition to the current Parquet indexes
and collection sidecars.

On the same date, deployed requests with `Origin: https://example.org` and a
matching external Referer confirmed `Access-Control-Allow-Origin: *` on `/stac`
and a successful `/cog-info` response. A 16-byte `/proxy` request returned HTTP 206
and `Content-Range`, but no CORS permission on the successful response. The COG
tile routes also set wildcard CORS in the implementation. See
[browser access and CORS](../../python/README.md#browser-access-and-cors) for the
distinction between metadata, rendered tiles and raw raster bytes.

## HTTP API checks

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

## Deployment verification

The deployed API was checked again after publication:

- [OSM 50k](https://indianopenmaps.com/stac/collections/india-topo-50k): 4,517 items.
- [NHP 25k](https://indianopenmaps.com/stac/collections/india-topo-25k-nhp): 4,582 items.
- PySTAC Client passed GET/POST pagination, late item lookup, bbox/datetime search
  and schema checks for both collections. Rustac retrieved all 9,099 unique
  collection/item pairs in one paginated search.
- The independent validator completed all four conformance groups and pagination
  with zero errors for each collection; the recommendations above remain.
- GDAL STACIT opened the deployed 50k search as a 6,144 × 6,656 raster. Direct-release
  `iomaps` extraction produced a 466 × 566 pixel NHP COG from a small WGS84 bbox.

Publication receipts verified release hashes, collection counts, sample IDs, bbox
search and the deployed asset URLs. Catalog commits:
[50k](https://github.com/ramSeraph/indianopenmaps/commit/69c3de79e7ababf054afc2c297e1a7d6ab8ec768),
[NHP](https://github.com/ramSeraph/indianopenmaps/commit/d28a16fc54a5eefd0c0c4f7dac031421cefbd1de).

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
