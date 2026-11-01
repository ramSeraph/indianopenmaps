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

The Worker uses Hyparquet HTTP range reads. Unfiltered pages read only the
necessary row groups; filtered searches load IDs, geometry, bounds and dates,
then fetch complete records for the selected page. It keeps at most 8 MiB of
search records and 2 MiB of footer metadata. STAC and COG reads share a 32 MiB
cache of completed byte ranges; validated STAC ranges also use the edge cache.
Pending network operations remain request-local. Failed, truncated or ignored
range responses are rejected and invalidated so later requests can retry.

New `iomaps admin stac` indexes use at most 512 records per row group and include
page indexes. Hyparquet 1.25 has a dictionary-decoding problem in its optional
offset-index path, so the Worker currently uses column and row-group pruning.
Other clients can use the page indexes. Older one-row-group files remain readable,
but selecting one item can require decoding that group's columns.

The deployed configuration explicitly uses the Workers Paid 30-second CPU budget.
This does not increase the isolate's 128 MiB memory limit.

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

The expanded catalog contains **17 collections and 53,983 raster items**. The
15 additions contain 44,884 items:

| Added collection | Items |
| --- | ---: |
| Nepal Survey | 619 |
| Nepal JICA | 81 |
| Nepal–China border 1979 | 57 |
| Myanmar 50k | 1,001 |
| American ONC | 281 |
| American TPC | 665 |
| American AMS 250k | 1,130 |
| Soviet 25k | 3,050 |
| Soviet 50k | 9,865 |
| Soviet 100k | 12,972 |
| Soviet 200k | 11,791 |
| Soviet 500k | 2,274 |
| Soviet 1m | 301 |
| Goa CZMP 2019 4k | 254 |
| CartoDEM v3r1 EGM2008 | 543 |

Rustac 0.9.19 paginated the complete final catalog through local workerd and
returned **53,983 unique collection/item pairs**. PySTAC Client 0.9.1 passed
complete GET/POST pagination, late item lookup, bbox/date filters and schema
validation across all 17 collections. These checks use the actual final release
indexes in workerd, separately from the deployed service. STAC API Validator
0.6.8 passed Core, Collections, Features, Item Search and pagination with zero
errors for both Soviet 200k and CartoDEM after correcting combined ID/spatial/date
filters. The optional recommendations about summaries, link titles and source
identifier spelling remain. Rustac also searched
the four final Soviet release indexes directly over GitHub HTTPS, returning the
expected sheets `D47-081` (100k), `A32-06` (200k), `G42-111-1` (50k) and `A17-2`
(500k). The direct `iomaps` client extracted a 23 × 24 pixel EPSG:3857 RGBA COG
from `D47-081`, using the published index and imagery URLs.

The live service still intermittently returns Cloudflare HTTP 503 / Error 1102.
The active version and dashboard both report `cpu_ms: 30000`, while an error
trace reports `exceededCpu` after 19 ms. A matched request succeeded through MAA
over IPv4 and failed through BLR over IPv6, but later IPv4 pagination also failed;
neither IP family is a verified workaround. The root cause is unconfirmed.
Publication of the final collections was authorized with live runtime checks
deferred. Release assets, notes and catalog commits are verified separately;
the publication receipts preserve the skipped/failed live checks. Do not treat
local compatibility results as proof that these deployed runtime failures are
resolved. Rerun the saved plans with verification enabled after investigation.

Release notes record the source audit: stale/missing listing entries, unavailable
original scans, nine missing supplied footprints derived from TIFF extents, and
one invalid Nepal Survey footprint replaced with its TIFF extent.
Goa CZMP and AMS assets are ordinary GeoTIFFs where their headers do not identify
COG layout. Their metadata and download assets remain available; they do not
advertise COG-only viewer links. PMTiles/archive-only datasets and an empty JOG
georeferenced release were excluded from the raster index inventory.

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
