# Raster discovery and extraction

Use `iomaps>=0.6.0`. Discovery requires the Python package; extraction also needs
`gdalwarp` on PATH with the COG driver. The `admin` and `ui` extras are unnecessary.
The optional GUI is for vector extraction; use the CLI or Python API for rasters.

## Discover collections and sheets

```bash
iomaps cli stac collections --json
iomaps cli stac search -c india-topo-50k -b '77.6,13.1,77.61,13.11' --json
iomaps cli stac item india-topo-50k 57G_12
```

Substitute a collection ID returned by `collections`. Search accepts repeated
`-c` options and `--max-items`; extraction takes one collection. Raster IDs are
not vector route paths or numeric positions. Item assets contain the actual
release download URLs. Indexed footprints, including holes, determine matches;
a match does not guarantee valid imagery at every pixel in the footprint.

The raster client reads the catalog JSON from raw GitHub, then STAC GeoParquet
indexes and GeoTIFF windows from their release URLs. It does not call the Indian
Open Maps API or read `listing_files.csv`. External clients can either query
`https://indianopenmaps.com/stac` or, if they support STAC GeoParquet, read the
same release indexes directly. Rustac 0.9.19 was verified with:

```bash
uvx --from rustac==0.9.19 rustac search \
  https://github.com/ramSeraph/indian_land_features/releases/download/cartodem-30m-v3r1/Bhuvan_CartoDEM_v3r1_EGM2008.stac.parquet \
  matches.json --bbox 77.6,13.0,77.61,13.01
```

This returns metadata for `cdnd43r` and `cdnd43x`; it does not extract pixels.
Rustac uses DuckDB for Parquet URLs. API clients such as PySTAC Client still need
an HTTP API root; a Parquet URL cannot replace it. The indexes and collection
sidecars are not a complete linked static STAC JSON catalog.

STAC metadata and rendered `/cog-tiles/...` responses allow cross-origin browser
access. Successful raw-file `/proxy` responses omit CORS permission, so another
website's JavaScript cannot read them. This is not authentication or a restriction
on command-line clients. Upstream GitHub asset URLs have their own CORS policy.

`--catalog-url` selects a compatible local/GitHub catalog JSON file containing
the release-index links; it does not accept a STAC HTTP API root. To bypass
catalog discovery, use an explicit local index or release Parquet URL:

```bash
iomaps cli stac \
  --index https://github.com/ramSeraph/india_topo_maps/releases/download/50k-osm-georef/SOI_OSM_50k.stac.parquet \
  --collection-id india-topo-50k search -b '77.6,13.1,77.61,13.11' --json
```

## Cache metadata

The catalog, collection metadata and indexes are cached by default in the
platform's `iomaps/stac` directory. Warm discovery/search makes no HTTP requests.
Caches have no automatic expiry. New catalog entries need a metadata refresh,
not a client upgrade:

```bash
iomaps cli stac --refresh collections
iomaps cli stac --cache-dir ./map-cache search -c india-topo-50k --max-items 1
iomaps cli stac --cache-dir ./map-cache --offline search -c india-topo-50k --max-items 1
```

Group options (`--cache-dir`, `--refresh`, `--offline`, `--index`,
`--collection-id`, `--catalog-url`) precede the subcommand. Refresh updates only
metadata/indexes touched by that command: `--refresh collections` does not
prefetch every index. Refresh a search when its cached index needs updating.
A failed refresh retains the previous valid cache. Offline mode requires cached
or local metadata and rejects extraction because the COGs still require network
access. Do not combine `--offline` and `--refresh`.

## Extract imagery or elevation

```bash
iomaps cli stac extract -c india-topo-50k \
  -b '77.6,13.1,77.61,13.11' -o bengaluru.tif
```

Bounds are WGS84 `west,south,east,north`. Split requests that cross the dateline.
The client selects intersecting sheets, reads remote GeoTIFF windows through
GDAL HTTP ranges, mosaics them and crops to the requested bbox. It writes a COG
in the source CRS/resolution unless `--crs` or `--resolution` overrides them.
Use `--resampling` and `--asset` when needed; inspect command help for accepted
values. `--max-sheets` limits accidental large mosaics. Existing output survives
errors and is replaced only with `--overwrite` after successful completion.

Numeric elevation values retain their type. Inspect output dimensions, CRS,
band type and alpha/mask as well as representative valid values. Respect the
mask when calculating statistics: uncovered pixels must not become measured
zero elevations. Overlaps use ascending sheet-ID order, with later valid pixels
winning. Range reads avoid downloading whole source rasters, but a wide bbox
or many overlapping sheets can still transfer substantial data.

The existing web tile renderer supports north-up EPSG:3857 byte imagery. Other
GeoTIFFs, including EPSG:4326 Float32 DEMs, can still be discovered and extracted
by this client. A missing web preview does not establish missing raster data.

`--datetime` filters the recorded STAC date. Read each collection's
`iomaps:datetime_description`; a release-publication or indexing date does not
establish acquisition, survey or map-edition dates. Preserve source limitations
such as experimental elevation corrections when describing extracted data.

## Python API

```python
from iomaps.stac import RasterCatalog

catalog = RasterCatalog(cache_dir="./map-cache")
collections = catalog.collections()
sheets = list(catalog.search(collections=["india-topo-50k"], bounds=[77.6, 13.1, 77.61, 13.11]))
result = catalog.extract(
    collection="india-topo-50k",
    bounds=[77.6, 13.1, 77.61, 13.11],
    output="bengaluru.tif",
)
```

`RasterCatalog(refresh=True)` and `RasterCatalog(offline=True)` follow the same
cache rules. `RasterCatalog(indexes={"maps": "https://github.com/.../index.parquet"})`
bypasses the catalog. Consult the package README or implementation for additional
arguments instead of substituting PySTAC Client's HTTP API methods.
