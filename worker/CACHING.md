# Worker caching

## HTTP responses

TileJSON and COG-info responses use `Cache-Control: no-cache`; tile responses use
`public, max-age=3600, must-revalidate` by default. Both include content ETags and return
`304 Not Modified` for matching `If-None-Match` requests. Revalidation still reads
or renders the current response before comparing its ETag; it saves response
bandwidth, not all upstream reads or rendering work. Errors use `no-store`.

## Configuration and rollback

Configure the shared period in [`worker/wrangler.toml`](wrangler.toml) and push:

```toml
[vars]
CACHE_REVALIDATION_SECONDS = "3600"
```

This setting controls source metadata, STAC index refresh, and browser tile
freshness (one hour by default). Fresh browser-cached tiles avoid Worker requests;
conditional requests still run the Worker even when they return 304. Missing or
invalid values use the default; durations must be non-negative whole seconds.

Set `CACHE_REVALIDATION_SECONDS` to `"0"` to disable periodic source/STAC refresh
and restore the previous tile/TileJSON/COG response policy: long response lifetimes,
no conditional responses, and no viewer URL migration on the next page load.
The old `TILE_CACHE_REVALIDATION` boolean is replaced by this duration setting.
No preview mode or package release is needed.

## PMTiles and mosaic metadata

On the next request after the configured interval, each Worker instance revalidates its
`mosaic.json` with `If-None-Match` (or downloads it again if no ETag was supplied).
An unchanged manifest retains its resolver; a changed manifest is validated before
replacing it. Single-archive PMTiles metadata refreshes at the same interval,
using the library's ETag mismatch recovery. Failed refreshes retain the last valid
metadata and retry after 30 seconds or the upstream `Retry-After`, whichever is
longer. The source and browser freshness windows can add together, so a manifest
change can take up to roughly two hours with the default to appear on subsequent
tile requests, provided upstream revalidation succeeds. These are maximum freshness
windows, not guaranteed cache retention: a new isolate or eviction requires a new read.

## COG sources

COG sources use the same refresh interval, with conditional HEAD requests.
Cached byte ranges are keyed by the source's strong ETag and size; network range
reads use `If-Match`. If the source changes during rendering, the Worker reopens
the entire reader and retries once, keeping TIFF metadata, masks and image blocks
from the same version. Sources without a strong ETag skip the shared byte cache.
The same rollback switch disables COG source revalidation too.

The COG reader uses a shared 32 MiB compressed-range cache containing only
completed reads; readers and fetch queues belong to individual requests.

## STAC indexes

STAC's decoded search indexes and Parquet metadata use the source refresh interval
as well. Their memory budgets and the ETag/revision-keyed byte-range cache remain
independent of this interval.

## Viewer cache migration

Our viewers append a fixed `iomaps-cache=2` query parameter to metadata and tile
requests to bypass responses previously cached for 1,000 days. This is a one-time
migration, not a per-publication version: release asset filenames and API paths
stay the same. Other clients holding old responses need a forced reload or their
own cache-busting query parameter once. Already displayed tiles are not pushed
updates; reload the page or map source to request them again.
