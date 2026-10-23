"""Shared dataset basename conventions for tiles, Parquet and coverage."""

from urllib.parse import urljoin, urlsplit, urlunsplit


def replace_asset_suffix(url, suffix):
    """Replace only the final asset suffix, retaining escaped paths and queries."""
    parts = urlsplit(url)
    for ending in (".parquet.meta.json", ".mosaic.json", ".pmtiles", ".parquet"):
        if parts.path.endswith(ending):
            return urlunsplit(parts._replace(path=parts.path[: -len(ending)] + suffix))
    return None


def get_parquet_info_from_source(source_config):
    if source_config.get("type") == "raster" or not source_config.get("url"):
        return None
    base_url = replace_asset_suffix(source_config["url"], ".parquet")
    if base_url is None:
        return None
    partitioned = bool(source_config.get("partitioned_parquet"))
    return {
        "base_url": base_url,
        "is_partitioned": partitioned,
        "meta_url": replace_asset_suffix(base_url, ".parquet.meta.json") if partitioned else None,
    }


def coverage_url(source, catalog_url=""):
    if source.get("type") == "raster":
        return None
    url = source.get("coverage_url") or replace_asset_suffix(source.get("url", ""), ".coverage.png")
    return urljoin(catalog_url, url) if url else None
