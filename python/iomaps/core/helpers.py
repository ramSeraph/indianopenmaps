from pathlib import Path

import fiona

# Curated list of supported output drivers
SUPPORTED_OUTPUT_DRIVERS = [
    "CSV",
    "ESRI Shapefile",
    "FlatGeobuf",
    "GeoJSON",
    "GeoJSONSeq",
    "GPKG",
    "KML",
    "GeoParquet-1.1",
    "GeoParquet-2.0",
]

GEOPARQUET_VERSIONS = {
    "GeoParquet-1.1": "1.1",
    "GeoParquet-2.0": "2.0",
}


def get_supported_output_drivers():
    return SUPPORTED_OUTPUT_DRIVERS


def get_supported_input_drivers():
    driver_list = []
    supported_drivers = fiona.supported_drivers
    for k, v in supported_drivers.items():
        # Check if the driver supports reading ('r')
        if "r" in v:
            driver_list.append(k)
    return driver_list


def fix_if_required(p):
    if p.is_valid:
        return p
    p = p.buffer(0)
    if not p.is_valid:
        raise Exception("could not fix polygon")
    return p


special_cases = {
    "dxf": "DXF",
    "dgn": "DGN",
    "csv": "CSV",
    "tab": "MapInfo File",
    "mif": "MapInfo File",
    "gmt": "OGR_GMT",
    "gdb": "OpenFileGDB",
    "gml": "GML",
    "sqlite": "SQLite",
    "geojsonl": "GeoJSONSeq",
    "ndjson": "GeoJSONSeq",
    "geojsonseq": "GeoJSONSeq",
    "fgb": "FlatGeobuf",
    "geojson": "GeoJSON",
    "shp": "ESRI Shapefile",
    "gpkg": "GPKG",
    "parquet": "GeoParquet-1.1",
    "kml": "KML",
}


def get_driver_from_filename(filename):
    ext = Path(filename).suffix.lower().lstrip(".")
    return special_cases.get(ext)
