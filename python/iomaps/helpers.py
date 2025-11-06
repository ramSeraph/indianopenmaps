import logging

import fiona
from pathlib import Path

def get_supported_output_drivers():
    driver_list = []
    supported_drivers = fiona.supported_drivers
    for k, v in supported_drivers.items():
        # Check if the driver supports writing ('w') or appending ('a')
        if 'w' in v or 'a' in v:
            driver_list.append(k)
    return driver_list

def get_supported_input_drivers():
    driver_list = []
    supported_drivers = fiona.supported_drivers
    for k, v in supported_drivers.items():
        # Check if the driver supports reading ('r')
        if 'r' in v:
            driver_list.append(k)
    return driver_list

def fix_if_required(p):
    if p.is_valid:
        return p
    p = p.buffer(0)
    if not p.is_valid:
        raise Exception('could not fix polygon')
    return p


def readable_size(size_bytes):
    if size_bytes == 0:
        return "0.00 KB"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(min(len(size_name) - 1, (size_bytes.bit_length() - 1) // 10))
    p = 1 << (i * 10)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"


def get_base_name(input_path):
    if '.7z.001' in input_path.name:
        base_name = input_path.name.rsplit('.7z.001', 1)[0]
    else:
        base_name = input_path.name.rsplit('.7z', 1)[0]
    return base_name

def get_geojsonl_file_info(archive):
    geojsonl_files_infos = [f for f in archive.files if f.filename.endswith('.geojsonl')]

    if not geojsonl_files_infos:
        logging.error("No .geojsonl file found in the archive.")
        return None

    if len(geojsonl_files_infos) > 1:
        logging.warning(f"Multiple .geojsonl files found, using the first one: {geojsonl_files_infos[0].filename}")

    return geojsonl_files_infos[0]


special_cases = {
    'dxf': 'DXF',
    'dgn': 'DGN',
    'csv': 'CSV',
    'tab': 'MapInfo File',
    'mif': 'MapInfo File',
    'gmt': 'OGR_GMT',
    'gdb': 'OpenFileGDB',
    'gml': 'GML',
    'sqlite': 'SQLite',
    'geojsonl': 'GeoJSONSeq',
    'ndjson': 'GeoJSONSeq',
    'geojsonseq': 'GeoJSONSeq',
    'fgb': 'FlatGeobuf',
    'geojson': 'GeoJSON',
    'shp': 'ESRI Shapefile',
    'gpkg': 'GPKG',
}

def get_driver_from_filename(filename):
    ext = Path(filename).suffix.lower().lstrip('.')
    return special_cases.get(ext)

