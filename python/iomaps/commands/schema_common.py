import logging

import multivolumefile
import py7zr
from tqdm import tqdm

from iomaps.helpers import (
    get_geojsonl_file_info,
)

from iomaps.streaming import StreamingWriterFactory

from iomaps.infer_schema import (
    SchemaWriter, 
    SchemaFilter,
)

class Updater:
    def __init__(self, pb):
        self.pb = pb

    def update_size(self, sz):
        self.pb.update(sz)

    def update_other_info(self, **kwargs):
        self.pb.set_postfix(processed=kwargs.get('processed', 0))

def process_archive_for_schema(archive, dummy_filter, schema_writer):
    geojsonl_file_info = get_geojsonl_file_info(archive)
    if geojsonl_file_info is None:
        raise FileNotFoundError("No .geojsonl file found in the archive.")
    
    target_file = geojsonl_file_info.filename
    file_size = geojsonl_file_info.uncompressed

    logging.info(f"Found geojsonl file: {target_file} (size: {file_size} bytes)")

    try:
        with tqdm(total=file_size, unit='B', unit_scale=True, desc=f"Inferring schema from {target_file}") as pbar:
            updater = Updater(pbar)
            factory = StreamingWriterFactory(dummy_filter, schema_writer, updater)
            archive.extract(targets=[target_file], factory=factory)
            factory.streaming_io.flush_last_line()
    finally:
        schema_writer.close()


def get_schema_from_archive(input_path, limit_to_geom_type=None, strict_geom_type_check=False):
    schema_writer = SchemaWriter()
    schema_filter = SchemaFilter(limit_to_geom_type=limit_to_geom_type, strict_geom_type_check=strict_geom_type_check)

    archive_path = str(input_path)

    try:
        if archive_path.endswith('.7z.001'):

            base_path = archive_path.rsplit('.', 1)[0]

            with multivolumefile.open(base_path, 'rb') as multivolume_file:
                with py7zr.SevenZipFile(multivolume_file, 'r') as archive:
                    process_archive_for_schema(archive, schema_filter, schema_writer)
        else:

            with py7zr.SevenZipFile(archive_path, mode='r') as archive:
                process_archive_for_schema(archive, schema_filter, schema_writer)
        
        return schema_writer.get_schema()

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None
