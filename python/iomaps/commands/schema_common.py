import logging

import multivolumefile
import py7zr
from rich.progress import BarColumn, DownloadColumn, Progress, TextColumn, TimeElapsedColumn, TransferSpeedColumn

from iomaps.core.helpers import (
    get_geojsonl_file_info,
)
from iomaps.core.infer_schema import (
    SchemaFilter,
    SchemaWriter,
)
from iomaps.core.streaming_7z import StreamingWriterFactory


class RichUpdater:
    def __init__(self, progress, task_id):
        self.progress = progress
        self.task_id = task_id

    def update_size(self, sz):
        self.progress.update(self.task_id, advance=sz)

    def update_other_info(self, **kwargs):
        processed = kwargs.get("processed", 0)
        self.progress.update(self.task_id, description=f"[cyan]Inferring schema ({processed:,} features)")


class CombinedUpdater:
    def __init__(self, *updaters):
        self.updaters = updaters

    def update_size(self, sz):
        for updater in self.updaters:
            updater.update_size(sz)

    def update_other_info(self, **kwargs):
        for updater in self.updaters:
            updater.update_other_info(**kwargs)


class QtUpdater:
    def __init__(self, signal, total_size):
        self.signal = signal
        self.total_size = total_size
        self.processed_size = 0

    def update_size(self, sz):
        self.processed_size += sz
        progress = int(self.processed_size * 100 / self.total_size)
        self.signal.emit(progress)

    def update_other_info(self, **kwargs):
        pass  # Not needed for the progress bar


def process_archive_for_schema(archive, dummy_filter, schema_writer, external_updater=None):
    geojsonl_file_info = get_geojsonl_file_info(archive)
    if geojsonl_file_info is None:
        raise FileNotFoundError("No .geojsonl file found in the archive.")

    target_file = geojsonl_file_info.filename
    file_size = geojsonl_file_info.uncompressed

    logging.info(f"Found geojsonl file: {target_file} (size: {file_size} bytes)")

    try:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeElapsedColumn(),
            console=None,
        ) as progress:
            task_id = progress.add_task(f"[cyan]Inferring schema from {target_file}", total=file_size)
            updater = RichUpdater(progress, task_id)
            if external_updater:
                qt_updater = QtUpdater(external_updater, file_size)
                updater = CombinedUpdater(qt_updater, updater)
            factory = StreamingWriterFactory(dummy_filter, schema_writer, updater)
            archive.extract(targets=[target_file], factory=factory)
            factory.streaming_io.flush_last_line()
    finally:
        schema_writer.close()


def get_schema_from_archive(input_path, external_updater=None):
    schema_writer = SchemaWriter()
    schema_filter = SchemaFilter()

    archive_path = str(input_path)

    try:
        if archive_path.endswith(".7z.001"):
            base_path = archive_path.rsplit(".", 1)[0]

            with multivolumefile.open(base_path, "rb") as multivolume_file:
                with py7zr.SevenZipFile(multivolume_file, "r") as archive:
                    process_archive_for_schema(archive, schema_filter, schema_writer, external_updater)
        else:
            with py7zr.SevenZipFile(archive_path, mode="r") as archive:
                process_archive_for_schema(archive, schema_filter, schema_writer, external_updater)

        schema, renames = schema_writer.get_schema()
        if schema is None:
            return None, {}

        # Convert geometry to list
        source_geom_types = schema["geometry"] if isinstance(schema["geometry"], list) else [schema["geometry"]]
        if not source_geom_types:
            logging.error("No geometry types found in source")
            return None, {}

        schema["geometry"] = sorted(source_geom_types)
        return schema, renames

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None, {}
