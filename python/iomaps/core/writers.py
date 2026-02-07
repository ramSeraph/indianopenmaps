"""
Output writers for various geospatial file formats.
"""

from pathlib import Path

import fiona


class FionaWriter:
    def __init__(self, output_file, schema, crs, driver):
        self.output_file = output_file
        self.schema = schema
        self.crs = crs
        self._collection = None
        self.driver = driver

    @property
    def collection(self):
        if self._collection is None:
            output_path = Path(self.output_file)
            if output_path.exists():
                output_path.unlink()
            self._collection = fiona.open(
                self.output_file,
                "w",
                driver=self.driver,
                schema=self.schema,
                crs=self.crs,
            )

        return self._collection

    def write(self, feature):
        self.collection.write(feature)

    def close(self):
        self.collection.close()

    def size(self):
        if self.output_file and Path(self.output_file).exists():
            return Path(self.output_file).stat().st_size

        return 0


def create_writer(output_file, schema, crs, output_driver):
    """
    Create an appropriate writer for the given output file.

    Args:
        output_file: Path to the output file
        schema: Fiona-compatible schema dict with 'geometry' and 'properties'
        crs: CRS object for the output file
        output_driver: Driver name for the output format

    Returns:
        FionaWriter instance
    """
    return FionaWriter(output_file, schema, crs, output_driver)
