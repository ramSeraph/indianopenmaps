import json
import os

import fiona
import py7zr
from click.testing import CliRunner

from iomaps.cli import main_cli


def test_filter_7z():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [77.5946, 12.9716]}, '
                '"properties": {"name": "Bengaluru"}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [78.4867, 17.3850]}, '
                '"properties": {"name": "Hyderabad"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            [
                "cli",
                "filter-7z",
                "-i",
                "test.7z",
                "-o",
                "filtered.geojsonl",
                "-b",
                "77,12,78,13",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("filtered.geojsonl") as f:
            filtered_features = [json.loads(line) for line in f]

        assert len(filtered_features) == 1
        assert filtered_features[0]["properties"]["name"] == "Bengaluru"


def test_filter_7z_with_filter_file():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [77.5946, 12.9716]}, '
                '"properties": {"name": "Bengaluru"}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [78.4867, 17.3850]}, '
                '"properties": {"name": "Hyderabad"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        # Define schema and polygon for filter files
        schema = {
            "geometry": "Polygon",
            "properties": {"name": "str"},
        }
        polygon = {
            "type": "Polygon",
            "coordinates": [[(77, 12), (78, 12), (78, 13), (77, 13), (77, 12)]],
        }

        # Write shapefile
        with fiona.open("filter.shp", "w", "ESRI Shapefile", schema) as c:
            c.write(
                {
                    "geometry": polygon,
                    "properties": {"name": "filter_area"},
                }
            )

        # Write geopackage
        with fiona.open("filter.gpkg", "w", "GPKG", schema) as c:
            c.write(
                {
                    "geometry": polygon,
                    "properties": {"name": "filter_area"},
                }
            )

        # Write flatgeobuf
        with fiona.open("filter.fgb", "w", "FlatGeobuf", schema) as c:
            c.write(
                {
                    "geometry": polygon,
                    "properties": {"name": "filter_area"},
                }
            )

        # Test with shapefile
        result_shp = runner.invoke(
            main_cli,
            [
                "cli",
                "filter-7z",
                "-i",
                "test.7z",
                "-o",
                "filtered_shp.geojsonl",
                "-f",
                "filter.shp",
            ],
            catch_exceptions=False,
        )
        assert result_shp.exit_code == 0
        with open("filtered_shp.geojsonl") as f:
            filtered_features_shp = [json.loads(line) for line in f]
        assert len(filtered_features_shp) == 1
        assert filtered_features_shp[0]["properties"]["name"] == "Bengaluru"

        # Test with geopackage
        result_gpkg = runner.invoke(
            main_cli,
            [
                "cli",
                "filter-7z",
                "-i",
                "test.7z",
                "-o",
                "filtered_gpkg.geojsonl",
                "-f",
                "filter.gpkg",
            ],
            catch_exceptions=False,
        )
        assert result_gpkg.exit_code == 0
        with open("filtered_gpkg.geojsonl") as f:
            filtered_features_gpkg = [json.loads(line) for line in f]
        assert len(filtered_features_gpkg) == 1
        assert filtered_features_gpkg[0]["properties"]["name"] == "Bengaluru"

        # Test with flatgeobuf
        result_fgb = runner.invoke(
            main_cli,
            [
                "cli",
                "filter-7z",
                "-i",
                "test.7z",
                "-o",
                "filtered_fgb.geojsonl",
                "-f",
                "filter.fgb",
            ],
            catch_exceptions=False,
        )
        assert result_fgb.exit_code == 0
        with open("filtered_fgb.geojsonl") as f:
            filtered_features_fgb = [json.loads(line) for line in f]
        assert len(filtered_features_fgb) == 1
        assert filtered_features_fgb[0]["properties"]["name"] == "Bengaluru"


def test_filter_7z_multiple_filter_features_fails():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [77.5946, 12.9716]}, '
                '"properties": {"name": "Bengaluru"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        # Create a filter geojson file with multiple features
        filter_geojson_multiple = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[77, 12], [78, 12], [78, 13], [77, 13], [77, 12]]],
                    },
                },
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[[79, 14], [80, 14], [80, 15], [79, 15], [79, 14]]],
                    },
                },
            ],
        }
        with open("filter_multiple.geojson", "w") as f:
            json.dump(filter_geojson_multiple, f)

        result = runner.invoke(
            main_cli,
            [
                "cli",
                "filter-7z",
                "-i",
                "test.7z",
                "-o",
                "filtered_multiple.geojsonl",
                "-f",
                "filter_multiple.geojson",
            ],
            catch_exceptions=False,
        )
        assert result.exit_code != 0
        assert not os.path.exists("filtered_multiple.geojsonl")


def test_filter_7z_output_driver_inference():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with a single point feature
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [77.5, 12.5]}, '
                '"properties": {"name": "TestPoint"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        # Define a bounding box that includes the TestPoint
        bounds = "77,12,78,13"

        output_formats = {
            ".geojsonl": "GeoJSONSeq",
            ".geojson": "GeoJSON",
            ".shp": "ESRI Shapefile",
            ".gpkg": "GPKG",
            ".fgb": "FlatGeobuf",
        }

        for ext, _driver_name in output_formats.items():
            output_filename = f"filtered{ext}"
            result = runner.invoke(
                main_cli,
                [
                    "cli",
                    "filter-7z",
                    "-i",
                    "test.7z",
                    "-o",
                    output_filename,
                    "-b",
                    bounds,
                ],
                catch_exceptions=False,
            )
            assert result.exit_code == 0, f"Command failed for {ext}: {result.stderr}"
            assert os.path.exists(output_filename), f"Output file {output_filename} not created"


def test_filter_7z_filter_file_driver_inference():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with two point features
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [77.5, 12.5]}, '
                '"properties": {"name": "Inside"}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [80, 15]}, '
                '"properties": {"name": "Outside"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        # Define schema and polygon for filter files
        schema = {
            "geometry": "Polygon",
            "properties": {"name": "str"},
        }
        polygon = {
            "type": "Polygon",
            "coordinates": [[(77, 12), (78, 12), (78, 13), (77, 13), (77, 12)]],
        }

        # Create filter files
        filter_files = {
            "filter.geojson": "GeoJSON",
            "filter.shp": "ESRI Shapefile",
            "filter.gpkg": "GPKG",
            "filter.fgb": "FlatGeobuf",
        }

        for filename, driver in filter_files.items():
            if driver == "GeoJSON":
                with open(filename, "w") as f:
                    json.dump(
                        {
                            "type": "FeatureCollection",
                            "features": [
                                {
                                    "type": "Feature",
                                    "geometry": polygon,
                                    "properties": {"name": "filter_area"},
                                }
                            ],
                        },
                        f,
                    )
            else:
                with fiona.open(filename, "w", driver, schema) as c:
                    c.write(
                        {
                            "geometry": polygon,
                            "properties": {"name": "filter_area"},
                        }
                    )

        for filename in filter_files:
            output_filename = f"filtered_by_{os.path.splitext(filename)[1][1:]}.geojsonl"
            result = runner.invoke(
                main_cli,
                [
                    "cli",
                    "filter-7z",
                    "-i",
                    "test.7z",
                    "-o",
                    output_filename,
                    "-f",
                    filename,
                ],
                catch_exceptions=False,
            )
            assert result.exit_code == 0, f"Command failed for {filename}: {result.stderr}"
            assert os.path.exists(output_filename), f"Output file {output_filename} not created"

            with open(output_filename) as f:
                features = [json.loads(line) for line in f]
            assert len(features) == 1
            assert features[0]["properties"]["name"] == "Inside"


def test_filter_7z_explicit_output_driver():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with a single point feature
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [77.5, 12.5]}, '
                '"properties": {"name": "TestPoint"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        # Define a bounding box that includes the TestPoint
        bounds = "77,12,78,13"

        output_drivers = {
            "GeoJSONSeq": ".geojsonl",
            "GeoJSON": ".geojson",
            "ESRI Shapefile": ".shp",
            "GPKG": ".gpkg",
            "FlatGeobuf": ".fgb",
        }

        for driver_name, ext in output_drivers.items():
            output_filename = f"filtered_explicit{ext}"
            result = runner.invoke(
                main_cli,
                [
                    "cli",
                    "filter-7z",
                    "-i",
                    "test.7z",
                    "-o",
                    output_filename,
                    "-b",
                    bounds,
                    "-d",
                    driver_name,
                ],
                catch_exceptions=False,
            )
            assert result.exit_code == 0, f"Command failed for driver {driver_name}: {result.stderr}"
            assert os.path.exists(output_filename), f"Output file {output_filename} not created"


def test_filter_7z_schema_inference():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with features having diverse properties
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, '
                '"properties": {"prop_str": "hello", "prop_int": 1, "prop_float": 1.1, "prop_null": null, '
                '"prop_int_float": 1}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [2, 2]}, '
                '"properties": {"prop_str": "world", "prop_int": null, "prop_float": 2.2, "prop_null": null, '
                '"prop_int_float": 2.2}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        # Define a bounding box that includes both points
        bounds = "0,0,3,3"
        output_filename = "filtered_inferred_schema.gpkg"

        result = runner.invoke(
            main_cli,
            ["cli", "filter-7z", "-i", "test.7z", "-o", output_filename, "-b", bounds],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, f"Command failed: {result.stderr}"
        assert os.path.exists(output_filename), "Output file not created"

        with fiona.open(output_filename, "r") as collection:
            expected_schema = {
                "geometry": "Point",
                "properties": {
                    "prop_str": "str",
                    "prop_int": "int",
                    "prop_float": "float",
                    "prop_null": "str",  # NoneType becomes str if no other type is present
                    "prop_int_float": "float",
                },
            }
            assert collection.schema == expected_schema
            assert len(collection) == 2
