import json
import os

import py7zr
from click.testing import CliRunner

from iomaps.cli import main_cli


def test_infer_schema():
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
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        assert schema == {"properties": {"name": "str"}, "geometry": ["Point"]}


def test_infer_schema_multipolygon_polygon():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with Polygon and MultiPolygon geometries
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[10, 10], [10, 20], '
                '[20, 20], [20, 10], [10, 10]]]}, "properties": {"name": "Polygon1"}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": [[[[30, 30], [30, 40], '
                '[40, 40], [40, 30], [30, 30]]]]}, "properties": {"name": "MultiPolygon1"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        # Now returns all geometry types as a sorted list
        assert schema == {"properties": {"name": "str"}, "geometry": ["MultiPolygon", "Polygon"]}


def test_infer_schema_multipoint_point():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with Point and MultiPoint geometries
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [10, 10]}, '
                '"properties": {"name": "Point1"}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[20, 20], [30, 30]]}, '
                '"properties": {"name": "MultiPoint1"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        # Now returns all geometry types as a sorted list
        assert schema == {"properties": {"name": "str"}, "geometry": ["MultiPoint", "Point"]}


def test_infer_schema_multilinestring_linestring():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with LineString and MultiLineString geometries
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[10, 10], [20, 20]]}, '
                '"properties": {"name": "LineString1"}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "MultiLineString", "coordinates": [[[30, 30], [40, 40]], '
                '[[50, 50], [60, 60]]]}, "properties": {"name": "MultiLineString1"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        # Now returns all geometry types as a sorted list
        assert schema == {"properties": {"name": "str"}, "geometry": ["LineString", "MultiLineString"]}


def test_infer_schema_mixed_incompatible_geometries():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with Point and LineString geometries
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [10, 10]}, '
                '"properties": {"name": "Point1"}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[20, 20], [30, 30]]}, '
                '"properties": {"name": "LineString1"}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        # Now succeeds and returns all geometry types as a list
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        assert schema == {"properties": {"name": "str"}, "geometry": ["LineString", "Point"]}


def test_infer_schema_none_values_ignored():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with properties having None and actual values
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, '
                '"properties": {"prop_str": "hello", "prop_int": 1, "prop_float": 1.1}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [2, 2]}, '
                '"properties": {"prop_str": null, "prop_int": null, "prop_float": null}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 3]}, '
                '"properties": {"prop_str": "world", "prop_int": 2, "prop_float": 2.2}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        assert schema == {
            "geometry": ["Point"],
            "properties": {"prop_str": "str", "prop_int": "int", "prop_float": "float"},
        }


def test_infer_schema_mixed_property_types_to_str():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with properties having mixed types
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, '
                '"properties": {"mixed_int_str": 1, "mixed_float_str": 1.1, "mixed_int_float": 1}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [2, 2]}, '
                '"properties": {"mixed_int_str": "two", "mixed_float_str": "2.2", "mixed_int_float": 2.2}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 3]}, '
                '"properties": {"mixed_int_str": 3, "mixed_float_str": 3.3, "mixed_int_float": 3}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        assert schema == {
            "properties": {
                "mixed_int_str": "str",
                "mixed_float_str": "str",
                "mixed_int_float": "float",
            },
            "geometry": ["Point"],
        }


def test_infer_schema_int_float_none_to_float():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with properties having int, float, and None values
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, '
                '"properties": {"value": 1}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [2, 2]}, '
                '"properties": {"value": 2.2}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [3, 3]}, '
                '"properties": {"value": null}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        assert schema == {"properties": {"value": "float"}, "geometry": ["Point"]}


def test_infer_schema_all_none_to_str():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.7z file with properties having all None values
        with open("test.geojsonl", "w") as f:
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, '
                '"properties": {"value": null}}\n'
            )
            f.write(
                '{"type": "Feature", "geometry": {"type": "Point", "coordinates": [2, 2]}, '
                '"properties": {"value": null}}\n'
            )

        with py7zr.SevenZipFile("test.7z", "w") as archive:
            archive.write("test.geojsonl")

        result = runner.invoke(
            main_cli,
            ["cli", "infer-schema", "-i", "test.7z", "-o", "schema.json"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0

        with open("schema.json") as f:
            schema = json.load(f)

        assert schema == {"properties": {"value": "str"}, "geometry": ["Point"]}
