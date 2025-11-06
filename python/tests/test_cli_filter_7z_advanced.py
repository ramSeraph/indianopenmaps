import json
import os
import py7zr
from click.testing import CliRunner
from iomaps.cli import main_cli
import fiona
from shapely.geometry import shape

def test_filter_7z_no_clip():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.geojsonl file with a polygon feature
        original_feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [0, 0],
                        [0, 10],
                        [10, 10],
                        [10, 0],
                        [0, 0]
                    ]
                ]
            },
            "properties": {"name": "OriginalPolygon"}
        }
        with open("test.geojsonl", "w") as f:
            f.write(json.dumps(original_feature) + '\n')
        
        with py7zr.SevenZipFile("test.7z", 'w') as archive:
            archive.write("test.geojsonl")

        # Create a filter geojson file that partially overlaps
        filter_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [5, 5],
                                [5, 15],
                                [15, 15],
                                [15, 5],
                                [5, 5]
                            ]
                        ]
                    }
                }
            ]
        }
        with open("filter.geojson", "w") as f:
            json.dump(filter_geojson, f)

        # Run filter-7z with --no-clip
        result = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered.geojsonl", "-f", "filter.geojson", "--no-clip"], catch_exceptions=False)
        assert result.exit_code == 0, f"Command failed: {result.stderr}"
        assert os.path.exists("filtered.geojsonl"), "Output file not created"

        with open("filtered.geojsonl", "r") as f:
            filtered_features = [json.loads(line) for line in f]
        
        assert len(filtered_features) == 1
        output_feature = filtered_features[0]

        # Verify that the geometry is not clipped (i.e., it's the original geometry)
        assert shape(output_feature['geometry']).equals(shape(original_feature['geometry']))
        assert output_feature['properties']['name'] == "OriginalPolygon"

def test_filter_7z_limit_to_geom_type():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.geojsonl file with features of different geometry types
        with open("test.geojsonl", "w") as f:
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, "properties": {"name": "Point1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}, "properties": {"name": "LineString1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "properties": {"name": "Polygon1"}}\n')
        
        with py7zr.SevenZipFile("test.7z", 'w') as archive:
            archive.write("test.geojsonl")

        # Define a broad bounding box to ensure all features are considered
        bounds = "-10,-10,10,10"

        # Test for Point geometry type
        result_point = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_point.geojsonl", "-b", bounds, "-g", "Point"], catch_exceptions=False)
        assert result_point.exit_code == 0, f"Command failed for Point: {result_point.stderr}"
        with open("filtered_point.geojsonl", "r") as f:
            features = [json.loads(line) for line in f]
        assert len(features) == 1
        assert features[0]["properties"]["name"] == "Point1"

        # Test for LineString geometry type
        result_linestring = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_linestring.geojsonl", "-b", bounds, "-g", "LineString"], catch_exceptions=False)
        assert result_linestring.exit_code == 0, f"Command failed for LineString: {result_linestring.stderr}"
        with open("filtered_linestring.geojsonl", "r") as f:
            features = [json.loads(line) for line in f]
        assert len(features) == 1
        assert features[0]["properties"]["name"] == "LineString1"

        # Test for Polygon geometry type
        result_polygon = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_polygon.geojsonl", "-b", bounds, "-g", "Polygon"], catch_exceptions=False)
        assert result_polygon.exit_code == 0, f"Command failed for Polygon: {result_polygon.stderr}"
        with open("filtered_polygon.geojsonl", "r") as f:
            features = [json.loads(line) for line in f]
        assert len(features) == 1
        assert features[0]["properties"]["name"] == "Polygon1"

def test_filter_7z_strict_geom_type_check():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.geojsonl file with a mix of single and multi-part geometries
        with open("test.geojsonl", "w") as f:
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, "properties": {"name": "Point1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "MultiPoint", "coordinates": [[2, 2], [3, 3]]}, "properties": {"name": "MultiPoint1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}, "properties": {"name": "LineString1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "MultiLineString", "coordinates": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]}, "properties": {"name": "MultiLineString1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]]}, "properties": {"name": "Polygon1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "MultiPolygon", "coordinates": [[[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]], [[[2, 2], [2, 3], [3, 3], [3, 2], [2, 2]]]]}, "properties": {"name": "MultiPolygon1"}}\n')
        
        with py7zr.SevenZipFile("test.7z", 'w') as archive:
            archive.write("test.geojsonl")

        # Define a broad bounding box to ensure all features are considered
        bounds = "-10,-10,10,10"

        # Test with --limit-to-geom-type Point and --strict-geom-type-check
        result_point = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_point_strict.geojsonl", "-b", bounds, "-g", "Point", "--strict-geom-type-check"], catch_exceptions=False)
        assert result_point.exit_code == 0, f"Command failed for Point (strict): {result_point.stderr}"
        with open("filtered_point_strict.geojsonl", "r") as f:
            features = [json.loads(line) for line in f]
        assert len(features) == 1
        assert features[0]["properties"]["name"] == "Point1"

        # Test with --limit-to-geom-type MultiPoint and --strict-geom-type-check
        result_multipoint = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_multipoint_strict.geojsonl", "-b", bounds, "-g", "MultiPoint", "--strict-geom-type-check"], catch_exceptions=False)
        assert result_multipoint.exit_code == 0, f"Command failed for MultiPoint (strict): {result_multipoint.stderr}"
        with open("filtered_multipoint_strict.geojsonl", "r") as f:
            features = [json.loads(line) for line in f]
        assert len(features) == 1
        assert features[0]["properties"]["name"] == "MultiPoint1"

        # Test with --limit-to-geom-type Polygon and --strict-geom-type-check
        result_polygon = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_polygon_strict.geojsonl", "-b", bounds, "-g", "Polygon", "--strict-geom-type-check"], catch_exceptions=False)
        assert result_polygon.exit_code == 0, f"Command failed for Polygon (strict): {result_polygon.stderr}"
        with open("filtered_polygon_strict.geojsonl", "r") as f:
            features = [json.loads(line) for line in f]
        assert len(features) == 1
        assert features[0]["properties"]["name"] == "Polygon1"

        # Test with --limit-to-geom-type MultiPolygon and --strict-geom-type-check
        result_multipolygon = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_multipolygon_strict.geojsonl", "-b", bounds, "-g", "MultiPolygon", "--strict-geom-type-check"], catch_exceptions=False)
        assert result_multipolygon.exit_code == 0, f"Command failed for MultiPolygon (strict): {result_multipolygon.stderr}"
        with open("filtered_multipolygon_strict.geojsonl", "r") as f:
            features = [json.loads(line) for line in f]
        assert len(features) == 1
        assert features[0]["properties"]["name"] == "MultiPolygon1"

def test_filter_7z_pick_filter_feature_id():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.geojsonl file with features
        with open("test.geojsonl", "w") as f:
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, "properties": {"name": "Feature1"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [6, 6]}, "properties": {"name": "Feature2"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [11, 11]}, "properties": {"name": "Feature3"}}\n')
        
        with py7zr.SevenZipFile("test.7z", 'w') as archive:
            archive.write("test.geojsonl")

        # Create a filter geojson file with multiple features
        filter_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"id": 0},
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]]]}
                },
                {
                    "type": "Feature",
                    "properties": {"id": 1},
                    "geometry": {"type": "Polygon", "coordinates": [[[5, 5], [5, 10], [10, 10], [10, 5], [5, 5]]]}
                },
                {
                    "type": "Feature",
                    "properties": {"id": 2},
                    "geometry": {"type": "Polygon", "coordinates": [[[10, 10], [10, 15], [15, 15], [15, 10], [10, 10]]]}
                }
            ]
        }
        with open("filter.geojson", "w") as f:
            json.dump(filter_geojson, f)

        # Test picking the first filter feature (index 0)
        result_0 = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_0.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-id", "0"], catch_exceptions=False)
        assert result_0.exit_code == 0, f"Command failed for id 0: {result_0.stderr}"
        with open("filtered_0.geojsonl", "r") as f:
            features_0 = [json.loads(line) for line in f]
        assert len(features_0) == 1
        assert features_0[0]["properties"]["name"] == "Feature1"

        # Test picking the second filter feature (index 1)
        result_1 = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_1.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-id", "1"], catch_exceptions=False)
        assert result_1.exit_code == 0, f"Command failed for id 1: {result_1.stderr}"
        with open("filtered_1.geojsonl", "r") as f:
            features_1 = [json.loads(line) for line in f]
        assert len(features_1) == 1
        assert features_1[0]["properties"]["name"] == "Feature2"

        # Test picking the third filter feature (index 2)
        result_2 = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_2.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-id", "2"], catch_exceptions=False)
        assert result_2.exit_code == 0, f"Command failed for id 2: {result_2.stderr}"
        with open("filtered_2.geojsonl", "r") as f:
            features_2 = [json.loads(line) for line in f]
        assert len(features_2) == 1
        assert features_2[0]["properties"]["name"] == "Feature3"

        # Test with an out-of-bounds index (should result in no features or an error depending on implementation)
        result_out_of_bounds = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_out.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-id", "99"], catch_exceptions=False)
        assert result_out_of_bounds.exit_code != 0 # Expecting an error for out-of-bounds index
        assert not os.path.exists("filtered_out.geojsonl") # No output file should be created on error

def test_filter_7z_pick_filter_feature_kv():
    runner = CliRunner()
    with runner.isolated_filesystem() as td:
        os.chdir(td)
        # Create a dummy test.geojsonl file with features
        with open("test.geojsonl", "w") as f:
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [1, 1]}, "properties": {"name": "FeatureA"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [6, 6]}, "properties": {"name": "FeatureB"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [11, 11]}, "properties": {"name": "FeatureC"}}\n')
            f.write('{"type": "Feature", "geometry": {"type": "Point", "coordinates": [16, 16]}, "properties": {"name": "FeatureD"}}\n')
        
        with py7zr.SevenZipFile("test.7z", 'w') as archive:
            archive.write("test.geojsonl")

        # Create a filter geojson file with multiple features, each with a unique key-value pair
        filter_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"filter_id": "first", "value": 1},
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]]]}
                },
                {
                    "type": "Feature",
                    "properties": {"filter_id": "second", "value": 2},
                    "geometry": {"type": "Polygon", "coordinates": [[[5, 5], [5, 10], [10, 10], [10, 5], [5, 5]]]}
                },
                {
                    "type": "Feature",
                    "properties": {"filter_id": "third", "value": 3},
                    "geometry": {"type": "Polygon", "coordinates": [[[10, 10], [10, 15], [15, 15], [15, 10], [10, 10]]]}
                },
                {
                    "type": "Feature",
                    "properties": {"filter_id": "fourth", "value": 100},
                    "geometry": {"type": "Polygon", "coordinates": [[[15, 15], [15, 20], [20, 20], [20, 15], [15, 15]]]}
                }
            ]
        }
        with open("filter.geojson", "w") as f:
            json.dump(filter_geojson, f)

        # Test picking the feature with filter_id="first"
        result_first = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_first.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-kv", "filter_id=first"], catch_exceptions=False)
        assert result_first.exit_code == 0, f"Command failed for filter_id=first: {result_first.stderr}"
        with open("filtered_first.geojsonl", "r") as f:
            features_first = [json.loads(line) for line in f]
        assert len(features_first) == 1
        assert features_first[0]["properties"]["name"] == "FeatureA"

        # Test picking the feature with filter_id="second"
        result_second = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_second.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-kv", "filter_id=second"], catch_exceptions=False)
        assert result_second.exit_code == 0, f"Command failed for filter_id=second: {result_second.stderr}"
        with open("filtered_second.geojsonl", "r") as f:
            features_second = [json.loads(line) for line in f]
        assert len(features_second) == 1
        assert features_second[0]["properties"]["name"] == "FeatureB"

        # Test picking the feature with an integer value
        result_int_value = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_int_value.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-kv", "value=100"], catch_exceptions=False)
        assert result_int_value.exit_code == 0, f"Command failed for value=100: {result_int_value.stderr}"
        with open("filtered_int_value.geojsonl", "r") as f:
            features_int_value = [json.loads(line) for line in f]
        assert len(features_int_value) == 1
        assert features_int_value[0]["properties"]["name"] == "FeatureD"

        # Test picking the feature with value=3 (this should now pass and filter FeatureC)
        result_third = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_third.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-kv", "value=3"], catch_exceptions=False)
        assert result_third.exit_code == 0, f"Command failed for value=3: {result_third.stderr}"
        with open("filtered_third.geojsonl", "r") as f:
            features_third = [json.loads(line) for line in f]
        assert len(features_third) == 1
        assert features_third[0]["properties"]["name"] == "FeatureC"

        # Test with a non-existent key-value pair (should result in an error)
        result_non_existent = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_non_existent.geojsonl", "-f", "filter.geojson", "--pick-filter-feature-kv", "value=999"], catch_exceptions=False)
        assert result_non_existent.exit_code != 0 # Expecting an error for non-existent KV pair
        assert not os.path.exists("filtered_non_existent.geojsonl") # No output file should be created on error

        # Test with a key-value pair that matches multiple features (should result in an error)
        filter_geojson_duplicate = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"common_key": "common_value"},
                    "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [0, 5], [5, 5], [5, 0], [0, 0]]]}
                },
                {
                    "type": "Feature",
                    "properties": {"common_key": "common_value"},
                    "geometry": {"type": "Polygon", "coordinates": [[[5, 5], [5, 10], [10, 10], [10, 5], [5, 5]]]}
                }
            ]
        }
        with open("filter_duplicate.geojson", "w") as f:
            json.dump(filter_geojson_duplicate, f)

        result_duplicate = runner.invoke(main_cli, ["cli", "filter-7z", "-i", "test.7z", "-o", "filtered_duplicate.geojsonl", "-f", "filter_duplicate.geojson", "--pick-filter-feature-kv", "common_key=common_value"], catch_exceptions=False)
        assert result_duplicate.exit_code != 0 # Expecting an error for multiple matches
        assert not os.path.exists("filtered_duplicate.geojsonl") # No output file should be created on error
