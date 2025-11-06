import json
import logging

from pathlib import Path

import fiona
from shapely.geometry import shape, box
from shapely.prepared import prep

from iomaps.helpers import (
    fix_if_required,
    get_driver_from_filename,
)

class Updater:
    def __init__(self, pb):
        self.pb = pb

    def update_size(self, sz):
        self.pb.update(sz)

    def update_other_info(self, **kwargs):
        self.pb.set_postfix(processed=kwargs.get('processed', 0))


class FionaWriter:
    def __init__(self, output_file, schema, crs, driver):
        self.output_file = output_file
        self.schema = schema
        self.crs = crs
        self.collection = fiona.open(
            self.output_file,
            'w',
            driver=driver,
            schema=self.schema,
            crs=self.crs
        )

    def convert_feature(self, feature):
        geom_type = feature['geometry']['type']
        schema_geom_type = self.schema['geometry']
        if geom_type == schema_geom_type:
            return feature

        if schema_geom_type == 'MultiPolygon' and geom_type == 'Polygon':
            feature['geometry'] = {
                'type': 'MultiPolygon',
                'coordinates': [feature['geometry']['coordinates']]
            }
            return feature

        if schema_geom_type == 'MultiLineString' and geom_type == 'LineString':
            feature['geometry'] = {
                'type': 'MultiLineString',
                'coordinates': [feature['geometry']['coordinates']]
            }
            return feature

        if schema_geom_type == 'MultiPoint' and geom_type == 'Point':
            feature['geometry'] = {
                'type': 'MultiPoint',
                'coordinates': [feature['geometry']['coordinates']]
            }
            return feature

        logging.warning(f"Skipping feature with incompatible geometry type: {geom_type} (expected {schema_geom_type})")


    def write(self, feature):
        feature = self.convert_feature(feature)
        if feature is None:
            return
        self.collection.write(feature)

    def close(self):
        self.collection.close()

    def size(self):
        if self.output_file and Path(self.output_file).exists():
            return Path(self.output_file).stat().st_size

        return 0


class ShapeFilter:
    def __init__(self, filter_shape,
                 clip=True,
                 limit_to_geom_type=None,
                 strict_geom_type_check=False):
        self.filter_shape = prep(filter_shape)
        self.count = 0
        self.passed = 0
        self.unparsed = 0
        self.error_count = 0
        self.clip = clip
        self.limit_to_geom_type = limit_to_geom_type
        self.strict_geom_type_check = strict_geom_type_check

    def _geom_type_matches(self, feature_geom_type):
        if self.limit_to_geom_type is None:
            return True

        if self.strict_geom_type_check:
            return feature_geom_type == self.limit_to_geom_type
        else:
            # Non-strict check: Multi-part geometries match their single-part counterparts
            if self.limit_to_geom_type == feature_geom_type:
                return True
            elif self.limit_to_geom_type == "Polygon" and feature_geom_type == "MultiPolygon":
                return True
            elif self.limit_to_geom_type == "LineString" and feature_geom_type == "MultiLineString":
                return True
            elif self.limit_to_geom_type == "Point" and feature_geom_type == "MultiPoint":
                return True
            return False

    def process(self, line):
        self.count += 1

        try:
            feature = json.loads(line)
        except json.JSONDecodeError:
            logging.warning(f"Skipping line, could not decode JSON: {line}")
            self.unparsed += 1
            return None

        if not self._geom_type_matches(feature['geometry']['type']):
            logging.debug(f"Skipping feature due to geometry type mismatch: expected {self.limit_to_geom_type}, got {feature['geometry']['type']}")
            return None

        try:
            feature_shape = shape(feature['geometry'])
            feature_shape = fix_if_required(feature_shape)
            if self.filter_shape.intersects(feature_shape):
                self.passed += 1
                if self.clip:
                    feature['geometry'] = (self.filter_shape.context.intersection(feature_shape)).__geo_interface__
                return feature
        except Exception as e:
            logging.error(f"An error occurred processing line: {e}")
            self.error_count += 1

        return None


class FilterFeaturePicker:
    def __init__(self, polygon_features,
                 pick_filter_feature_id=None,
                 pick_filter_feature_kv=None):
        self.polygon_features = polygon_features
        self.pick_filter_feature_id = pick_filter_feature_id
        self.pick_filter_feature_kv = pick_filter_feature_kv

    def pick(self):
        if self.pick_filter_feature_id is not None and self.pick_filter_feature_kv:
            logging.error("Only one of pick_filter_feature_id or pick_filter_feature_kv can be used, not both.")
            return None

        if self.pick_filter_feature_id is not None:
            return self._pick_by_id()
        
        if self.pick_filter_feature_kv:
            return self._pick_by_kv()

        return self.polygon_features

    def _pick_by_id(self):
        feature_index = self.pick_filter_feature_id
        if 0 <= feature_index < len(self.polygon_features):
            return [self.polygon_features[feature_index]]

        logging.error(f"Invalid feature index '{feature_index}'. Filter file contains {len(self.polygon_features)} polygon features.")
        return None

    def _pick_by_kv(self):
        filtered_features = self.polygon_features
        for kv in self.pick_filter_feature_kv:
            if '=' not in kv:
                logging.error(f"Invalid --pick-filter-feature-kv format: '{kv}'. Expected 'key=value'.")
                return None
            key, value = kv.split('=', 1)
            filtered_features = [f for f in filtered_features if f['properties'].get(key) == value]

        return filtered_features

def get_shape_from_bounds(bounds):
    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bounds.split(','))
    except ValueError:
        logging.error(f"Invalid bounds format: {bounds}. Expected 'min_lon,min_lat,max_lon,max_lat'")
        return None

    if not (-180 <= min_lon <= 180 and -90 <= min_lat <= 90 and -180 <= max_lon <= 180 and -90 <= max_lat <= 90):
        logging.error("Bounds values are out of valid range.")
        return None

    if min_lon >= max_lon or min_lat >= max_lat:
        logging.error("Bounds values are not in correct order: min values must be less than max values.")
        return None

    filter_shape = box(min_lon, min_lat, max_lon, max_lat)

    return filter_shape

def print_filter_features(filter_features):
    print("Available polygon features in the filter file:")
    for idx, feature in enumerate(filter_features):
        props = feature['properties']
        props_str = ', '.join([f"{k}={v}" for k, v in props.items()])
        bbox = shape(feature['geometry']).bounds
        print(f"  ID {idx}: {props_str}, BBOX={bbox}")

def get_shape_from_filter_file(filter_file, driver=None, pick_filter_feature_id=None, pick_filter_feature_kv=None):

    filter_file_path = Path(filter_file)

    if not filter_file_path.exists():
        logging.error(f"Filter file not found: {filter_file}")
        return None

    if driver is None:
        driver = get_driver_from_filename(filter_file)

    try:
        with fiona.open(filter_file, 'r', driver=driver) as collection:
            polygon_features = []
            for feature in collection:
                geom_type = feature['geometry']['type']
                if geom_type in ['Polygon', 'MultiPolygon']:
                    polygon_features.append(feature)

            picker = FilterFeaturePicker(polygon_features, pick_filter_feature_id, pick_filter_feature_kv)
            filter_features = picker.pick()

            if filter_features is None:
                return None

            if len(filter_features) == 0:
                logging.error("No polygon features in the filter matched the selection criteria.")
                return None

            if len(filter_features) > 1:
                logging.error("Multiple polygon features matched the selection criteria. Please refine your selection using --pick-filter-feature-id or --pick-filter-feature-kv.")
                print_filter_features(filter_features)
                return None

            filter_feature = filter_features[0]

            
            filter_shape = shape(filter_feature['geometry'])
            filter_shape = fix_if_required(filter_shape)
            return filter_shape

    except fiona.errors.DriverError as e:
        logging.error(f"Error opening filter file {filter_file}: {e}. If the file type is not recognized, please specify the driver using --filter-file-driver.")
        return None


def create_filter(filter_file=None,
                  filter_file_driver=None,
                  bounds=None,
                  no_clip=False,
                  limit_to_geom_type=None,
                  strict_geom_type_check=False,
                  pick_filter_feature_id=None,
                  pick_filter_feature_kv=None):

    clip_features = not no_clip

    filter_shape = None

    if filter_file:
        filter_shape = get_shape_from_filter_file(filter_file,
                                                  driver=filter_file_driver,
                                                  pick_filter_feature_id=pick_filter_feature_id,
                                                  pick_filter_feature_kv=pick_filter_feature_kv)
    else:
        filter_shape = get_shape_from_bounds(bounds)

    if filter_shape is None:
        return None

    return ShapeFilter(filter_shape,
                       clip=clip_features,
                       limit_to_geom_type=limit_to_geom_type,
                       strict_geom_type_check=strict_geom_type_check)


def create_writer(output_file, schema, crs, output_driver=None):

    driver = output_driver
    if driver is None:
        driver = get_driver_from_filename(output_file)

    if driver is None:
        logging.error(f"Could not determine output file format from extension: {output_file}, please specify driver explicitly using -d/--output-driver.")
        return None

    return FionaWriter(output_file, schema, crs, driver)
