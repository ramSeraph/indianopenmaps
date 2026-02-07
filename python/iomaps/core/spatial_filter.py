"""
Spatial filter utilities for handling filter shapes and bounds.
"""

import json
import logging
from pathlib import Path

import fiona
from shapely.geometry import box, shape
from shapely.prepared import prep

from iomaps.core.helpers import fix_if_required, get_driver_from_filename


class FilterFeaturePicker:
    """Picks specific features from a collection based on ID or key-value pairs."""

    def __init__(self, polygon_features, pick_filter_feature_id=None, pick_filter_feature_kv=None):
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

        logging.error(
            f"Invalid feature index '{feature_index}'. "
            f"Filter file contains {len(self.polygon_features)} polygon features."
        )
        return None

    def _pick_by_kv(self):
        filtered_features = self.polygon_features
        for kv in self.pick_filter_feature_kv:
            if "=" not in kv:
                logging.error(f"Invalid --pick-filter-feature-kv format: '{kv}'. Expected 'key=value'.")
                return None
            key, value = kv.split("=", 1)
            filtered_features = [f for f in filtered_features if str(f["properties"].get(key)) == value]

        return filtered_features


def print_filter_features(filter_features):
    """Prints available polygon features in a filter file."""
    print("Available polygon features in the filter file:")
    for idx, feature in enumerate(filter_features):
        props = feature["properties"]
        props_str = ", ".join([f"{k}={v}" for k, v in props.items()])
        bbox = shape(feature["geometry"]).bounds
        print(f"  ID {idx}: {props_str}, BBOX={bbox}")


def get_shape_from_bounds(bounds):
    """
    Creates a bounding box shape from a bounds string.

    Args:
        bounds: Comma-separated string 'min_lon,min_lat,max_lon,max_lat'

    Returns:
        shapely.geometry: Box shape, or None if invalid
    """
    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bounds.split(","))
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


def get_shape_from_filter_file(filter_file, driver=None, pick_filter_feature_id=None, pick_filter_feature_kv=None):
    """
    Extracts a single polygon shape from a filter file.

    Args:
        filter_file: Path to the filter file
        driver: OGR driver name (optional, inferred if not provided)
        pick_filter_feature_id: Index of feature to pick (optional)
        pick_filter_feature_kv: Key-value pairs to filter features (optional)

    Returns:
        shapely.geometry: Filter shape, or None if error
    """
    filter_file_path = Path(filter_file)

    if not filter_file_path.exists():
        logging.error(f"Filter file not found: {filter_file}")
        return None

    if driver is None:
        driver = get_driver_from_filename(filter_file)

    try:
        with fiona.open(filter_file, "r", driver=driver) as collection:
            polygon_features = []
            for feature in collection:
                geom_type = feature["geometry"]["type"]
                if geom_type in ["Polygon", "MultiPolygon"]:
                    polygon_features.append(feature)

            picker = FilterFeaturePicker(polygon_features, pick_filter_feature_id, pick_filter_feature_kv)
            filter_features = picker.pick()

            if filter_features is None:
                return None

            if len(filter_features) == 0:
                logging.error("No polygon features in the filter matched the selection criteria.")
                return None

            if len(filter_features) > 1:
                logging.error(
                    "Multiple polygon features matched the selection criteria. "
                    "Please refine your selection using --pick-filter-feature-id or --pick-filter-feature-kv."
                )
                print_filter_features(filter_features)
                return None

            filter_feature = filter_features[0]

            filter_shape = shape(filter_feature["geometry"])
            filter_shape = fix_if_required(filter_shape)
            return filter_shape

    except fiona.errors.DriverError as e:
        logging.error(
            f"Error opening filter file {filter_file}: {e}. "
            "If the file type is not recognized, please specify the driver using --filter-file-driver."
        )
        return None


class BaseFilter:
    """Base class for filters with common functionality."""

    def __init__(self, property_renames=None, schema_properties=None):
        self.property_renames = property_renames or {}
        self.schema_properties = schema_properties or {}
        self.count = 0
        self.passed = 0
        self.unparsed = 0
        self.error_count = 0

    def _apply_renames(self, feature):
        """Apply property renames to a feature."""
        if not self.property_renames:
            return feature
        props = feature.get("properties")
        if props:
            for old_key, new_key in self.property_renames.items():
                if old_key in props:
                    props[new_key] = props.pop(old_key)
        return feature

    def _normalize_properties(self, feature):
        """Ensure feature has all schema properties, filling missing with None."""
        if not self.schema_properties:
            return feature
        props = feature.get("properties") or {}
        normalized = {key: props.get(key) for key in self.schema_properties}
        feature["properties"] = normalized
        return feature


class ShapeFilter(BaseFilter):
    """Filters GeoJSON features based on spatial intersection."""

    def __init__(self, filter_shape, property_renames=None, schema_properties=None):
        super().__init__(property_renames, schema_properties)
        self.filter_shape = prep(filter_shape)

    def process(self, line):
        """
        Process a JSON line through the spatial filter.

        Args:
            line: JSON string representing a GeoJSON feature

        Returns:
            Filtered feature dict, or None if filtered out or unparseable
        """
        self.count += 1

        try:
            feature = json.loads(line)
        except json.JSONDecodeError:
            logging.warning(f"Skipping line, could not decode JSON: {line}")
            self.unparsed += 1
            return None

        try:
            feature_shape = shape(feature["geometry"])
            feature_shape = fix_if_required(feature_shape)

            if self.filter_shape.intersects(feature_shape):
                self.passed += 1
                feature = self._apply_renames(feature)
                return self._normalize_properties(feature)
        except Exception as e:
            logging.error(f"An error occurred processing feature: {e}")
            self.error_count += 1

        return None


class PassThroughFilter(BaseFilter):
    """A filter that passes all features through without modification."""

    def __init__(self, property_renames=None, schema_properties=None):
        super().__init__(property_renames, schema_properties)

    def process(self, line):
        """Process a JSON line through the pass-through filter."""
        self.count += 1

        try:
            feature = json.loads(line)
        except json.JSONDecodeError:
            logging.warning(f"Skipping line, could not decode JSON: {line}")
            self.unparsed += 1
            return None

        self.passed += 1
        feature = self._apply_renames(feature)
        return self._normalize_properties(feature)


def create_filter(
    filter_file=None,
    filter_file_driver=None,
    bounds=None,
    pick_filter_feature_id=None,
    pick_filter_feature_kv=None,
    property_renames=None,
    schema_properties=None,
):
    """
    Creates a ShapeFilter from either a filter file or bounds.

    Args:
        filter_file: Path to filter file (optional)
        filter_file_driver: OGR driver name (optional)
        bounds: Bounds string (optional)
        pick_filter_feature_id: Feature ID to pick (optional)
        pick_filter_feature_kv: Key-value pairs to filter features (optional)
        property_renames: Dict mapping old property names to new names (optional)
        schema_properties: Dict of schema properties to normalize features (optional)

    Returns:
        ShapeFilter: Configured filter instance, or None if error
    """
    filter_shape = None

    if filter_file:
        filter_shape = get_shape_from_filter_file(
            filter_file,
            driver=filter_file_driver,
            pick_filter_feature_id=pick_filter_feature_id,
            pick_filter_feature_kv=pick_filter_feature_kv,
        )
    else:
        filter_shape = get_shape_from_bounds(bounds)

    if filter_shape is None:
        return None

    return ShapeFilter(filter_shape, property_renames=property_renames, schema_properties=schema_properties)
