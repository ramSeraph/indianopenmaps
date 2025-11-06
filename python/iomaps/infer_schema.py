import json
import logging


class SchemaFilter:
    def __init__(self, limit_to_geom_type=None, strict_geom_type_check=False):
        self.count = 0
        self.passed = 0
        self.unparsed = 0
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
            if self.limit_to_geom_type == "Polygon" and feature_geom_type == "MultiPolygon":
                return True
            if self.limit_to_geom_type == "LineString" and feature_geom_type == "MultiLineString":
                return True
            if self.limit_to_geom_type == "Point" and feature_geom_type == "MultiPoint":
                return True
            return False

    def process(self, line):
        self.count += 1
        try:
            feature = json.loads(line)
            geom_type = feature.get('geometry', {}).get('type', None)
            if not self._geom_type_matches(geom_type):
                return None
            self.passed += 1
        except json.JSONDecodeError:
            logging.warning(f"Skipping line, could not decode JSON: {line}")
            self.unparsed += 1
            return None

        return feature  


class SchemaWriter:
    def __init__(self):
        self.geom_types = set()
        self.properties = {}

    def write(self, feature):
        if 'geometry' in feature and feature['geometry']:
            geom_type = feature['geometry']['type']
            self.geom_types.add(geom_type)

        if 'properties' in feature and feature['properties']:
            for key, value in feature['properties'].items():
                if key not in self.properties:
                    self.properties[key] = set()
                self.properties[key].add(type(value).__name__)

    def size(self):
        return 0

    def close(self):
        pass

    def get_schema(self):
        if len(self.geom_types) == 0:
            logging.warning("No geometry types found in features.")
            return None

        if len(self.geom_types) > 1:
            if len(self.geom_types) == 2 and 'Point' in self.geom_types and 'MultiPoint' in self.geom_types:
                logging.info("Consolidating Point and MultiPoint to MultiPoint.")
                self.geom_types = {'MultiPoint'}
            elif len(self.geom_types) == 2 and 'LineString' in self.geom_types and 'MultiLineString' in self.geom_types:
                logging.info("Consolidating LineString and MultiLineString to MultiLineString.")
                self.geom_types = {'MultiLineString'}
            elif len(self.geom_types) == 2 and 'Polygon' in self.geom_types and 'MultiPolygon' in self.geom_types:
                logging.info("Consolidating Polygon and MultiPolygon to MultiPolygon.")
                self.geom_types = {'MultiPolygon'}
            else:
                logging.error(f"Incompatible geometry types found, cannot consolidate. Types: {self.geom_types}, Consider limiting which geometries to include.. using the -g/--limit-to-geometry-type option.")
                return None

        out_geometry_type = self.geom_types.pop()
        out_properties = {}
        for key, types in self.properties.items():
            if 'int' in types and 'float' in types:
                types.remove('int')

            if 'NoneType' in types and len(types) == 2:
                types.remove('NoneType')

            if len(types) == 1:
                inferred_type = types.pop()
                if inferred_type == 'NoneType':
                    out_properties[key] = 'str'
                else:
                    out_properties[key] = inferred_type
            else:
                out_properties[key] = 'str'

        return {
            'geometry': out_geometry_type,
            'properties': out_properties
        }
