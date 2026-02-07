import json
import logging


class SchemaFilter:
    def __init__(self):
        self.count = 0
        self.passed = 0
        self.unparsed = 0

    def process(self, line):
        self.count += 1
        try:
            feature = json.loads(line)
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
        if "geometry" in feature and feature["geometry"]:
            geom_type = feature["geometry"]["type"]
            self.geom_types.add(geom_type)

        if "properties" in feature and feature["properties"]:
            for key, value in feature["properties"].items():
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

        out_properties = {}
        for key, types in self.properties.items():
            if "int" in types and "float" in types:
                types.remove("int")

            if "NoneType" in types and len(types) == 2:
                types.remove("NoneType")

            if len(types) == 1:
                inferred_type = types.pop()
                if inferred_type == "NoneType":
                    out_properties[key] = "str"
                else:
                    out_properties[key] = inferred_type
            else:
                out_properties[key] = "str"

        return {"geometry": sorted(self.geom_types), "properties": out_properties}
