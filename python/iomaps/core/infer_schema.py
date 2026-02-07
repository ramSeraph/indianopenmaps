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
            return None, {}

        # Detect case-insensitive duplicates and create rename mapping
        seen_lower = {}  # maps lowercase -> canonical key
        renames = {}  # maps original key -> renamed key
        for key in self.properties.keys():
            key_lower = key.lower()
            if key_lower not in seen_lower:
                seen_lower[key_lower] = key
            else:
                # Find a unique name by appending _2, _3, etc.
                suffix = 2
                while f"{key}_{suffix}".lower() in seen_lower:
                    suffix += 1
                new_key = f"{key}_{suffix}"
                renames[key] = new_key
                seen_lower[new_key.lower()] = new_key
                logging.warning(
                    f"Duplicate property name (case-insensitive): '{key}' conflicts with "
                    f"'{seen_lower[key_lower]}', renaming to '{new_key}'"
                )

        out_properties = {}
        for key, types in self.properties.items():
            # Use renamed key if applicable
            out_key = renames.get(key, key)

            # Make a copy to avoid modifying the original set
            types = types.copy()

            if "int" in types and "float" in types:
                types.remove("int")

            if "NoneType" in types and len(types) == 2:
                types.remove("NoneType")

            if len(types) == 1:
                inferred_type = types.pop()
                if inferred_type == "NoneType":
                    out_properties[out_key] = "str"
                else:
                    out_properties[out_key] = inferred_type
            else:
                out_properties[out_key] = "str"

        return {"geometry": sorted(self.geom_types), "properties": out_properties}, renames
