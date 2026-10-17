import json

from iomaps.commands.admin_prepare import filter_empty_geometries


def test_cleaning_preserves_nonempty_geometry_collections(tmp_path):
    point = {"type": "Point", "coordinates": [77.2, 28.6]}
    collection = {"type": "GeometryCollection", "geometries": [point]}
    nested = {"type": "GeometryCollection", "geometries": [collection]}
    empty = {"type": "GeometryCollection", "geometries": []}
    geometries = [collection, nested, empty, {"type": "GeometryCollection", "geometries": [empty]}, None]
    features = [
        {"type": "Feature", "properties": {"id": i}, "geometry": geometry}
        for i, geometry in enumerate(geometries)
    ]
    source = tmp_path / "plots.geojsonl"
    original = "".join(json.dumps(feature) + "\n" for feature in features)
    source.write_text(original)

    total, kept, cleaned = filter_empty_geometries(source)

    assert (total, kept) == (5, 2)
    assert [json.loads(line) for line in cleaned.read_text().splitlines()] == features[:2]
    assert source.read_text() == original
