import json
from pathlib import Path
from osm2geojson import overpass_call, xml2geojson
from shapely.geometry import shape, mapping
from shapely import intersection, difference, union_all

DATA_DIR = 'data/'

def save_as_geojson(query, path):
    xml = overpass_call(query)
    geojson = xml2geojson(xml)
    path.write_text(json.dumps(geojson))


def get_shape(rel_id):
    rel_query = f'rel({rel_id}); out body; >; out skel qt;'
    rel_path = Path(f'{DATA_DIR}{rel_id}.geojson')
    if not rel_path.exists():
        save_as_geojson(rel_query, rel_path)
    data = json.loads(rel_path.read_text())
    feats = data['features']
    rel_feats = [ f for f in feats if f['properties']['type'] == 'relation' ]
    if len(rel_feats) != 1:
        raise Exception('unexpected number of polygons')
    return shape(rel_feats[0]['geometry'])

def write_geojson(shapes, path):
    feats = []
    for shape in shapes:
        feat = { 'type': 'Feature', 'properties': {} }
        feat['geometry'] = mapping(shape)
        feats.append(feat)
    out = { 'type': 'FeatureCollection', 'features': feats }
    path.write_text(json.dumps(out))


if __name__ == '__main__':
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    rels_data = json.loads(Path('rels_osm.json').read_text())
    rel_ind = rels_data['India']
    rel_pak = rels_data['Pakistan']
    ind_shape = get_shape(rel_ind)
    pak_shape = get_shape(rel_pak)
    ind_shape_bound = ind_shape.boundary
    pak_shape_bound = pak_shape.boundary
    extra_shapes = {}
    for rel_name, rel_id in rels_data['extras'].items():
        extra_shapes[rel_name] = { 'shape': get_shape(rel_id), 'rel_id': rel_id }

    ind_official_shape = union_all([ind_shape] + [ e['shape'] for e in extra_shapes.values() ])
    ind_official_shape_bound = ind_official_shape.boundary
    official_diff_shape_bound = difference(ind_official_shape_bound, ind_shape_bound)
    official_diff_shape_bound = difference(official_diff_shape_bound, pak_shape_bound)
    write_geojson([official_diff_shape_bound], Path(f'{DATA_DIR}to_add.geojson'))

    ind_intersect_official = ind_shape_bound.intersection(ind_official_shape)
    pak_intersect_official = pak_shape_bound.intersection(ind_official_shape)

    to_del = []
    to_del.append(difference(ind_intersect_official, ind_official_shape_bound))
    to_del.append(difference(pak_intersect_official, ind_official_shape_bound))
    write_geojson(to_del, Path(f'{DATA_DIR}to_del.geojson'))
    








