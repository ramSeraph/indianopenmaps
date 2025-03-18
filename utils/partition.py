#!/usr/bin/env -S uv run 
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "mercantile",
#     "pmtiles",
# ]
# ///

import json
import copy
import sqlite3
from pathlib import Path
from pprint import pprint

import mercantile
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from pmtiles.writer import Writer as PMTilesWriter


def get_tile_type(fmt):
    if fmt == 'pbf':
        return TileType.MVT
    if fmt == 'png':
        return TileType.PNG
    if fmt == 'jpeg':
        return TileType.JPEG
    if fmt == 'webp':
        return TileType.WEBP
    if fmt == 'avif':
        return TileType.AVIF
    return TileType.UNKNOWN


class MissingTileError(Exception):
    pass

class MBTilesSource:
    def __init__(self, fname):
        self.con = sqlite3.connect(fname)

    def get_tile_data(self, tile):
        z = tile.z
        y = (1 << z) - 1 - tile.y
        x = tile.x
        res = self.con.execute(f'select tile_data from tiles where zoom_level={z} and tile_column={x} and tile_row={y};')
        out = res.fetchone()
        if not out:
            raise MissingTileError()
        return out[0]

    def get_tile_size(self, tile):
        data = self.get_tile_data(tile)
        return len(data)
 
    def for_all_z(self, z):
        res = self.con.execute(f'select tile_column, tile_row, tile_data from tiles where zoom_level={z};')
        while True:
            t = res.fetchone()
            if not t:
                break
            x = t[0]
            y = (1 << z) - 1 - t[1]
            data = t[2]
            tile = mercantile.Tile(x=x, y=y, z=z)
            yield (tile, len(data))

    def all(self):
        res = self.con.execute('select zoom_level, tile_column, tile_row, tile_data from tiles;')
        while True:
            t = res.fetchone()
            if not t:
                break
            z = t[0]
            x = t[1]
            y = (1 << z) - 1 - t[2]
            data = t[3]
            tile = mercantile.Tile(x=x, y=y, z=z)
            yield (tile, data)

    def all_sizes(self):
        for t, data in self.all():
            yield (t, len(data))

    def get_metadata(self):
        all_metadata = {}
        for row in self.con.execute("SELECT name,value FROM metadata"):
            k = row[0]
            v = row[1]
            if k == 'json':
                json_data = json.loads(v)
                for k, v in json_data.items():
                    all_metadata[k] = v
                continue
            all_metadata[k] = v

        metadata = {}
        for k in ['type', 'format', 'attribution', 'description', 'name', 'version', 'vector_layers', 'maxzoom', 'minzoom']:
            if k not in all_metadata:
                continue
            metadata[k] = all_metadata[k]

        return metadata
            

# account for some directory overhead
# TODO: this should be dependant on the file size
# TODO: seeing an overhead of 1090 bytes per tile which seems very off.. needs to be investigated
DELTA = 150 * 1024 * 1024
# github release limit
size_limit_bytes = (2 * 1024 * 1024 * 1024) - DELTA
# github git file size limit
#size_limit_bytes = (100 * 1024 * 1024) - DELTA
# cloudflare cache size limit
#size_limit_bytes = (512 * 1024 * 1024) - DELTA
max_level = None
min_level = None
mbtiles_type = 'vector'


def get_layer_info(level, reader):
    tiles = {}
    total_size = 0
    for tile, size in reader.for_all_z(level):
        tiles[tile] = size
        total_size += size
    return total_size, tiles



# TODO: can do better than just vertical slices
def get_buckets(sizes, tiles):
    buckets = []
    bucket_tiles = []

    max_x = max(sizes.keys())
    min_x = min(sizes.keys())

    cb = (min_x, min_x)
    cs = 0
    bts = {}
    for i in range(min_x, max_x + 1):
        if i not in sizes:
            continue
        if cs > size_limit_bytes:
            buckets.append(cb)
            bucket_tiles.append(bts)
            cb = (i,i)
            cs = 0
            bts = {}
        cs += sizes[i]
        bts.update(tiles[i])
        cb = (cb[0], i)
    buckets.append(cb)
    bucket_tiles.append(bts)
    return buckets, bucket_tiles


def get_stripes(min_stripe_level, reader):
    sizes = {}
    tiles = {}
    print(f'striping from level {min_stripe_level}')
    for t, tsize in reader.all_sizes():
        if t.z < min_stripe_level:
            continue
        if t.z == min_stripe_level:
            pt = t
        else:
            pt = mercantile.parent(t, zoom=min_stripe_level)
        if pt.x not in tiles:
            tiles[pt.x] = {}
        tiles[pt.x][t] = tsize
        if pt.x not in sizes:
            sizes[pt.x] = 0
        sizes[pt.x] += tsize
    return sizes, tiles


def get_top_slice(reader):
    print('getting top slice')
    size_till_now = 0
    tiles = {}
    for level in range(min_level, max_level + 1):
        lsize, ltiles = get_layer_info(level, reader)
        size_till_now += lsize
        print(f'{level=}, {lsize=}, {size_till_now=}, {size_limit_bytes=}')
        tiles.update(ltiles)
        if size_till_now > size_limit_bytes:
            return level - 1, tiles
    return max_level, tiles

def save_partition_info(inp_p_info, partition_file):
    p_info = copy.deepcopy(inp_p_info)
    for p_name, p_data in p_info.items():
        tiles_new = { f'{t.z},{t.x},{t.y}':size for t, size in p_data['tiles'].items() }
        p_data['tiles'] = tiles_new

    partition_file.parent.mkdir(exist_ok=True, parents=True)
    partition_file.write_text(json.dumps(p_info))


def get_bounds(tiles):

    bounds = [ mercantile.bounds(t) for t in tiles ]

    max_x = bounds[0].east
    min_x = bounds[0].west
    max_y = bounds[0].north
    min_y = bounds[0].south
    for b in bounds:
        if b.east > max_x:
            max_x = b.east

        if b.west < min_x:
            min_x = b.west

        if b.north > max_y:
            max_y = b.north

        if b.south < min_y:
            min_y = b.south

    return (min_y, min_x, max_y, max_x)


def get_partition_info(reader):
    if to_partition_file.exists():
        partition_info = json.loads(to_partition_file.read_text())
        for suffix, data in partition_info.items():
            tdata = {}
            for k, size in data['tiles'].items():
                kps = k.split(',')
                tile = mercantile.Tile(
                    x=int(kps[1]),
                    y=int(kps[2]),
                    z=int(kps[0]),
                )
                tdata[tile] = size
            data['tiles'] = tdata
        return partition_info

    partition_info = {}
    top_slice_max_level, top_slice_tiles = get_top_slice(reader)

    top_slice_bounds = get_bounds(top_slice_tiles.keys())

    partition_name = f'z{min_level}'
    if min_level != top_slice_max_level:
        partition_name += f'-{top_slice_max_level}'

    partition_info[partition_name] = {
        "tiles": top_slice_tiles,
        "min_zoom": min_level,
        "max_zoom": top_slice_max_level,
        "bounds": top_slice_bounds
    }
    if top_slice_max_level == max_level:
        print('no more slicing required')
        return partition_info


    from_level = top_slice_max_level + 1
    stripe_sizes, stripe_tiles = get_stripes(from_level, reader)
    buckets, bucket_tiles = get_buckets(stripe_sizes, stripe_tiles)

    
    for i,bucket in enumerate(buckets):
        if len(buckets) == 1:
            part_suffix = ''
        else:
            part_suffix = f'-part{i}'
        if from_level != max_level:
            partition_name = f'z{from_level}-{max_level}{part_suffix}'
        else:
            partition_name = f'z{from_level}{part_suffix}'
        partition_info[partition_name] = {
            'tiles': bucket_tiles[i],
            "min_zoom": from_level,
            "max_zoom": max_level,
            "bounds": get_bounds(bucket_tiles[i].keys()),
        }
    return partition_info


def create_pmtiles(partition_info, to_pmtiles_prefix, reader):
    mosaic_data = {}
    writers = {}
    suffix_arr = []
    tiles_to_suffix = {}
    i = 0
    for suffix, data in partition_info.items():
        out_pmtiles_file = f'{to_pmtiles_prefix}{suffix}.pmtiles'
        Path(out_pmtiles_file).parent.mkdir(exist_ok=True, parents=True)
        writer = PMTilesWriter(open(out_pmtiles_file, 'wb'))
        writers[suffix] = writer
        suffix_arr.append(suffix)
        for t in data['tiles'].keys():
            tiles_to_suffix[t] = i
        i += 1

    curr_zs = {}
    max_lats = {}
    min_lats = {}
    max_lons = {}
    min_lons = {}
    min_zooms = {}
    max_zooms = {}
    for suffix in suffix_arr:
        curr_zs[suffix] = None
        max_lats[suffix] = min_lats[suffix] = max_lons[suffix] = min_lons[suffix] = None
        min_zooms[suffix] = max_zooms[suffix] = None
    done = set()

    for t, t_data in reader.all():
        if t in done:
            continue
        suffix = suffix_arr[tiles_to_suffix[t]]
        writer = writers[suffix]
        if curr_zs[suffix] is None or curr_zs[suffix] < t.z:
            max_lats[suffix] = min_lats[suffix] = max_lons[suffix] = min_lons[suffix] = None
            curr_zs[suffix] = t.z
        t_bounds = mercantile.bounds(t)
        if max_lats[suffix] is None or t_bounds.north > max_lats[suffix]:
            max_lats[suffix] = t_bounds.north
        if min_lats[suffix] is None or t_bounds.south < min_lats[suffix]:
            min_lats[suffix] = t_bounds.south
        if max_lons[suffix] is None or t_bounds.east > max_lons[suffix]:
            max_lons[suffix] = t_bounds.east
        if min_lons[suffix] is None or t_bounds.west < min_lons[suffix]:
            min_lons[suffix] = t_bounds.west
        if min_zooms[suffix] is None or min_zooms[suffix] > t.z:
            min_zooms[suffix] = t.z
        if max_zooms[suffix] is None or max_zooms[suffix] < t.z:
            max_zooms[suffix] = t.z
        t_id = zxy_to_tileid(t.z, t.x, t.y)
        writer.write_tile(t_id, t_data)
        done.add(t)

    for suffix in suffix_arr:
        out_pmtiles_file = f'{to_pmtiles_prefix}{suffix}.pmtiles'
        metadata = reader.get_metadata()
        tile_type = get_tile_type(metadata['format'])
        header = {
            "tile_type": tile_type,
            "tile_compression": Compression.GZIP if mbtiles_type == 'vector' else Compression.NONE,
            "min_lon_e7": int(min_lons[suffix] * 10000000),
            "min_lat_e7": int(min_lats[suffix] * 10000000),
            "max_lon_e7": int(max_lons[suffix] * 10000000),
            "max_lat_e7": int(max_lats[suffix] * 10000000),
            "min_zoom": min_zooms[suffix],
            "max_zoom": max_zooms[suffix],
            "center_zoom": max_zooms[suffix],
            "center_lon_e7": int(10000000 * (min_lons[suffix] + max_lons[suffix])/2),
            "center_lat_e7": int(10000000 * (min_lats[suffix] + max_lats[suffix])/2),
        }
        m_header = copy.copy(header)
        m_key = f'../{Path(out_pmtiles_file).name}'
        m_header['tile_type'] = header['tile_type'].value
        m_header['tile_compression'] = header['tile_compression'].value
        writer = writers[suffix]
        print(f'finalizing writing {suffix}')
        writer.finalize(header, metadata)
        mosaic_data[m_key] = { 'header': m_header, 'metadata': metadata }
    return mosaic_data


if __name__ == '__main__':
    import sys

    mbtiles_fname = sys.argv[1]
    if not mbtiles_fname.endswith('.mbtiles'):
        raise Exception('expecting an mbtiles file')
    fname_prefix = mbtiles_fname[:-len('.mbtiles')]

    to_partition_file = Path(f'{fname_prefix}.partition_info.json')
    reader = MBTilesSource(mbtiles_fname)
    metadata = reader.get_metadata()
    pprint(metadata)
    if 'vector_layers' in metadata:
        num_layers = len(metadata['vector_layers'])
        if num_layers != 1:
            raise Exception(f'got {num_layers} layers, expecting 1')
        max_level = metadata['vector_layers'][0]['maxzoom']
        min_level = metadata['vector_layers'][0]['minzoom']
    else:
        max_level = int(metadata['maxzoom'])
        min_level = int(metadata['minzoom'])
        mbtiles_type = 'raster'

    partition_info = get_partition_info(reader)
    if not to_partition_file.exists():
        save_partition_info(partition_info, to_partition_file)

    mosaic_data = create_pmtiles(partition_info, f'{fname_prefix}_', reader)
    pprint(mosaic_data)
    Path(f'{fname_prefix}.mosaic.json').write_text(json.dumps(mosaic_data))


