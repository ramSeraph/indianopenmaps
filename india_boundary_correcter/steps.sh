#!/bin/bash

pip install osm2geojson shapely

mkdir data

python fix_bounds_osm.py

tippecanoe -A '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>' -L'{"file": "data/to_add.geojson", "layer": "to-add"}' -L'{"file": "data/to_del.geojson", "layer": "to-del"}' -o data/osm_corrections.mbtiles

pmtiles convert data/osm_corrections.mbtiles data/osm_corrections.pmtiles
mv data/osm_corrections.pmtiles .
