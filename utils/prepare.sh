#!/bin/bash

file=$1
src_url=$2
src_name=$3
is_points_str=$4


usage="$0 <geojsonl file> <src_url> <src_name> [yes]"

fbase=$(basename $file)

[[ $fbase != *.geojsonl ]] && echo "ERROR: $file not a geojsonl file" && echo $usage && exit 1
lname=${fbase%.geojsonl}

[[ $src_url == '' ]] && echo "ERROR: src url not specified" && echo $usage && exit 1
[[ $src_name == '' ]] && echo "ERROR: src url not specified" && echo $usage && exit 1

dir=$(dirname $file)

set -x
cd $dir
if [[ $NO_ZIP != 1 ]]; then
  echo "creating archive"
  7z a -m0=PPMd ${fbase}.7z ${fbase}
fi

# not for points

cmd="coalesce-smallest-as-needed"
if [[ $is_points_str == yes ]]; then
    cmd="drop-densest-as-needed"
fi

echo "creating mbtiles file"
tippecanoe -P --increase-gamma-as-needed -zg  -o ${lname}.mbtiles --simplify-only-low-zooms  --$cmd  --extend-zooms-if-still-dropping -n $lname -l $lname -A 'Source: <a href="'$src_url'" target="_blank" rel="noopener noreferrer">'"$src_name"'</a>' $fbase

if [[ $NO_PMTILES != 1 ]]; then
  echo "converting to pmtiles"
  pmtiles convert ${lname}.mbtiles ${lname}.pmtiles
fi

