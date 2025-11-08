Deployment to fly.io for the indianopenmaps domain

A display server for the data hosted at the following repos:
* [indian_admin_boundaries](https://github.com/ramSeraph/indian_admin_boundaries)
* [indian_water_features](https://github.com/ramSeraph/indian_water_features)
* [indian_railways](https://github.com/ramSeraph/indian_railways)
* [indian_roads](https://github.com/ramSeraph/indian_roads)
* [indian_communications](https://github.com/ramSeraph/indian_communications)
* [indian_facilities](https://github.com/ramSeraph/indian_facilities)
* [indian_cadastrals](https://github.com/ramSeraph/indian_cadastrals)
* [indian_land_features](https://github.com/ramSeraph/indian_land_features)
* [india_natural_disasters](https://github.com/ramSeraph/india_natural_disasters)
* [indian_buildings](https://github.com/ramSeraph/indian_buildings)
* [indian_power_infra](https://github.com/ramSeraph/indian_power_infra)

Contains [code](https://github.com/ramSeraph/indianopenmaps/blob/main/server/mosaic_handler.js) for getting tiles from a big pmtiles file which has been split into multiple shards( to overcome hosting size limits? ).

Tools for splitting a big pmtiles into smaller ones is at [pmtiles_mosaic](https://github.com/ramSeraph/pmtiles_mosaic).

See the list of data available at https://indianopenmaps.fly.dev/

A tool is available to filter the large .7z files in the repo based on another polygon shape or bounds and export them in other geospatial formats at [iomaps](https://github.com/ramSeraph/indianopenmaps/tree/main/python)

