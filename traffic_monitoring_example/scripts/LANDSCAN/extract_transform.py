cities_path = "../../datasets/raw/LANDSCAN/simplemaps_uscities.csv"

data_paths = [
    "../../datasets/raw/LANDSCAN/landscan-usa-2018-day-assets/landscan-usa-2018-day/landscan-usa-2018-conus-day.tif",
    "../../datasets/raw/LANDSCAN/landscan-usa-2019-day-assets/landscan-usa-2019-day/landscan-usa-2019-conus-day.tif",
    "../../datasets/raw/LANDSCAN/landscan-usa-2020-day-assets/landscan-usa-2020-day/landscan-usa-2020-conus-day.tif",
    "../../datasets/raw/LANDSCAN/landscan-usa-2021-day-assets/landscan-usa-2021-day/landscan-usa-2021-conus-day.tif",
    "../../datasets/raw/LANDSCAN/landscan-usa-2018-night-assets/landscan-usa-2018-night/landscan-usa-2018-conus-night.tif",
    "../../datasets/raw/LANDSCAN/landscan-usa-2019-night-assets/landscan-usa-2019-night/landscan-usa-2019-conus-night.tif",
    "../../datasets/raw/LANDSCAN/landscan-usa-2020-night-assets/landscan-usa-2020-night/landscan-usa-2020-conus-night.tif",
    "../../datasets/raw/LANDSCAN/landscan-usa-2021-night-assets/landscan-usa-2021-night/landscan-usa-2021-conus-night.tif",
    ]
metadata_paths = [
    "../../datasets/raw/LANDSCAN/landscan-usa-2018-day-assets/conus_day_metadata.xml",
    "../../datasets/raw/LANDSCAN/landscan-usa-2019-day-assets/conus_day_metadata.xml",
    "../../datasets/raw/LANDSCAN/landscan-usa-2020-day-assets/conus_day_metadata.xml",
    "../../datasets/raw/LANDSCAN/landscan-usa-2021-day-assets/conus_day_metadata.xml",
    "../../datasets/raw/LANDSCAN/landscan-usa-2018-night-assets/conus_night_metadata.xml",
    "../../datasets/raw/LANDSCAN/landscan-usa-2019-night-assets/conus_night_metadata.xml",
    "../../datasets/raw/LANDSCAN/landscan-usa-2020-night-assets/conus_night_metadata.xml",
    "../../datasets/raw/LANDSCAN/landscan-usa-2021-night-assets/conus_night_metadata.xml",
    ]
DATA_OUT_PATH = "../../datasets/processed/LANDSCAN/"
H3_RES = 9

import polars as pl
import pandas as pd
import h3pandas
import h3
import rasterio as rio
from rasterstats import zonal_stats
import geopandas as gpd
from pathlib import Path
import json
import xmltodict
import os
import warnings 

from shapely.errors import ShapelyDeprecationWarning
warnings.filterwarnings("ignore", category=ShapelyDeprecationWarning)



# check if accessible

try:
    pd.read_csv(cities_path,chunksize=1)
except Exception:
    print(f"Input data not found: {cities_path}. Make sure to run this script from its containing folder.")
    quit()
#TODO: instead of lists, put a dict organizing all the file paths



# Create the export data directory if not exists
out_path_base = DATA_OUT_PATH
try:
    os.makedirs(Path(out_path_base))
except OSError as err:
    if "File exists:" in str(err):
        pass



main_metadata_dict = {

    "time_granularity": "year",
    "dt_acquired": '2022-11-18 00:00:00',
    "source_name": 'Landscan'

}



# Part 1: sample the cities data and get top level h3 res
top_level_h3_res = 5
num_cities = 50
cities_top_h3 = pl.read_csv(cities_path) \
            .sort(["population"], descending=True) \
            .slice(0, num_cities) \
            .to_pandas() \
            .h3.geo_to_h3(top_level_h3_res) \
            .reset_index(names="h3") \
            ['h3'].unique()

# Part 2: get a list of all the child indexes
children = []
for h in cities_top_h3:
    children = children + list(h3.h3_to_children(h, H3_RES))

children = list(set(children))

# Part 3: get the polygons
# TODO: wrap in module to make single function, or do Polars in Rust
boundaries_df = pd.DataFrame(children, columns = ['h3']) \
                    .set_index(['h3']) \
                    .h3.h3_to_geo_boundary() \
                    .reset_index()



for i, data_path in enumerate(data_paths):

    # Prepare pathname
    pathname = Path(data_path).stem
    year = int(pathname.split('-')[2])
    daynight = pathname.split('-')[4]
    out_path_base = Path(DATA_OUT_PATH) / str(pathname)

    #Read metadata from each xml file
    #Add in the main metadata dictionary
    with open(metadata_paths[i]) as f:
        metadata_xml = f.read()
    metadata_dict =  {"original_metadata": xmltodict.parse(metadata_xml)}
    metadata_dict.update(main_metadata_dict)
    metadata_dict.update({'csv_paths':  [str(pathname + ".csv")]})

                                   


    with rio.open(data_path) as reader:
        raster_crs = reader.crs


    boundaries = gpd.GeoDataFrame(
                        boundaries_df, 
                        geometry='geometry', 
                        crs='EPSG:4326') \
                    .to_crs(raster_crs) \
                    .__geo_interface__




    # Part 4: load the image, reproject, and sample


    # Use zonal_stats to calculate sum of all pixels touched by each polygon
    # apparently zonal_stats uses lazy reading: see documentation for gen_zonal_stats
    # https://github.com/perrygeo/python-rasterstats/blob/d188eaf1f1c20c3ef33aad407f55f9fce51a1220/src/rasterstats/main.py
    # Uses less memory than reading in the entire raster
    # TODO: can this whole thing be done in Rust?
    print("doing zonal stats")
    stats = pd.DataFrame(
                zonal_stats(
                    boundaries, 
                    data_path, all_touched=True, stats=['sum'])
            )


    # Join with h3 keys. Order is the same
    # Also clean up the columns
    concat_df = pl.from_pandas(
                    pd.concat([boundaries_df, stats], axis=1)
                        .drop(columns=["geometry"])
                ).with_columns(
                    pl.lit(year).alias('year'),
                    pl.lit('LANDSCAN_' + daynight).alias('variable'),
                    pl.lit(0).alias('metadata_id')
                ).rename({"sum": "value"})

    print(concat_df)


    # Export this and the metadata to file
    # Make the out path
    # TODO: create if not exists
    # Write data

    concat_df[['h3', 'variable', 'value', 'year', 'metadata_id']] \
                .write_csv(out_path_base.with_suffix(".csv"))

    with open(out_path_base.with_suffix(".mdjson"), "w") as f:
            json.dump([metadata_dict], f)