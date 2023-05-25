md_aadt_path = "../../datasets/raw/AADT/Maryland/MDOT_SHA_Annual_Average_Daily_Traffic_(AADT)/MDOT_SHA_Annual_Average_Daily_Traffic__AADT__Locations.shp"

RES_H3=9
DATA_OUT_PATH = "../../datasets/processed/AADT/Maryland/data"

import geopandas as gpd
import h3pandas
import pandas as pd
import polars as pl
from pathlib import Path
import json

import traceback

import itertools
from datetime import datetime

import os

import reusable_etl_functionality.functionality as etl_tools


# test if file accessible
try:
    gpd.read_file(md_aadt_path,rows=1)
except Exception:
    print(f"Input data not found: {md_aadt_path}. Make sure to run this script from its containing folder.")
    quit()
#TODO: instead of lists, put a dict organizing all the file paths




# Configure file metadata


main_metadata_dict = {

    "time_granularity": "year",
    "dt_acquired": '2023-01-01 00:00:00',
    "source_name": 'Maryland',
    "csv_paths": ['data.csv']

}

# Create the export data directory if not exists
out_path_base = DATA_OUT_PATH

etl_tools.make_data_directory(out_path_base)

id_vars=['NUM_LANES']
value_vars= [
    'AADT_2012','AADT_2013','AADT_2014',
    'AADT_2015','AADT_2016','AADT_2017',
    'AADT_2018', 'AADT_2019', 'AADT_2020']


# INITIAL PROCESSING

# Read in the aadt data
gdf = gpd.read_file(md_aadt_path) 
gdf_converted = gdf \
                .drop(
                    gdf.columns.difference(id_vars + value_vars + ['geometry']),
                    axis = 1) \
                .to_crs("EPSG:4326") \
                .h3.geo_to_h3(RES_H3) \
                .drop(columns=['geometry']) \
                .reset_index(names=["h3"]) 

# Transform to polars and convert dtype
df = pl.from_pandas(gdf_converted) \
        .with_columns([pl.col("NUM_LANES").cast(pl.Int32)])
 
# PART 1 
# pivot the dataframe into long form
# each of the variable_columns becomes a value of 'variable'
# preserve the metadata columns
df_long = df.melt(
    id_vars=id_vars + ['h3'],
    value_vars= value_vars,
    variable_name="variable",
    value_name="value"
).drop_nulls().to_pandas()

print(df_long)
print(id_vars)


# pass the df and the list of metadata columns to extract_metadata
# this identifies unique combinations of the metadata values
# then, a separate version of the metadata entries is created for 
df_long, metadata_entries = etl_tools.extract_categorical_variables_as_metadata(df_long, id_vars)
print(df_long)
print(metadata_entries)

df_long = pl.from_pandas(df_long)

# map to dictionary

metadata_entries = [{"original_metadata": d} for d in metadata_entries]
# add in the global metadata
for e in metadata_entries:
    e.update(main_metadata_dict)




# clean up the year and variable columns, and add the metadata id
df_long = df_long.with_columns(
    pl.col('variable').apply(lambda v: int(str(v)[-4:])).alias('year'),
    pl.lit('AADT').alias('variable')
    )




# Write data

df_long[['h3', 'variable', 'value', 'year', 'metadata_id']] \
             .write_csv(out_path_base + ".csv")

with open(out_path_base + ".mdjson", "w") as f:
        json.dump(metadata_entries, f)





