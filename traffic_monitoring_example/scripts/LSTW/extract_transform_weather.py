weather_path = "../../datasets/raw/LSTW/WeatherEvents_Aug16_Dec20_Publish.csv"
DATA_OUT_PATH = "../../datasets/processed/LSTW/Weather/data"
CHUNK_SIZE = 100_000
DATA_SIZE = 5_700_000
H3_RES = 9

from tqdm import tqdm

"""
Goes into main_instantaneous, with the following schema:
(
    h3 h3index,
    value numeric,
    variable text,
    date_id integer, -- needs to be matched to date table (id in form yyyymmdd)
    datetime timestamp,
    dataset_metadata_id integer REFERENCES dataset_metadata (id)
)


h3, value, variable_id, date_id, datetime, dataset_metadata_id

severity: 'light', 'moderate', 'heavy'


"""


import h3pandas
import pandas as pd
import json
from pathlib import Path


import os


# Check if data is accessible
try:
    pd.read_csv(weather_path, chunksize=1)
except Exception:
    print(f"""
    Input data not found at provided location: {weather_path}.
    Please ensure that you run this script from its containing folder.
    """)
    quit()



main_metadata_dict = {

    "time_granularity": "instantaneous",
    "dt_acquired": '2023-02-07 00:00:00',
    "source_name": 'LSTW',
    "original_metadata": {},
    "csv_paths": [Path(DATA_OUT_PATH).stem + '.csv']

}

# Create the export data directory if not exists
out_path_base = DATA_OUT_PATH
try:
    os.makedirs(Path(out_path_base).parents[0])
except OSError as err:
    if "File exists:" in str(err):
        pass

# Remove the previous csv
try:
    os.remove(DATA_OUT_PATH + '.csv')
except Exception:
    pass



# Columns to select from dataframe
use_cols = ['Severity', 'StartTime(UTC)',
       'LocationLat', 'LocationLng']

# Chunked processing of the CSV
# Read and process each chunk individually
with tqdm(total=DATA_SIZE) as pbar:
    with pd.read_csv(weather_path, chunksize=CHUNK_SIZE, usecols=use_cols) as reader:
            for i, chunk in enumerate(reader):
                
                
                # rename the coordinate columns so h3pandas can find them
                # also rename the start time
                chunk = chunk.rename(columns={
                    "LocationLat": "lat", "LocationLng": "lng",
                    "StartTime(UTC)": "datetime"})

                
                # transform the severity to 0, 2, 4 (with reference to traffic severity from same dataset)
                def map_value(x):
                    if x == 'Light': return 0
                    elif x == 'Moderate': return 2
                    elif x == 'Severe': return 4
                    else: return -1 #TODO: filter out these
                chunk['value'] = chunk.Severity.apply(map_value)


                # clean up dataframe
                # filter out nulls and process dates
                # add variable column
                chunk = chunk[
                    (chunk.value.notnull()) & 
                    (chunk.datetime.notnull() &
                    (chunk.datetime != 0))
                    ]
                chunk.datetime = pd.to_datetime(
                    chunk.datetime, format="%Y-%m-%d %H:%M:%S", errors='coerce'
                )
                chunk = chunk.dropna(subset=['datetime']).dropna() # after coercing to NaT, this drops the NaT
                
                chunk['variable'] = 'WEATHER_EVENT'
                chunk['metadata_id'] = 0

                # get h3 as df index, then reset
                # also filter out only the columns to export
                chunk = chunk.h3.geo_to_h3(H3_RES).reset_index(names="h3") 
                chunk = chunk[['h3', 'variable', 'value', 'datetime', 'metadata_id']].reset_index(drop=True)


                # to prevent pandas from inconsistently outputting datetime...
                chunk['datetime'] = chunk['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")

                # chunk csv write
                # columns: h3, value, variable, datetime
                chunk.to_csv(
                    DATA_OUT_PATH + '.csv', mode='a',
                    header = False if i > 0 else True,
                    index=False
                )

                pbar.update(CHUNK_SIZE)

# export metadata
with open(DATA_OUT_PATH + ".mdjson", "w") as f:
        json.dump(main_metadata_dict, f)





