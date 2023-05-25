import polars as pl
import h3
import os
import json

from pathlib import Path

H3_RES = 9
DATA_OUT_PATH = "../../datasets/processed/AADT/UK/"
aadt_variable_name = 'All_motor_vehicles'

data_path = "../../datasets/raw/AADT/UK/dft_traffic_counts_aadf.csv"

# Check if data is accessible
if not os.path.exists(data_path):
    print(f"""
    Input data not found at provided location: {data_path}.
    Please ensure that you run this script from its containing folder.
    """)
    quit()

# Create data out folder if not available yet
try:
    os.makedirs(DATA_OUT_PATH)
except OSError as err:
    if "File exists:" in str(err):
        pass


# Read dataframe with wanted columns and filter by wanted columns and years
df = pl.read_csv(
    data_path,
    columns=['Year','Latitude','Longitude',aadt_variable_name]
    ).filter(
    pl.col('Year').is_in([2016,2017,2018,2019,2020])
    )

# Apply h3 function to each row
# Please refer to solution approach here: https://stackoverflow.com/questions/74433918/apply-a-function-to-2-columns-in-polars

df = df.with_columns(
    pl.struct(['Latitude','Longitude']).apply(
        lambda x: h3.geo_to_h3(x['Latitude'], x['Longitude'], H3_RES)
        ).alias('h3'),
        pl.lit('AADT').alias('variable'),
        pl.lit('1').alias('metadata_id'))

# Rename and rearrange columns
df = df.rename(
    {aadt_variable_name: 'value',
     'Year': 'year'}
    ).select([pl.col(x) for x in ['h3', 'variable', 'value', 'year', 'metadata_id']])

# Export csv
df.write_csv(Path(DATA_OUT_PATH) / ('data' + '.csv'))

# Construct metadata dictionary
main_metadata_dict = {

    "time_granularity": "year",
    "dt_acquired": '2023-04-17 10:00:00',
    "source_name": 'UK Traffic AADT Data',
    "original_metadata": {
    "url": "https://storage.googleapis.com/dft-statistics/road-traffic/downloads/data-gov-uk/dft_traffic_counts_aadf.zip"
    },
    "csv_paths": ["data.csv"]
    
}

# Export metadata to file
with open(Path(DATA_OUT_PATH) / ('data' + '.mdjson'), "w") as f:
            json.dump(main_metadata_dict, f)