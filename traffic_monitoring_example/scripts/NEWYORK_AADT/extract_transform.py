import fiona
import geopandas as gpd
import pandas as pd
import h3
from pathlib import Path
import os
import json

from tqdm import tqdm

"""
ogr2ogr -f "ESRI Shapefile" 2019.shp NYSDOT_TDV_AADT_2019.gdb -sql "select AADTLastAct AS value FROM NYSDOT_TDV_AADT_2019" 
ogr2ogr -f "ESRI Shapefile" 2020.shp AADT_2020.gdb -sql "select  MAT_ALH_2020_csv_AADT AS value FROM Traffic_Station_Locations_AADT2020"
ogr2ogr -f "ESRI Shapefile" 2021.shp AADT_2021.gdb -sql "select  MAT_ALH_2021_csv_AADT AS value FROM Traffic_Station_Locations_AADT2021"


"""
path_2019 = "../../datasets/raw/AADT/New York/2019.shp"
path_2020 = "../../datasets/raw/AADT/New York/2020.shp"
path_2021 = "../../datasets/raw/AADT/New York/2021.shp"

data_paths = [path_2019, path_2020, path_2021]
H3_RES = 9
CHUNK_SIZE = 3500

DATA_OUT_PATH = "../../datasets/processed/AADT/New York/"


# Create the export data directory if not exists (correct way)
out_path_base = DATA_OUT_PATH
try:
    os.makedirs(Path(out_path_base))
except OSError as err:
    if "File exists:" in str(err):
        pass

def get_h3(row):

    lat = row.y
    lng = row.x

    return h3.geo_to_h3(lat, lng, H3_RES)

main_metadata_dict = main_metadata_dict = {

    "time_granularity": "year",
    "dt_acquired": '2023-01-01 00:00:00',
    "source_name": 'New York Traffic Data',

}

for i in range(len(data_paths)):

    year=2019 + i
    print(year)
    path = data_paths[i]

    # handle metadata
    metadata_dict = main_metadata_dict
    metadata_dict.update({
        "original_metadata": {
            "url": "https://www.dot.ny.gov/tdv"
        },
        "csv_paths": [f"{year}.csv"]
    })

    

    with fiona.open(path) as handle:
        length = len(handle)


    total_df_list = []
    for i in tqdm(range(0, length, CHUNK_SIZE)):

        # read and convert to csv
        df = gpd.read_file(
            path,
            rows=slice(i,i+CHUNK_SIZE,1), #slice format: start, stop, step
            crs=26918,
            ).to_crs("EPSG:4326").dropna()

        # get representative points
        df.geometry = df.geometry.apply(lambda l: l.representative_point())

        # get h3
        df = df.assign(h3=df.geometry.apply(get_h3))
        
        # concat
        total_df_list.append(df.drop('geometry', axis=1))

    total_df = pd.concat(total_df_list)
    total_df = total_df.assign(metadata_id=0,variable='AADT',year=year)
    total_df = total_df[['h3', 'variable', 'value', 'year', 'metadata_id']].reset_index(drop=True)

    # output csv
    total_df.to_csv(
                    Path(DATA_OUT_PATH) / (str(year) + '.csv'), 
                    index=False
                )


    # export metadata
    with open(Path(DATA_OUT_PATH) / (str(year) + '.mdjson'), "w") as f:
            json.dump(main_metadata_dict, f)
