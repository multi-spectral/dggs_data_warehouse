import fiona
import geopandas as gpd
import pandas as pd
import h3
from pathlib import Path
import os
import json

from tqdm import tqdm

import reusable_etl_functionality.functionality as etl_tools

H3_RES = 9
DATA_OUT_PATH = "../../datasets/processed/AADT/France/"

data_paths = [
    "../../datasets/raw/AADT/France/tmja-2017/TMJA2017_SHP.shp",
    "../../datasets/raw/AADT/France/tmja-2018/TMJA2018.shp",
    "../../datasets/raw/AADT/France/tmja-2019/TMJA2019.shp"
]

# Create the export data directory if not exists (correct way)
out_path_base = DATA_OUT_PATH
etl_tools.make_data_directory(DATA_OUT_PATH)



#TODO: dict metadata

metadata = [
{
    "original_metadata": {
        "url": "https://static.data.gouv.fr/resources/trafic-moyen-journalier-annuel-sur-le-reseau-routier-national/20190206-140547/tmja-2017-shp.zip"
    },
    "time_granularity": "year",
    "dt_acquired": '2023-05-01 09:00:00',
    "source_name": 'France Traffic AADT Data'
},
{
    "original_metadata": {
        "url": "https://static.data.gouv.fr/resources/trafic-moyen-journalier-annuel-sur-le-reseau-routier-national/20200601-112539/tmja2018-shp.zip"
    },
    "dt_acquired": '2023-05-01 09:00:00',
    "time_granularity": "year",
    "source_name": 'France Traffic AADT Data'
},
{

    "original_metadata": {
        "url": "https://static.data.gouv.fr/resources/trafic-moyen-journalier-annuel-sur-le-reseau-routier-national/20211222-170254/tmja2019-shp.zip"
    },
    "dt_acquired": '2023-05-01 09:00:00',
    "time_granularity": "year",
    "source_name": 'France Traffic AADT Data',
}]

for i in range(len(data_paths)):

    year=2017 + i
    print(year)
    path = data_paths[i]

    # handle metadata
    metadata_dict = metadata[i]
    metadata_dict.update({
        "csv_paths": [f"{year}.csv"]
    })

    

    with fiona.open(path) as handle:
        length = len(handle)
        print(length)




    # read and convert to csv
    df = gpd.read_file(
            path,
    )
    try:
        df = df.to_crs("EPSG:4326").dropna()
    except ValueError as e:
        if df.crs is None:
            df = df.set_crs("IGNF:LAMB93").to_crs("EPSG:4326").dropna()
        else:
            raise e

    # get representative points
    df.geometry = df.geometry.apply(lambda l: l.representative_point())

    # get h3
    df = df.assign(h3=df.geometry.apply(etl_tools.get_h3))
        


    total_df = df
    print(df)
    total_df = total_df.assign(metadata_id=0,variable='AADT',year=year).rename(columns={"TMJA": "value","tmja": "value"})
    total_df = total_df[['h3', 'variable', 'value', 'year', 'metadata_id']].reset_index(drop=True)

    # output csv
    total_df.to_csv(
                    Path(DATA_OUT_PATH) / (str(year) + '.csv'), 
                    index=False
                )
    metadata


    # export metadata
    with open(Path(DATA_OUT_PATH) / (str(year) + '.mdjson'), "w") as f:
            json.dump(metadata_dict, f)
