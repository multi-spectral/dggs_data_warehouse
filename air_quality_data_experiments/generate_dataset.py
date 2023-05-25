"""

The goal of this file is to generate
A simulated dataset which can be used
To reproduce the experiment of Kang et al. 2015,
For which the paper can be found at:
http://dx.doi.org/10.1080/17538947.2014.962999

This script simply generates a dataset
Consisting of grids of N h3 tiles in the country of France
With randomly generated values,
For which M grids are available along the time dimension.

According to the authors,
"We used a database containing 2411 grids 
(with a resolution of 100 × 100, i.e. 10,000cells)."

There is no time resolution given in the paper,
But an assumption of hourly data can be fairly made,
Given that the dataset would then contain 100 days and 11 hours of data.

To get the France boundaries, Geopandas was used, and 
The French country centroid, found at:
https://developers.google.com/public-data/docs/canonical/countries_csv

"""

csv_out_folder = "out"
metadata_out_path = "out/airq.mdjson"

H3_RES = 7

import pandas as pd
import geopandas as gpd
import h3
import shapely
from shapely.geometry import Point
import json
from scipy.stats import truncnorm
import numpy as np
from datetime import datetime, timedelta

import os
from pathlib import Path
from tqdm import tqdm

print("Loading France shapefile...")
world_boundaries_path = gpd.datasets.get_path("naturalearth_lowres")
boundaries = gpd.read_file(world_boundaries_path)



try:
    os.makedirs(Path(csv_out_folder))
except OSError as err:
    if "File exists:" in str(err):
        pass


print("Tiling France boundaries...")
# From the France multipolygon, choose the French mainland
# this is stored at index 1
# search for the one containing the paris point
paris_lat, paris_lng = 46.227638, 2.213749
paris = Point(paris_lng, paris_lat)

france_mainland = None
for geom in boundaries[boundaries['name'] == 'France'].iloc[0].geometry.geoms:
    if geom.contains(paris):
        france_mainland = geom
        break



# Now tile the French mainland, and get only the first 100_000 cells
france_geojson = json.loads(shapely.to_geojson(france_mainland))
h3_tiles = list(h3.polyfill(france_geojson, H3_RES))[0:100_000]
del france_geojson


# Now define the procedure for generating one grid of sample data
# Use truncated normal distribution
# For truncnorm use see: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.truncnorm.html
def generate_airq_data(tiles_list):
    
    def generate_children_data(sz):
        
        mean = 26
        std = 7
        left = 0
        right = 150
        a,b = (left - mean)/std, (right - mean)/std

        return truncnorm.rvs(a, b, size=sz)

    

    # filter to only the desired tiles
    return {
        "h3": h3_tiles,
        "value": generate_children_data(100_000)
    }
        



# Now generate data for each grid,
# And export in batches of grids.
# Also add all necessary columns
# For the import.
dfs = []
start_dt = datetime(2015,1,1,12,0,0)
print("Generating and exporting data...")
header = True
mode = 'w'
paths = []
BATCH_SIZE = 300 # in records
for i in tqdm(range(2411)): # 2411 x 100_000 rows,

    path_addendum = str(int(i/BATCH_SIZE)) # the addendum is the csv file number
    path = "airq_" + path_addendum + ".csv"
    
    if i % BATCH_SIZE == 0:
        header, mode = True, 'w'
        paths.append(str(path))
    else: 
        header, mode = False, 'a'

    dt = start_dt + timedelta(hours=i*1) #one hour each...previously was 4.
    data = generate_airq_data(h3_tiles)

    df = pd.DataFrame(data,columns=['h3', 'value']) \
            .assign(datetime=dt)

    df = df.assign(variable='AIRQ',metadata_id=0)

    # to prevent pandas from inconsistently outputting datetime...
    df['datetime'] = df['datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
    
    write_path = Path(csv_out_folder) / path
    df[['h3', 'variable', 'value', 'datetime', 'metadata_id']] \
        .to_csv(write_path,header=header, mode=mode,index=False)
 



# export metadata
main_metadata_dict = {

    "time_granularity": "instantaneous",
    "dt_acquired": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    "source_name": 'Simulated Air Quality Data',
    "original_metadata": {},
    "csv_paths": paths

}
with open(metadata_out_path, "w") as f:
        json.dump(main_metadata_dict, f)
