
"""



"""

import h3

import pandas as pd
import h3pandas
import geopandas as gpd



from io import BytesIO
from urllib.request import urlopen
from fiona.io import ZipMemoryFile

import matplotlib.pyplot as plt

from clickhouse_driver import Client




ISO_A3 = 'USA' # or 'USA'
H3_RES = 6

# get the world boundaries
world_boundaries_path = gpd.datasets.get_path("naturalearth_lowres")
boundaries = gpd.read_file(world_boundaries_path)

# get Great Britain geojson snippet
geometry = boundaries[boundaries['iso_a3'] == ISO_A3].explode(index_parts=True).__geo_interface__
polygon_geojsons = [f['geometry'] for f in geometry['features']]



# pass the geojson snippet to h3
polygons = [h3.polyfill(poly,H3_RES,True) for poly in polygon_geojsons]
polygons = list(set(a for b in polygons for a in b)) # flatten and unique the list


db_client = Client(host="localhost", database="traffic_monitoring")
query = f"""
    WITH res AS (
        select h3ToParent(m.h3,{H3_RES}) as h3_d, count(*) as traffic_ct
            FROM main_instantaneous AS m
            JOIN date as d
            ON d.date_id = m.date_id
            And d.year = 2018
            WHERE m.variable = 'TRAFFIC_EVENT'
            group by h3_d
            HAVING traffic_ct > 0
            )
    SELECT h3ToString(h3_d), traffic_ct FROM res;
    """


result = db_client.execute(query)


# convert to dataframe
h3_colname = 'h3_d'
df = pd.DataFrame(result, columns=[h3_colname, 'traffic_ct']).set_index(h3_colname)\
        .h3.h3_to_geo_boundary().reset_index()


df.to_csv("traffic_all_out.csv",index=False)

df.plot(column='traffic_ct')

plt.title("Full country 2018 traffic incident count map")
plt.show()
