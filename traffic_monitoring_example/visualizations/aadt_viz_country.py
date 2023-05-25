
"""

Simple UK visualization


"""

import h3

import pandas as pd
import h3pandas
import geopandas as gpd

import matplotlib.pyplot as plt

from clickhouse_driver import Client

ISO_A3 = 'GBR' # or 'USA'
H3_RES = 4
year = '2018'

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
    select h3ToString(h3ToParent(m.h3,{H3_RES})) as h3_d, medianDeterministic(m.value, 1) as traffic
    FROM ext AS e
    LEFT OUTER JOIN main_by_year AS m
    ON e.h3 = h3_d
    where m.variable = 'AADT' AND m.year = {year} group by h3_d
    HAVING traffic > 0;
    """

tables = [{"name": 'ext',
    'structure': [('h3', 'String')],
    'data': [{'h3': poly} for poly in polygons],
}]


result = db_client.execute(query, external_tables=tables)


# convert to dataframe
h3_colname = 'h3_d'
df = pd.DataFrame(result, columns=[h3_colname, 'traffic']).set_index(h3_colname)\
        .h3.h3_to_geo_boundary().reset_index()


if len(df) == 0:
    print("Cannot render empty df")
    quit()

df.to_csv(f"{ISO_A3}_aadt.csv", index=False)
df.plot(column='traffic')

plt.title(f"{year} worldwide traffic (avg aadt by cell)")
plt.show()