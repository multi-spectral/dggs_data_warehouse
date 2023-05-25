
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


ISO_A3 = 'GBR' # or 'USA'
H3_RES = 9



# download file using this method: https://gis.stackexchange.com/questions/383465/from-uploaded-zipped-shapefile-to-geopandas-dataframe-in-django-application
states_path = "https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_state_20m.zip"
with ZipMemoryFile(BytesIO(urlopen(states_path).read())) as file:
    with file.open() as src:
        gdf = gpd.GeoDataFrame.from_features(src, crs=src.crs)

gjson = gdf[gdf.NAME.isin(['New York'])].explode(index_parts='true').__geo_interface__['features']

polygons = [h3.polyfill(feature['geometry'], H3_RES,True) for feature in gjson]
polygons = list(set(s for t in polygons for s in t))
#gjson = gdf[gdf.NAME == 'California'].explode(index_parts='true').__geo_interface__['features'][5]['geometry']



db_client = Client(host="localhost", database="traffic_monitoring")
query = f"""
    select h3ToString(h3ToParent(m.h3,{H3_RES})) as h3_d, sum(m.value) as pop_dens
    FROM ext AS e
    LEFT OUTER JOIN main_by_year AS m
    ON e.h3 = h3_d
    where m.variable = 'LANDSCAN_day' AND m.year = 2018 group by h3_d
    HAVING pop_dens > 0;
    """

tables = [{"name": 'ext',
    'structure': [('h3', 'String')],
    'data': [{'h3': poly} for poly in polygons],
}]


result = db_client.execute(query, external_tables=tables)


# convert to dataframe
h3_colname = 'h3_d'
df = pd.DataFrame(result, columns=[h3_colname, 'pop_dens']).set_index(h3_colname)\
        .h3.h3_to_geo_boundary().reset_index()


df.to_csv("landscan_ny_out.csv",index=False)
"""
df.plot(column='pop_dens')

plt.title("2018 New York State population density map")
plt.show()
"""