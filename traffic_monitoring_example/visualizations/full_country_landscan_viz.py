
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


H3_RES = 9

db_client = Client(host="localhost", database="traffic_monitoring")
query = f"""
    select h3ToString(h3ToParent(m.h3,{H3_RES})) as h3_d, sum(m.value) as pop_dens
    FROM main_by_year AS m
    where m.variable = 'LANDSCAN_day' AND m.year = 2018 group by h3_d
    HAVING pop_dens > 0;
    """



result = db_client.execute(query)


# convert to dataframe
h3_colname = 'h3_d'
df = pd.DataFrame(result, columns=[h3_colname, 'pop_dens']).set_index(h3_colname)\
        .h3.h3_to_geo_boundary().reset_index()



df.to_csv("landscan_all_out.csv",index=False)
"""
df.plot(column='pop_dens')

plt.title("2018 population density map")
plt.show()
"""