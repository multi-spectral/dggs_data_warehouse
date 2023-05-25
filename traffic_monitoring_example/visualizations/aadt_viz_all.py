
"""

Simple UK visualization


"""

import h3

import pandas as pd
import h3pandas
import geopandas as gpd

import matplotlib.pyplot as plt

from clickhouse_driver import Client

H3_RES = 5





db_client = Client(host="localhost", database="traffic_monitoring")

query = f"""
    select h3ToString(h3ToParent(m.h3,{H3_RES})) as h3_d, medianDeterministic(m.value, 1) as traffic
    FROM  main_by_year AS m
    where m.variable = 'AADT' AND m.year = 2019 group by h3_d
    HAVING traffic > 0;
    """


result = db_client.execute(query)


# convert to dataframe
h3_colname = 'h3_d'
df = pd.DataFrame(result, columns=[h3_colname, 'traffic']).set_index(h3_colname)\
        .h3.h3_to_geo_boundary().reset_index()


if len(df) == 0:
    print("Cannot render empty df")
    quit()

df.plot(column='traffic')

plt.title("2019 worldwide traffic (median aadt by cell)")
plt.show()