"""

The goal of this file is to test the accuracy of the Landscan ETL procedure.


"""


data_path_2018 = "../datasets/raw/LANDSCAN/landscan-usa-2018-day-assets/landscan-usa-2018-day/landscan-usa-2018-conus-day.tif"

import h3


import h3pandas
import pandas as pd
import rasterio as rio

from rasterstats import zonal_stats

from clickhouse_driver import Client


H3_RES = 9



def raster_get_h3_value(lat, lng, data_path):

    import h3.api.basic_str as h3 # get h3 as string

    h3_id = h3.geo_to_h3(lat, lng, H3_RES)

    # get polygon boundaries in geojson form, using the h3pandas interface
    boundaries = pd.DataFrame([h3_id],columns=['h3_09'])\
        .set_index('h3_09')\
        .h3.h3_to_geo_boundary()\
        .__geo_interface__

    
    # get sum of all pixels touched by this polygon in raster
    stats = pd.DataFrame(
                zonal_stats(
                    boundaries, 
                    data_path, all_touched=True, stats=['sum'])
            )
    
    return float(stats.iloc[0])

def db_get_h3_value_day(lat, lng, client):

    import h3.api.basic_int as h3 # get h3 as int (clickhouse compatibility)

    h3_id = h3.geo_to_h3(lat, lng, H3_RES)



    query_result = client.execute("""
        SELECT value FROM main_by_year
        WHERE variable = 'LANDSCAN_day'
        AND h3 = %(h3)s
        """, {'h3': h3_id})
    
    if len(query_result) == 0:
        return None
    else:
        return query_result[0][0]


def test_landscan_new_york_rasterstats():

    #get NYC as H3
    lat, lng = 40.730610, -73.935242 #source: latlon.net


    file_value = raster_get_h3_value(lat, lng, data_path_2018)
    db_value = db_get_h3_value_day(lat, lng, db_client)

    assert(file_value == db_value)



db_client = Client(host="localhost", database="traffic_monitoring")