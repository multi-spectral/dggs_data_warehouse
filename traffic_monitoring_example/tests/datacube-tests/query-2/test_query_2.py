import pytest
import os
import numpy as np

import pandas as pd
from datetime import date
import h3pandas
import json

import sys

sys.path.append("../../../../datacube")

from interface import *

regenerate_cube = False

cube_specification = {
    "agg_time_granularity": "year", 
    "h3_res": 7, 
    "variables": [
        {
            "orig_time_granularity": "instantaneous", 
            "var_name": "TRAFFIC_EVENT",
            "agg_method": "count"
        },
        {
            "orig_time_granularity": "year",
            "var_name": "AADT",
            "agg_method": "avg",
            "metadata_filter": [
            { "NUM_LANES": {"op": "gt", "val": 2, "type": "int"}}
            ]
        }
    ]
}

def cube_setup():
    db_connection_str = "postgres://clara4:123@localhost:5432/traffic_monitoring"
    i = Interface(db_connection_str)

    cube_name = "query_2_cube"

    if regenerate_cube:
        try:
            i.drop_cube(cube_name)
        except Exception:
            pass


        try:
            cube = i.create_cube(cube_name, cube_specification)

        except Exception as e:
            raise Exception(f"Could not create cube: {e}")
        
    
   
    cube = Cube(db_connection_str, cube_name)

    return cube

cube = cube_setup()

def get_from_cube():

    # A measure is then applied indicating the sum of total traffic events over the (sum) average annual daily traffic. 
    cube.set_measure({
        "measures": [{
            "name": "TRAFFIC_EVENT_count",
            "aggregation_op": "sum",
            }, {
            "name": "AADT",
            "aggregation_op": "avg"
            }],
        "combination_op": "divide",

    })

    result_unrolled = cube.get_cube()
    result_unrolled["value"] = result_unrolled.value.astype(float)

    # add the rollup
    cube.perform_op({"operation": "rollup", "options": {
        "dimension": "h3",
        "aggregation_scale": 5
    }})

    print(cube.generate_query())

    

    result_rolled = cube.get_cube()
    result_rolled["value"] = result_rolled.value.astype(float)

    #sort values for comparison
    result_unrolled['h3_07'] = result_unrolled['h3_07'].astype(str)
    result_rolled['h3_05'] = result_rolled['h3_05'].astype(str)
    result_unrolled = result_unrolled.sort_values(["h3_07","datetime_year"])
    result_rolled = result_rolled.sort_values(["h3_05","datetime_year"])

    return result_unrolled, result_rolled

def get_from_python():

    # Dataset paths

    traffic_event_data = "../../../datasets/processed/LSTW/Traffic/data.csv"
    AADT_data_MD = "../../../datasets/processed/AADT/Maryland/data.csv"
    AADT_data_MD_json = "../../../datasets/processed/AADT/Maryland/data.mdjson"


    # Read traffic event data

    traffic_event = pd.read_csv(traffic_event_data)

    # h3 get resolution 7 and rename columns
    traffic_event = traffic_event.rename(
        columns={"h3": "h3_09"})\
        .set_index('h3_09').\
        h3.h3_to_parent(7).\
        reset_index()\
        .drop(columns=['h3_09'])
    
    

    # Read MD data and get resolution 7. Only this one has NUM_lates

    with open(AADT_data_MD_json) as f:
        j = json.load(f)
    
    aadt = pd.read_csv(AADT_data_MD)\
            .rename(columns={"h3": "h3_09"})\
            .set_index('h3_09')\
            .h3.h3_to_parent(7)\
            .reset_index()\
            .drop(columns=['h3_09'])

    aadt['year'] = aadt['year'].astype(str)

    #get only with correct num lanes
    lanes_mapping = {i: item['original_metadata']['NUM_LANES'] for i, item in enumerate(j)}
    aadt['num_lanes'] = aadt['metadata_id'].apply(lambda x: lanes_mapping[x])
    aadt = aadt[aadt['num_lanes'] > 2].reset_index(drop=True).drop(columns=["num_lanes"])

    aadt_orig = aadt

    # Rollup AADT to res 7 first and rename columns
    aadt = aadt.groupby([aadt["h3_07"], aadt["year"]])\
                        .agg({"value": {"sum", "count"}})\
                        #.reset_index()
    aadt.columns = aadt.columns.map(''.join) # as in https://stackoverflow.com/questions/50571793/concise-way-of-flattening-multiindex-columns
    aadt = aadt.rename(columns={"valuesum": "aadt_sum", "valuecount": "aadt_count"})

    

    # Also rollup traffic to res 7 (filtering for relevant columns first)
    traffic_event['datetime'] = pd.to_datetime(traffic_event['datetime'],errors='coerce',utc=True)
    traffic_event['year'] = traffic_event['datetime'].dt.year.astype(str)
    traffic_event = traffic_event.drop(columns='datetime')
    traffic_event = traffic_event.rename(columns={"value": "traffic_sum"})

    traffic_event = traffic_event.groupby([traffic_event["h3_07"], traffic_event["year"]])\
                    .traffic_sum.agg("count")\
                    .reset_index()

    traffic_event["h3_07"] = traffic_event["h3_07"].astype(str)


    #join
    #joined_7 = traffic_event.join(aadt)
    joined_7 = traffic_event.merge(
        aadt,
        left_on=["h3_07", "year"],
        right_on=["h3_07", "year"]
        )


    # divide values
    joined_7["value"] = joined_7["traffic_sum"]/(joined_7["aadt_sum"]/joined_7["aadt_count"])



    joined_7_orig = joined_7
    # filter to correct columns
    joined_7 = joined_7[["h3_07", "year", "value"]].reset_index(drop=True)

    """ 
    # reset data to unrolled state
    aadt = aadt_orig

    del aadt_orig

    print("About to get h3 5 for traffic")

    # get h3_05 indexes
    traffic_event = traffic_event.set_index("h3_07")\
                            .h3.h3_to_parent(5)\
                            .reset_index().drop(columns=["h3_07"])
    aadt = aadt.set_index('h3_07')\
            .h3.h3_to_parent(5)\
            .reset_index()\
            .drop(columns=['h3_07'])

    print("about to rollup aadt 5")


    # Rollup AADT to res 5 first and rename columns
    aadt = aadt.groupby([aadt["h3_05"], aadt["year"]])\
                        .agg({"value": {"sum", "count"}})\
                        #.reset_index()
    aadt.columns = aadt.columns.map(''.join) # as in https://stackoverflow.com/questions/50571793/concise-way-of-flattening-multiindex-columns
    aadt = aadt.rename(columns={"valuesum": "aadt_sum", "valuecount": "aadt_count"})

    print("about to datetime traffic")

    # Also rollup traffic to res 5 (filtering for relevant columns first)
    print("about to groupby traffic")
    traffic_event = traffic_event.rename(columns={"value": "traffic_sum"})

    # groupby: sum instead of count because the value is already counts
    traffic_event = traffic_event.groupby([traffic_event["h3_05"], traffic_event["year"]])\
                    .traffic_sum.agg("sum")\
                    .reset_index()

    traffic_event["h3_05"] = traffic_event["h3_05"].astype(str)


    #join
    #joined_5 = traffic_event.join(aadt)
    print("about to join")
    joined_5 = traffic_event.merge(
        aadt,
        left_on=["h3_05", "year"],
        right_on=["h3_05", "year"]
        )

    #divide values
    joined_5["value"] = joined_5["traffic_sum"]/(joined_5["aadt_sum"]/joined_5["aadt_count"])

    print(joined_5.sort_values(["aadt_sum"],ascending=False))

    """
    joined_5 = joined_7_orig.reset_index().set_index("h3_07").h3.h3_to_parent(5).reset_index()
    joined_5["h3_05"] = joined_5["h3_05"].astype(str)
    print(joined_5)
    joined_5 = joined_5.groupby(["h3_05", "year"])\
                    .agg({"traffic_sum": "sum", "aadt_sum": "sum", "aadt_count": sum})\
                    .reset_index()


    # divide values
    joined_5["value"] = joined_5["traffic_sum"]/(joined_5["aadt_sum"]/joined_5["aadt_count"])

    print(joined_5.sort_values(["aadt_sum"],ascending=False))

    # filter to correct columns
    joined_5 = joined_5[["h3_05", "year", "value"]].reset_index(drop=True)


    
    # sort values for comparison
    joined_5 = joined_5.sort_values(["h3_05", "year"])
    joined_7 = joined_7.sort_values(["h3_07", "year"])
    return joined_7, joined_5
 

def test_cube_correct():

    # setup the measure
    # A measure is then added to the cube, 
    # which specifies that the average number of yearly traffic events over population should be retrieved. 
    # The retrieved data can then be  filtered for population:.


    from_cube_7, from_cube_5 = get_from_cube()
    from_python_7, from_python_5 = get_from_python()



    assert(np.allclose(list(from_cube_7.value),list(from_python_7.value),atol=1e-07))
    assert(np.allclose(list(from_cube_5.value),list(from_python_5.value),atol=1e-07))

    # get the value
    assert(1 == 1)

