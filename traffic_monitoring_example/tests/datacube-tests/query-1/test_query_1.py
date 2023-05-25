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

regenerate_cube = True

cube_specification = {
    "date_range": ["2020-01-01", "2020-12-31"],
    "agg_time_granularity": "year", 
    "h3_res": 9, 
    "variables": [
        {
            "orig_time_granularity": "instantaneous", 
            "var_name": "TRAFFIC_EVENT",
            "agg_method": "count"
        },
        {
            "orig_time_granularity": "year",
            "var_name": "LANDSCAN_day",
            "agg_method": "avg"
        }
    ]
}

def cube_setup():
    db_connection_str = "postgres://clara4:123@localhost:5432/traffic_monitoring"
    i = Interface(db_connection_str)

    cube_name = "query_1_cube"

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

    cube.set_measure({
        "measures": [{
            "name": "TRAFFIC_EVENT_count",
            "aggregation_op": "sum",
            }, {
            "name": "LANDSCAN_day",
            "aggregation_op": "avg"
            }],
        "combination_op": "divide",

    })

    # add the filter
    cube.add_filter({
        "column": "LANDSCAN_day_sum",
        "op": "gt",
        "val": 5000
    })

    result = cube.get_cube()
    result["value"] = result.value.astype(float)
    result = result.nlargest(50, 'value')[["h3_09", "value"]].reset_index(drop=True)

    return result

def get_from_python():

    traffic_event_data = "../../../datasets/processed/LSTW/Traffic/data.csv"
    LANDSCAN_day = "../../../datasets/processed/LANDSCAN/landscan-usa-2020-conus-day.csv"


    # Read datasets

    traffic_event = pd.read_csv(traffic_event_data)
    landscan = pd.read_csv(LANDSCAN_day)

    # filter traffic event to 2020 only
    traffic_event['datetime'] = pd.to_datetime(traffic_event['datetime'],errors='coerce',utc=True)
    traffic_event = traffic_event[traffic_event["datetime"].dt.year == 2020]

    # Count traffic events by year
    traffic_event= traffic_event.groupby([
        #traffic_event['datetime'].dt.year,
        traffic_event["h3"]
    ]).value.count().reset_index()

    # Set fields to correct type for joining
    traffic_event['h3'] = traffic_event['h3'].astype(str)


    # Join on h3. 
    combined = traffic_event.merge(
        landscan,
        left_on=["h3"],
        right_on=["h3"]
        )

    # filter to necessary columns
    combined = combined[["h3", "value_x", "value_y"]].reset_index(drop=True)
    combined.columns = ["h3", "traffic_events_count", "LANDSCAN_day"]


    # filter population threshold > 5000
    # when is this filter applied?
    combined = combined[combined["LANDSCAN_day"] > 5000].reset_index(drop=True)

    # divide
    combined["value"] = combined["traffic_events_count"] / combined["LANDSCAN_day"]

    #order
    combined = combined.nlargest(50, 'value')[["h3", "value"]].reset_index(drop=True)

    return combined

def test_cube_correct():

    # setup the measure
    # A measure is then added to the cube, 
    # which specifies that the average number of yearly traffic events over population should be retrieved. 
    # The retrieved data can then be  filtered for population:.

    from_cube = get_from_cube()
    from_python = get_from_python()

    print(from_python)
    print(from_cube)

    assert(list(from_cube.value) == list(from_python.value))

    # get the value
    assert(1 == 1)

