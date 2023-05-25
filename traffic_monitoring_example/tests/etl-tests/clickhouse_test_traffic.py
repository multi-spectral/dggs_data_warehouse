"""

Test strategy
1. compare number of rows
2. compare average
3. sort and compare a subset of n elements


"""


import pytest

import polars as pl
from clickhouse_driver import Client

data_path = "../datasets/processed/LSTW/Traffic/data.csv"
duckdb_path = "../duckdb/duck.db3"

# run python -m pytest on mac,
# or python3 -m pytest on ubuntu.

def load_data_from_file(data, variable=None):


    # calculate the number of records
    # see https://stackoverflow.com/questions/75523498/python-polars-how-to-get-the-row-count-of-a-dataframe
    if variable == 'count':
        return data.select(pl.count()).collect().item()
    elif variable == 'mean':
    # calculate the mean value
        return data.select(pl.mean('value')).collect().item()
    elif variable == 'subset':
    # order by descending, and get the 1000-1099th elements
        return data.select("value")\
                    .filter(pl.element != 0)\
                    .sort("value", descending=True)\
                    .slice(1000,100)\
                    .collect()\
                    .get_columns()[0]\
                    .to_list()

def load_data_from_db(client, variable=None):


    # calculate the number of records
    if variable == 'count':
        return client.execute("""
        SELECT COUNT(value) FROM main_instantaneous
        WHERE variable = 'TRAFFIC_EVENT'
        """)[0][0]

    # calculate the mean value 
    elif variable == 'mean':
        return client.execute("""
        SELECT AVG(value) FROM main_instantaneous
        WHERE variable = 'TRAFFIC_EVENT'
        """)[0][0]


    # order by descending, and get the 1000-1099th elements
    elif variable == 'subset': 
        result = client.execute("""
        SELECT value FROM main_instantaneous
        WHERE variable = 'TRAFFIC_EVENT'
        AND value <> 0
        ORDER BY value DESC
        LIMIT 100 OFFSET 1000
        """)

        return [int(item[0]) for item in result]



def test_data_count():
    

    assert(load_data_from_file(file_df, 'count') == load_data_from_db(db_client, 'count'))

def test_data_mean():
    # Load the data from the original dataset.
    file_df = pl.scan_csv(data_path)
    db_client = Client(host="localhost", database="traffic_monitoring")

    assert(load_data_from_file(file_df, 'mean') == load_data_from_db(db_client, 'mean'))

def test_data_slice():
    # Load the data from the original dataset.
    file_df = pl.scan_csv(data_path)
    db_client = Client(host="localhost", database="traffic_monitoring")


    assert(load_data_from_file(file_df, 'subset') == load_data_from_db(db_client, 'subset'))


# Load the data from the original dataset.
file_df = pl.scan_csv(data_path)
db_client = Client(host="localhost", database="traffic_monitoring")