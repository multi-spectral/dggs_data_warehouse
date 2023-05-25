"""
TODO: parametrize the query building

"""

import psycopg

from enum import Enum
import psycopg
import pandas as pd
import argparse
import json

import os
from pathlib import Path

from datetime import datetime

TABLE_NAME_DAY = 'main_by_date'
TABLE_NAME_YEAR = 'main_by_year'
TABLE_NAME_INSTANTANEOUS = 'main_instantaneous'

from utils import DbType, TimeGranularity, AggregationMethod
from utils import parse_connection_str, execute_query


"""

TODO:
    - Rename the columns so they match the patterns needed for the operations.

"""



def main():

    parser = configure_parser()
    args = parser.parse_args()

    if args.out_path is not None:
        if Path(args.out_path).suffix != '.csv':
            raise Exception(f'out path must be csv.')

    result = dc_query(import_json=args.datacube_params, connection_str=args.connection_str)

    if args.out_path is not None:
        result.to_csv(args.out_path, index=False)

    return result

def configure_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,
                                     description="""
    
    The --datacube_params argument must be a json-formatted string, or a path to a json file. Its format follows a specific convention.
    
    {
        (optional) "date_range": ...,
        "agg_time_granularity": ...,
        "h3_res": ...,
        "variables": ...,
    }

    * date_range: a date range in the format [start, end] with 'YYYY-MM-DD' format
    * agg_time_granularity: one of ['day', 'year']
    * h3_res: an integer between 0-15 (inclusive) specifying the H3 resolution for aggregation
    * variables: a list of one or two dictionaries of the following format:
        {
            "orig_time_granularity": <one of [instantaneous, day, year]>,
            "var_name": <name of the variable>
            "agg_method": <one of [count, sum, avg]>
            (optional) "metadata_filter": [
                {"metadata.field1.field2.field3": {"op": < gt, lt, eq>, "val": <val>, "type": <int, text, float>}}
                ...
            ]
        }.


    """)
    parser.add_argument('--datacube_params', help='Datacube parameters in the specified format', required=True)
    parser.add_argument('--connection_str', help = 'Connection string for the database', required=True)
    parser.add_argument('--out_path', help = 'Out path for the data')
    return parser

def dc_query_construct(import_json: str, connection_str: str):

    

    # parse connection string and get db type
    db_type = parse_connection_str(connection_str)

    # parse and validate import json
    if os.path.isfile(import_json):
        correct_json = load_json(import_json, file=True)
    else:
        correct_json = load_json(import_json, file=False)

    # build query
    query = build_query(correct_json, db_type)
    

    return query

def dc_query(import_json: str, connection_str: str):

    # parse connection string and get db type
    db_type = parse_connection_str(connection_str)

    # parse and validate import json
    if os.path.isfile(import_json):
        correct_json = load_json(import_json, file=True)
    else:
        correct_json = load_json(import_json, file=False)

    # build query
    query = build_query(correct_json, db_type)
    print(query)

    # execute query
    result = execute_query(query, db_type, connection_str)

    # print result (or err)
    return result


def load_json(json_data: str, file=False):

    # first try to load the json
    try:
        if file is True:
            with open(json_data) as f:
                json_unvalidated = json.load(f) #'load' loads from file
        else:
            json_unvalidated = json.loads(json_data) # 'loads' loads from string
    except Exception as e:
        raise Exception(f"Could not load json from input data: {e}. Check that input data is valid json")
    
    # validate the keys
    for key in ['agg_time_granularity', 'h3_res', 'variables']:
        if key not in json_unvalidated:
            raise Exception(f"JSON missing key '{key}'. Please run with --help for more information")
        
    for key in list(json_unvalidated.keys()):
        if key not in ['date_range','agg_time_granularity', 'h3_res', 'variables']:
            raise Exception(f"Invalid key {key}.")
        
    # validate the date range
    if 'date_range' in json_unvalidated:
        date_range = json_unvalidated['date_range']
        if len(date_range) != 2:
            raise Exception
        elif len(date_range) == 2:
            start, end = date_range
            try:
                if datetime.strptime(start, '%Y-%m-%d') > datetime.strptime(end, '%Y-%m-%d'):
                    raise Exception("Start date must come before end date")
            except Exception as e:
                raise Exception("Date range must be in format ['YYYY-MM-DD', 'YYYY-MM-DD']")
        
    # validate the time unit
    agg_time_granularity = json_unvalidated['agg_time_granularity']
    try:
        agg_time_granularity = TimeGranularity[agg_time_granularity.upper()]
    except Exception as e:
        raise Exception(f'Agg time granularity {agg_time_granularity} is not valid>')
    
    
    # validate the h3 res
    h3_res = json_unvalidated['h3_res']
    if type(h3_res) != int:
        raise Exception
    elif h3_res > 15 or h3_res < 0:
        raise Exception
    
    # validate the variables
    variables = json_unvalidated['variables']
    if len(variables) not in [1,2]:
        raise Exception(f"There are {len(variables)} variables provided when there should be one or two.")
    double_variable = True if len(variables) == 2 else False
    for variable in variables:
        if type(variable) != dict:
            raise Exception("All variables must be dictionaries with the correct format. Please run --help for more details.")
        # schema: time_granularity, var_name, agg_method, metadata_filter
        for key in ['orig_time_granularity','var_name', 'agg_method']:
            if key not in variable:
                raise Exception(f"Key '{key}' missing from variable.")
            
        for key in list(variable.keys()):
            if key not in ['orig_time_granularity','var_name', 'agg_method', 'metadata_filter']:
                raise Exception(f"Invalid key {key}.")
        
        orig_time_granularity = variable['orig_time_granularity']
        if orig_time_granularity not in ['day', 'year', 'instantaneous']:
            raise Exception(f"Invalid time granularity '{orig_time_granularity}'. Please provide one of <day, year, instantaneous>")
        
        # convert and compare time granularities
        orig_time_granularity = TimeGranularity[orig_time_granularity.upper()]

        #if orig_time_granularity > agg_time_granularity:
        #    raise Exception(f"Original time granularity '{orig_time_granularity}' is greater than the aggregation time granularity '{agg_time_granularity}'")
        
        # check aggregation method
        agg_method = variable['agg_method']
        if agg_method not in ['count', 'sum', 'avg']:
            raise Exception(f"Invalid aggregation method '{agg_method}'. Please provide one of <count,sum,avg>")

        # check metadata filter
        if 'metadata_filter' in variable:
            metadata_filter = variable['metadata_filter']
            if type(metadata_filter) != list:
                raise Exception("Metadata filter must be a list of dicts of attributes. See --help for details")
            for filter in metadata_filter:
                if type(filter) != dict:
                    raise Exception("Metadata filter must be a list of dicts of attributes. See --help for details")
                for filter_var, filter_dict in filter.items():
                    for key in ['op', 'val', 'type']:
                        if key not in filter_dict:
                            raise Exception(f"Metadata filter for '{filter_var}' missing key '{key}'")
                    if filter_dict['op'] not in ['gt', 'lt', 'eq']:
                        raise Exception(f"Metadata filter for '{filter_var}' has invalid operation. Must be in <gt, lt, eq>")
                    if type(filter_dict['val']) not in [str, int, float]:
                        raise Exception(f"Metadata filter for '{filter_var}': variable must be a string, int or float")
                    if filter_dict['type'] not in ['str', 'int', 'float']:
                        raise Exception(f"Metadata filter for '{filter_var}': variable must be in <text, int, float>")

    
    
    # note whether it is double variable
    json_unvalidated['double_variable'] = double_variable

    json_validated = json_unvalidated
    return json_validated

def build_query(correct_json, db_type):

    return build_query_double(correct_json, db_type) \
         if correct_json['double_variable'] is True \
         else build_query_single(correct_json, db_type)


def build_query_single(correct_json, db_type, sub_query=False):

    #setup variables
    variable = correct_json['variables'][0]
    date_range = correct_json['date_range'] if "date_range" in correct_json else None
    varname = variable['var_name']
    agg_method = variable['agg_method']
    agg_time_granularity = correct_json['agg_time_granularity']
    table_granularity = variable['orig_time_granularity']

    # handle time granularity
    if table_granularity == 'year' and agg_time_granularity == 'year': #only agg allowed for year
        date_field = "t.year"
        date_join = ""
        date_where = ""
    elif table_granularity == 'instantaneous' and agg_time_granularity == 'day':
        # join on date id and filter
        #TODO
        date_field = "d.date_id"
        date_join = ", date AS d " 
        date_where = "AND t.date_id = d.date_id"
    elif table_granularity == 'instantaneous' and agg_time_granularity == 'year':
        date_field = "d.year"
        date_join = ", date AS d"
        date_where = "AND t.date_id = d.date_id"
    elif table_granularity == 'year' and agg_time_granularity == 'day':
        date_field = "d.date_id"
        date_join = ", date AS d "
        date_where = "AND d.year = t.year"
    else:
        raise Exception(f"Not implemented for table_granularity {table_granularity} and agg_time_granularity {agg_time_granularity}")


    # handle date range
    if date_range is None:
        date_range_cond = ""
    elif table_granularity == 'year':
        date_range_cond = f"AND '[{date_range[0]},{date_range[1]}]'::daterange @> to_date(t.year::varchar, 'YY')"
    elif table_granularity == 'day':
        date_range_cond = f"AND '[{date_range[0]},{date_range[1]}]'::daterange @> d.date_actual"
    else: #instantaneous
        date_range_cond = f"AND '[{date_range[0]},{date_range[1]}]'::daterange @> d.date_actual"



  
    
    if table_granularity == 'day':
        table = TABLE_NAME_DAY
    elif table_granularity =='instantaneous':
        table = TABLE_NAME_INSTANTANEOUS
    else: #year
        table = TABLE_NAME_YEAR

    h3_res = correct_json['h3_res']


    #make the columns to aggregate
    if agg_method == "count":
        agg_fields_sql = "COUNT(t.value) AS count_value"
    else: #avg
        agg_fields_sql = "SUM(t.value) AS sum_value, COUNT(t.value) AS count_value"


    # add metadata filters where applicablle
    if 'metadata_filter' in variable:
        query = f"""
        SELECT h3_cell_to_parent(t.h3, {h3_res}) AS h3, {date_field} AS {agg_time_granularity}, {agg_fields_sql}
        FROM dataset_metadata AS m,
        {table} AS t{date_join}
        WHERE t.dataset_metadata_id = m.id
        AND t.variable = '{varname}' {date_where} {date_range_cond}
        """

        #TODO: add filters
        for filter in variable['metadata_filter']:
            varname = list(filter.keys())[0]
            varname_psql = "".join(f"['{s}']" for s in varname.split("."))

            filter_params = filter[varname] #get the first and only key
            vartype = filter_params['type']
            operator = filter_params['op']
            if operator == 'gt':
                operator = '>'
            elif operator == 'lt':
                operator = '<'
            else:
                operator = '='
            value = filter_params['val']

            # append to query
            query = query + f"AND m.metadata{varname_psql}::{vartype} {operator} {value}\n\t"
    else:
        query = f"""
        SELECT h3_cell_to_parent(t.h3, {h3_res}) AS h3, {date_field} AS {agg_time_granularity}, {agg_fields_sql}
        FROM {table} AS t {date_join}
        WHERE t.variable = '{varname}' {date_where} {date_range_cond}
        """

    # TODO: add date if available

    # group by cube
    # note: do not use the 'GROUP BY CUBE' option because this is just the foundational table.
    query = query + f"GROUP BY h3_cell_to_parent(t.h3, {h3_res}), {date_field}"

    h3_cols_sql = generate_h3_dimension(h3_res)

    # Write the SQL for date hierarchy cross reference
    if agg_time_granularity == "year":
        date_hierarchy_sql = "a.year as datetime_year"
        date_hierarchy_join = ""
        date_hierarchy_where = ""
    else:
        #populate from table
        date_hierarchy_join = ", date as d"
        date_hierarchy_where = "WHERE d.date_id = a.day"
        date_hierarchy_sql  = "d.year AS datetime_year, d.month AS datetime_month, d.day_of_month AS datetime_day"


     #make the columns to aggregate
    if agg_method == "count":
        a_columns_sql = f"a.count_value AS {variable['var_name']}_count"
    else: #avg
        a_columns_sql = f"a.sum_value AS {variable['var_name']}_sum, a.count_value AS {variable['var_name']}_count"


    if not sub_query:

        query = f"""
        WITH a AS ({
            query
        })
        SELECT  {h3_cols_sql}, {date_hierarchy_sql}, {a_columns_sql} FROM a{date_hierarchy_join}
        {date_hierarchy_where}

        """ 

        
    return query

def build_query_double(correct_json, db_type):

    # get variables
    variable_1, variable_2 = correct_json['variables']
    agg_time_granularity = correct_json['agg_time_granularity']
    json_1, json_2 = correct_json.copy(), correct_json.copy() #copy
    json_1['variables'] = [variable_1]
    json_2['variables'] = [variable_2]
    agg_method_1 = variable_1['agg_method']
    agg_method_2 = variable_2['agg_method']
    h3_res = correct_json["h3_res"]
    

    q1 = build_query_single(json_1, db_type,sub_query=True)
    q2 = build_query_single(json_2, db_type,sub_query=True)

    #make the columns to aggregate
    if agg_method_1 == "count":
        a_columns_sql = f"a.count_value AS {variable_1['var_name']}_count"
    else: #avg
        a_columns_sql = f"a.sum_value AS {variable_1['var_name']}_sum, a.count_value AS {variable_1['var_name']}_count"

    if agg_method_2 == "count":
        b_columns_sql = f"b.count_value AS {variable_2['var_name']}_count"
    else: #avg
        b_columns_sql = f"b.sum_value AS {variable_2['var_name']}_sum, b.count_value AS {variable_2['var_name']}_count"


    # Write the SQL for h3 columns generation
    valid_indexes = range(h3_res,-1,-1)
    h3_colnames = [f"h3_{h:02}" for h in valid_indexes]
    h3_ops = [f"h3_cell_to_parent(b.h3,{i}) AS {colname}" for i, colname in zip(valid_indexes,h3_colnames)]
    h3_cols_sql = ", ".join(h3_ops)
    #column names: h3, {agg_time_granularity}, {agg_method}_value

    # Write the SQL for date hierarchy cross reference
    if agg_time_granularity == "year":
        date_hierarchy_sql = "b.year as datetime_year"
        date_hierarchy_join = ""
        date_hierarchy_and = ""
    else:
        #populate from table
        date_hierarchy_join = ", date as d"
        date_hierarchy_and = "AND d.date_id = a.day"
        date_hierarchy_sql  = "d.year AS datetime_year, d.month AS datetime_month, d.day_of_month AS datetime_day"

        """
            date_id integer primary key,
            date_actual date,
            day_of_month integer,
            month integer,
            year integer,
            day_name VARCHAR(9) NOT NULL,
            day_of_week integer,
            day_of_year integer,
            month_name text
    """


    query = f"""
    
    WITH a AS (
        {q1}
        ), 
        b AS (
        {q2}
        )
SELECT {h3_cols_sql}, {date_hierarchy_sql},
        {a_columns_sql}, {b_columns_sql}
        from b, a {date_hierarchy_join}
        where a.h3 = b.h3 AND a.{agg_time_granularity} = b.{agg_time_granularity}
        {date_hierarchy_and}
    """



    return query

    
def generate_h3_dimension(h3_res):
    # Write the SQL for h3 columns generation
    valid_indexes = range(h3_res,-1,-1)
    h3_colnames = [f"h3_{h:02}" for h in valid_indexes]
    h3_ops = [f"h3_cell_to_parent(a.h3,{i}) AS {colname}" for i, colname in zip(valid_indexes,h3_colnames)]
    h3_cols_sql = ", ".join(h3_ops)
    #column names: h3, {agg_time_granularity}, {agg_method}_value

    return h3_cols_sql

if __name__ == "__main__":
    print(main())