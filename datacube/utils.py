from enum import Enum

import psycopg
import pandas as pd

class DbType(Enum):
    POSTGRES = 1
    CLICKHOUSE = 2
    DUCKDB = 3

class TimeGranularity(Enum):
    INSTANTANEOUS = 3
    DAY = 3
    MONTH = 4
    YEAR = 5
    DAY_OF_WEEK = 3.1
    DAY_OF_MONTH = 3.2
    DAY_OF_YEAR = 3.3
    HOUR_OF_DAY = 1

    def __lt__(self, other):
        return self.value < other.value
    def __le__(self, other):
        return self.value <= other.value
    def __eq__(self, other):
        return self.name == other.name

    
class AggregationMethod(Enum):
    COUNT = 1
    SUM = 2
    AVG = 3

def parse_connection_str(connection_str: str):

    return DbType['POSTGRES']

def execute_query(query, db_type: DbType, connection_str,return_result=True):

    # switch for conn str
    if db_type == DbType.POSTGRES:
        return execute_query_postgres(query, connection_str, return_result)
    else:
        return f"Failed: db_type not valid for connection string {connection_str}"
    
def execute_query_postgres(query, connection_str, return_result):

    try:
        with psycopg.connect(connection_str) as conn:
            with conn.cursor() as cur:
                result = cur.execute(query)

                if return_result:
                    colnames = [desc[0] for desc in cur.description] #https://stackoverflow.com/questions/10252247/how-do-i-get-a-list-of-column-names-from-a-psycopg2-cursor
                    result = pd.DataFrame(cur.execute(query),columns=colnames)

    except Exception as e:
        raise e

    if return_result:
        return result