"""

Main interface to data cube

"""

import h3
import json
#import pandas as pd

H3_RES = 9

import re, os
import operations, cube_sql
from utils import parse_connection_str
#import materialize_cube
from utils import execute_query, parse_connection_str, TimeGranularity


class Interface:

    def __init__(self, db_connection_str):
    
        self.connection_str = db_connection_str
        self.db_type = parse_connection_str(self.connection_str)

        #TODO: setup database connection and get db type

    def create_cube(self, cube_name, import_json):

        try:
            return self._create_cube(cube_name, import_json)
        except Exception as e:
            print(f"Could not create cube: {e}")
            return None
    
    def drop_cube(self, cube_name):

        return self._drop_cube(cube_name)
    


    def create_cube_from_sql(self, cube_name, cube_query):

        # Create the cube if not exists. If it does, throw error
        creation_query = f"CREATE TABLE {cube_name} AS" + cube_query
        try:
            execute_query(creation_query, self.db_type, self.connection_str, return_result=False)
        except Exception as e:
            raise Exception(f"Could not create table: {e}")

        # return a handle to the cube, and set its column names as state
        try:
            return Cube(self.connection_str, cube_name)
        except Exception as e:
            raise Exception(f"Could not return cube handle: {e}")
        


    def _create_cube(self, name: str, import_json):

        if type(import_json) is dict:
            import_json = json.dumps(import_json)
        elif type(import_json) == str:
            pass
        else:
            raise Exception("Input must be str or dict")

        #Validate cube name
        # Only letters, numbers and dashes/hyphens, and ends with cube
        try:
            Cube.validate_name(name)
        except Exception as e:
            raise Exception("Cube name contains invalid characters")

        # TODO: construct the cube creation query as SQL
        #query = materialize_cube.dc_query_construct(query, conn)
        # parse and validate import json
        try:
            if os.path.isfile(import_json):
                correct_json = cube_sql.load_json(import_json, file=True)
            else:
                correct_json = cube_sql.load_json(import_json, file=False)
        except Exception as e:
            raise Exception(f"Could not load json: {e}")

        # build query
        cube_query = cube_sql.build_query(correct_json, self.db_type)

        #Create the cube if not exists. If it does, throw error
        creation_query = f"CREATE TABLE {name} AS" + cube_query
        try:
            execute_query(creation_query, self.db_type, self.connection_str, return_result=False)
        except Exception as e:
            raise Exception(f"Could not create table: {e}")

        # return a handle to the cube, and set its column names as state
        try:
            return Cube(self.connection_str, name)
        except Exception as e:
            raise Exception(f"Could not return cube handle: {e}")
        


    def _drop_cube(self, name:str):

        # TODO: rm cube if exists

        #Validate cube name
        # Only letters, numbers and dashes/hyphens, and ends with cube
        try:
            Cube.validate_name(name)
        except Exception as e:
            raise Exception("Cube name contains invalid character or does not end in _cube")
        
        drop_query = f"DROP TABLE {name}"

        try:
            execute_query(drop_query, self.db_type, self.connection_str, return_result=False)
        except Exception as e:
            raise e
        


    

class Measure:

    """
        {
            "measure": {
                "name": <colname>,
                "aggregation_op": <sum, avg, max>
                }
        }
        OR
        {
            "measures": [{
                "name": <colname>,
                "aggregation_op": <sum, avg, max>
                }, {
                "name": <colname>,
                "aggregation_op": <sum, avg, max>
                }]
            "combination_op": <multiply, divide>,
        }
        """
        
    def __init__(self, description, valid_colnames):



        Measure.validate_measure(description, valid_colnames)

        self.description = description
        if len(list(description.keys())) == 1: #single
            self.single = True
        else:
            self.single = False


    def make_sql(self):


        if self.single:

            return self._make_single_measure_sql(self.description["measure"])
  

        else:

            var1, var2 = self.description["measures"]
            symbol = "*" if self.description["combination_op"] == "multiply" else "/"

            sql1, sql2 = self._make_single_measure_sql(var1), self._make_single_measure_sql(var2)

            op = self.description["combination_op"]
            if op == "multiply":
                return f"({sql1}) * ({sql2})"
            else:
                return f"({sql1})/NULLIF({sql2},0)"



    def _make_single_measure_sql(self, item):

        colname = item["name"]
        op = item["aggregation_op"]
        
        if op == "sum":
            return f"SUM({colname})"
        elif op == "avg" and colname.split("_")[-1] not in ["count", "sum"]:
            colname_base = colname
            colname_sum = colname_base + "_sum"
            colname_count = colname_base + "_count"

            return f"SUM({colname_sum})/SUM({colname_count})"
        elif op == "avg":
            return f"AVG({colname})"
        elif op == "max":
            return f"MAX({colname})"
    

    def validate_measure(measure,valid_colnames):

        if type(measure) is not dict:
            raise Exception("Measure must be provided as a dictionary")


        """
            {
                "measure": {
                    "name": <colname>,
                    "aggregation_op": <sum, avg, max>
                    }
            }
            OR
            {
                "measures": [{
                    "name": <colname>,
                    "aggregation_op": <sum, avg, max>
                    }, {
                    "name": <colname>,
                    "aggregation_op": <sum, avg, max>
                    }]
                "combination_op": <multiply, divide>,
            }
            """

        keys = sorted(measure.keys())

        if keys == ["measure"]:
            # single option

            item = measure["measure"]
            if type(item) is list:
                if len(item) != 1:
                    raise Exception("In this format, must provide exactly one measure. Otherwise try with keys (measures, combination_op)")
                else:
                    item = item[0]
            Measure._validate_single_measure(item, valid_colnames)


        elif keys == ["combination_op", "measures"]:
            # combined option
            if measure["combination_op"] not in ["multiply", "divide"]:
                raise Exception("operation must be multiply or divide")

            measures_pair = measure["measures"]
            if type(measures_pair) is not list:
                raise Exception("Provided measures must be list")
            elif len(measures_pair) != 2:
                raise Exception("Must provide exactly two measures")
            for item in measures_pair:

                Measure._validate_single_measure(item, valid_colnames)

        else:
            raise Exception("Invalid keys. Must be (measure) or (measures, operation)")

    def _validate_single_measure(item, valid_colnames):


        if sorted(item.keys()) != ["aggregation_op", "name"]:
            raise Exception(f"Need to provide exactly two keys: aggregation_op and name")

        name = item["name"].lower()
        if item["aggregation_op"] in ["sum"] and name not in valid_colnames:
            possibly_meant = [v for v in valid_colnames if v.startswith("name")]
            raise Exception(f"{name} Not a valid measure column for this cube. Try {name}_sum")
        elif item["aggregation_op"] in ["avg"]:
            suffix = "_"+name.split("_")[-1]
            print(suffix)
            if suffix not in ["_count", "_sum"]:
                if (name+"_sum" not in valid_colnames or name+"_count" not in valid_colnames):
                    print(valid_colnames)
                    raise Exception(f"For avg of {name}, must provide base variable name  (without _sum or _count)")

        if item["aggregation_op"] not in ["sum", "avg", "max"]:
            raise Exception(f"Aggregation op must be sum, avg or max")




        



        



class Cube:

    def __init__(self, db_connection_str, cube_name, measure=None):

        self.connection_str = db_connection_str
        self.cube_name = cube_name
        self.db_type = parse_connection_str(db_connection_str)

        self.base_metadata = self.retrieve_base_metadata()
        self.aggregation_state = self.default_aggregation_state()
        
        if measure:
            try:
                self.measure = Measure(measure, self.base_metadata["measure_colnames"])
            except Exception as e:
                raise Exception("Invalid measure", e)

    def execute_query(self, query):

        try:
            result = execute_query(query, self.db_type, self.connection_str)
        except Exception as e:
            print(f"Could not execute query: {e}")
            return None

        return result

    
    def get_cube(self):

        #Next, reconstruct the query from the state
        try:
            query = self.generate_query()

        except Exception as e:
            raise Exception(f"Failed to generate query: {e}")
    

        try:
            # Last, execute the query
            result = execute_query(query, self.db_type, self.connection_str)
        except Exception as e:
            raise Exception("Could not execute query:", e)
        else:
            return result

    def add_filter(self, filter):

        # Validate filter
        try:
            self._validate_filter(filter)
        except Exception as e:
            raise Exception(f"Invalid filter: {e}")

        # Add filter to aggregation state
        try:
            self._update_aggregation_state_filter(filter)
        except Exception as e:
            raise Exception(f"Could not update aggregation state: {e}")


    def perform_op(self, description: dict):

        if self.measure is None:
            print("Please run set_measure() to set the measure to use. Check the colnames for candidates")
            return None

        try:
            # First, validate and extract the operation and options
            operation, options = operations.extract_operation(description)
        except Exception as e:
            print(f"Failed to parse operation: {e}")
            return

    
        # Then, update the state with the op, if possible. If not, raise exception
         # This part also validates the semantics
        try:
            self.update_aggregation_state(operation, options)
        except Exception as e:
            print(f"Failed to update aggregation state: {e}")
            return

        try:
            return self.get_cube()
        except Exception as e:
            print(f"Failed to generate query: {e}")
            return

        



    def generate_query(self):

        # TODO: generate the query using the aggregation state, the base metadata, and the measure
        # Incorporate h3_agg_scale, datetime_agg_scale

        h3_colname = get_h3_colname(self.aggregation_state["h3_agg_scale"])

        datetime_agg_scale = self.aggregation_state["datetime_agg_scale"]
        if datetime_agg_scale == TimeGranularity.YEAR:
            datetime_sql = "datetime_year"
        elif datetime_agg_scale == TimeGranularity.MONTH:
            datetime_sql = "datetime_year, datetime_month"
        elif datetime_agg_scale == TimeGranularity.DAY:
            raise Exception(datetime_agg_scale)
            datetime_sql = "datetime_year, datetime_month, datetime_day_of_month"
        elif datetime_agg_scale == TimeGranularity.DAY_OF_MONTH:
            datetime_sql = "datetime_day_of_month"
        elif datetime_agg_scale == TimeGranularity.DAY_OF_WEEK:
            datetime_sql = "datetime_day_of_week"
        elif datetime_agg_scale == TimeGranularity.DAY_OF_YEAR:
            datetime_sql = "datetime_day_of_year"
        elif datetime_agg_scale == TimeGranularity.HOUR_OF_DAY:
            datetime_sql = "datetime_hour_of_day"
        else:
            raise Exception(f"{datetime_agg_scale} is not a supported datetime interval")

        measure_sql = self.measure.make_sql()


        filters = self.aggregation_state["filters"]
        if len(filters) == 0:
            filters_sql = ""
        else:
            filters_sql = "WHERE "
            for i, filter in enumerate(filters):
                if filter["column"] == "h3":
                    colname = get_h3_colname(filter["aggregation_scale"])
                    filters_sql = filters_sql + f"{colname} = {filter['value']}"
                elif filter["column"] == "datetime":
                    colname = get_date_colname(filter["aggregation_scale"])
                    filters_sql = filters_sql + f"{colname} = {filter['value']}"
                else:
                    # filter raw data
                    op = filter["op"]
                    if op == "gt":
                        op = ">"
                    elif op == "lt":
                        op = "<"
                    else:
                        op + "+"
                    colname = filter["column"]
                    filters_sql = filters_sql + f"{colname} {op} {filter['val']}"



                
                if i != len(filters) - 1:
                    filters_sql = filters_sql + "\n AND "

        """

        SELECT {all aggregation_columns}, {measure} FROM cube_table
        WHERE
            <slice filter 1> AND
            <slice filter 2> AND
            ....
        GROUP BY CUBE({rollup/drilldown by h3 AND/OR rollup/drilldown by time})
        """

        query = f"""

        SELECT {h3_colname}, {datetime_sql}, {measure_sql} AS value
        FROM {self.cube_name} {filters_sql}
        GROUP BY {h3_colname}, {datetime_sql}

        """

        return query


        return ""
    def retrieve_base_metadata(self, example=False):

        # TODO: describe the structure of the cube
        
        # return the column names (and types?) as state
        """
        This includes:
        - The maximum and minimum h3 index
        - The maximum and minimum date component
        - The other column names
        - Tracks sum and count, if necessary
        - Tracks operation
            e.g. sum_1, count_1, avg_1, sum_2, count_2
            the state tracks the operators, which are provided by the user to the cube interface (after creation)
            User provides measure to cube
        """

        result = execute_query(f"SELECT * FROM {self.cube_name} LIMIT 1", self.db_type, self.connection_str) #TODO: escape sql

        colnames = list(result.columns)
 
        datetime_colnames =[c for c in colnames if c[0:9] == "datetime_"]
        h3_colnames =  [c for c in colnames if c[0:3] == "h3_"]
        measure_colnames = [c for c in colnames if c[0:3] != "h3_" and c[0:9] !="datetime_"]

        max_h3_index = get_max_h3_index(h3_colnames)
        min_h3_index = 1
        min_datetime_index, max_datetime_index = get_datetime_index_bounds(datetime_colnames)
        
        

        return {
            "h3_max": max_h3_index,
            "h3_min": min_h3_index,
            "datetime_min": min_datetime_index,
            "datetime_max": max_datetime_index,
            "h3_colnames": h3_colnames,
            "datetime_colnames": datetime_colnames,
            "measure_colnames": measure_colnames
        }


    def set_measure(self, measure):

        # TODO: set the measure to be used
        """
        {
            "measure": {
                "name": <colname>,
                "aggregation_op": <sum, avg>
                }
        }
        OR
        {
            "measures": [{
                "name": <colname>,
                "aggregation_op": <sum, avg>
                }, {
                "name": <colname>,
                "aggregation_op": <sum, avg>
                }]
            "combination_op": <multiply, divide>,
        }
        """
        # TODO: check first if in the column names returned by describe


        self.reset_aggregation_state()

        try:
            self.measure = Measure(measure, self.base_metadata["measure_colnames"])
        except Exception as e:
            raise Exception(f"Invalid measure: {e}")


    def default_aggregation_state(self):

        return {

            "h3_agg_scale": self.base_metadata["h3_max"],
            "datetime_agg_scale": self.base_metadata["datetime_min"],
            "filters": []
        }

    
    def update_aggregation_state(self, operation, options):

        # TODO: update the state dictionary

        """

        {
            "h3_agg_scale": <None OR Level>,
            "datetime_agg_scale": <None OR Level>,
            "filters": [
                {
                    "dimension": <Name>,
                    "aggregation_scale" <Value>,
                    "value"> <Value>
                },
                ...
            ]
        }
        """

        # Update the aggregation state based on the options
        try:
            if operation == operations.Operation.DRILLDOWN:
                self._update_aggregation_state_drilldown(options)
            elif operation == operations.Operation.ROLLUP:
                self._update_aggregation_state_rollup(options)
            elif operation == operations.Operation.SLICE:
                self._update_aggregation_state_slice(options)
            elif operation == operations.Operation.DICE:
                self._update_aggregation_state_dice(options)
        except Exception as e:
            raise Exception(f"Invalid operation: {e}")

    def reset_aggregation_state(self):
        self.aggregation_state = self.default_aggregation_state()

    def validate_name(name:str):

        if not re.match("^[A-Za-z0-9-_]+_cube", name):
            raise Exception("Cube name may contain only letters, numbers, _, and - and must end in _cube")


    def _validate_filter(self, filter_dict):

        if type(filter_dict) is not dict:
            raise Exception("Type of filter must be dict")

        elif sorted(filter_dict.keys()) != ["column", "op", "val"]:
            raise Exception("Items must be (column, op, val)")
        
        elif type(filter_dict["val"]) not in [int, float]:
            raise Exception("Filter value must be numeric only")

        elif filter_dict['op'] not in ['gt', 'lt', 'eq']:
            raise Exception(f"Metadata filter for '{filter_var}' has invalid operation. Must be in <gt, lt, eq>")
        elif type(filter_dict['val']) not in [str, int, float]:
            raise Exception(f"Metadata filter for '{filter_var}': variable must be a string, int or float")

        elif filter_dict["column"].lower() not in self.base_metadata["measure_colnames"]:
            raise Exception(f"Invalid column: must be in {self.base_metadata['measure_colnames']}")

    def _update_aggregation_state_drilldown(self, drilldown_options):

        # TODO: first, check if this exceeds the max/min
        # Then update the h3 or time aggregation scale, as necessary
        # Then update the filters (get children since drill down)

        new_aggregation_state = self.aggregation_state.copy()

        if drilldown_options["dimension"] == "h3":
            
            # validate semantics against current state
            if self.base_metadata["h3_max"] < drilldown_options["aggregation_scale"]:
                raise Exception("Invalid drilldown scale: exceeds system max")
            elif self.aggregation_state["h3_agg_scale"] >= drilldown_options["aggregation_scale"]:
                raise Exception("Invalid drilldown scale: cannot drill down to less granular view ")

            new_aggregation_state["h3_agg_scale"] = drilldown_options["aggregation_scale"]


        elif drilldown_options["dimension"] == "datetime":
            # validate semantics against current state
            # TODO: represent as a TimeGranularity
            new_datetime_scale = TimeGranularity[drilldown_options["aggregation_scale"].upper()]
            if self.base_metadata["datetime_max"] < new_datetime_scale:
                raise Exception("Invalid drilldown scale: exceeds table max")
            if self.aggregation_state["datetime_agg_scale"] <= new_datetime_scale:
                raise Exception("Invalid drilldown scale: cannot drill down to less granular view ")


            new_aggregation_state["datetime_agg_scale"] = new_datetime_scale #TODO: represent as TimeGranularity
        else:
            raise Exception("Invalid dimension")


        self.aggregation_state = new_aggregation_state
    
    def _update_aggregation_state_rollup(self, rollup_options):

        # TODO: first, check if this exceeds the max/min
        # Then update the h3 or time aggregation scale, as necessary
        # Then update the filters (get parent since roll )

        new_aggregation_state = self.aggregation_state.copy()

        if rollup_options["dimension"] == "h3":
            
            # validate semantics against current state
            if self.base_metadata["h3_min"] > rollup_options["aggregation_scale"]:
                raise Exception("Invalid rollup scale: smaller than table min")
            elif self.aggregation_state["h3_agg_scale"] <= rollup_options["aggregation_scale"]:
                raise Exception("Invalid rollup scale: cannot rollup to more granular view ")

            # update state
            new_aggregation_state["h3_agg_scale"] = rollup_options["aggregation_scale"]


        elif rollup_options["dimension"] == "datetime":
            # validate semantics against current state
            # TODO: represent as a TimeGranularity
            new_datetime_scale = TimeGranularity[rollup_options["aggregation_scale"].upper()]
            if self.base_metadata["datetime_max"] < new_datetime_scale:
                raise Exception("Invalid rollup scale: smaller than table min")
            elif self.aggregation_state["datetime_agg_scale"] > new_datetime_scale:
                raise Exception("Invalid rollup scale: cannot rollup to more granular view ")


            # update state
            new_aggregation_state["datetime_agg_scale"] = new_datetime_scale #TODO: represent as TimeGranularity
        else:
            raise Exception("Invalid dimension")

        self.aggregation_state = new_aggregation_state

    def _update_aggregation_state_slice(self, options):
    
        # add filter to the aggregation state

        new_aggregation_state = self.aggregation_state.copy()

        """
        A filter has the form:
        {
            "dimension": <Name>,
            "aggregation_scale" <Value>,
            "value"> <Value>
        }

        """

        filter = options
        if filter["dimension"] == "datetime":
            filter["aggregation_scale"] = TimeGranularity[filter["aggregation_scale"].upper()]

        # add aggregation scale
        if filter["dimension"] == "h3":
            filter["aggregation_scale"] = h3.h3_get_resolution(filter["value"])
            filter["value"] = "'" + filter["value"] + "'"

        # rename to column
        filter["column"] = filter.pop("dimension")


        if filter not in new_aggregation_state["filters"]:
            new_aggregation_state["filters"].append(filter)

        self.aggregation_state = new_aggregation_state

    def _update_aggregation_state_dice(self, options):

        new_aggregation_state = self.aggregation_state.copy()
        
        # Sequentially update the dice filters
        for filter in options["dice_dimensions"]:
           self._update_aggregation_state_slice(filter)

    def _update_aggregation_state_filter(self, filter):

        new_aggregation_state = self.aggregation_state.copy()

        if filter not in new_aggregation_state["filters"]:
            new_aggregation_state["filters"].append(filter)

        self.aggregation_state = new_aggregation_state



def get_datetime_index_bounds(colnames):

    # crop to only those columns beginning in "datetime"
    datetime_colnames = [c[9:].upper() for c in colnames]

    # transform to enum form
    levels = [TimeGranularity[c] for c in datetime_colnames]
    max_level = TimeGranularity(max(levels))
    min_level = TimeGranularity(min(levels))
    return (min_level, max_level)

def get_max_h3_index(colnames):

    h3_colnames =  [int(c[3:]) for c in colnames]

    max_level = max(h3_colnames)

    return max_level



def get_h3_colname(h: int):

    return f"h3_{h:02}"

def get_date_colname(t: TimeGranularity):

    return "datetime_" + t.name.lower()