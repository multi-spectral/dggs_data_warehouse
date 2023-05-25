"""

This file specifies the operation construction

Operations are provided to the cube interface in the following format:

{

   "operation": <drilldown, rollup, slice, dice>

   "options": <operation-specific options, which specify the aggregation (NOT which measure to use)

}

Each of these should modify the state, from which a query is built.
The state is a dictionary with a standard format. It is of the form:

SELECT {columns}, {measure} FROM cube_table
WHERE
    <slice filter 1> AND
    <slice filter 2> AND
    ....
GROUP BY CUBE({rollup/drilldown by h3 AND/OR rollup/drilldown by time})

This is built from a dict of of the following form. This dict is the underlying state.
{
    "h3_agg_scale": <None OR Level>,
    "time_agg_scale": <None OR Level>,
    "filters": [
        {
            "column": <Name>,
            "isin_range": [start, end]
        },
        {
            "column": <Name>,
            "equals": <Value>
        },
        ...
    ]
}

Additional state is calculated from the column names of the underlying cube table.



"""

from enum import Enum
from utils import execute_query, TimeGranularity
import h3

class Operation(Enum):
    DRILLDOWN = 1
    ROLLUP = 2
    SLICE = 3
    DICE = 4


def extract_operation(description: dict):

    # TODO: extract the operation into the operation enum
    # Also, validate that the options are in the correct form for that operation
    # Don't pass raw SQL in. Instead, express the slice and dice operations using the JSON syntax

    """

    DRILLDOWN: {column, scale_level}
    ROLLUP: {column, scale_level}
    SLICE: {} <This is filtering along a single dimension > {}
    DICE: {} <This is filtering along multiple dimensions> -> implement as list of slices (check that this is correct)
    *** Dice should have exactly <n> dimensions.

    """

    # Validate dict structure
    if "operation" not in description:
        raise Exception("Does not contain operation.")
    elif "options" not in description:
        raise Exception("Does not contain options.")
    elif len(list(description.keys())) != 2:
        raise Exception("Description may contain exactly two items (operation, options) only.")

    # Validate option
    operation_id = description["operation"]
    try:
        operation_type = Operation[operation_id.upper()]
    except Exception as e:
        raise Exception("Operation must be one of: drilldown, rollup, slice, dice")


    options = description["options"]
    try:
        validate_operation(operation_type, options)
    except Exception as e:
        raise Exception(f"Operation {operation_id} has invalid options: {e}")


    return operation_type, options

def validate_operation(operation_type: Operation, options):

    # Validate the operation and its options
    # Options format must be correct for the operation
    # If not correct, raise exception

    if type(options) is not dict:
        raise Exception("Type of options must be dict")

    try:
        if operation_type == Operation.DRILLDOWN:
            validate_drilldown(options)
        elif operation_type == Operation.ROLLUP:
            validate_rollup(options)
        elif operation_type == Operation.SLICE:
            validate_slice(options)
        elif operation_type == Operation.DICE:
            validate_dice(options)
        else:
            raise Exception(f"{operation_type} is an invalid operation")
    except Exception as e:
        raise Exception(f"Invalid options for operation {operation_type}: {e}")

def validate_drilldown(options: dict):

    # Validate the drilldown options syntax
    # If invalid, raise exception
    # Can select either h3 or time dimension
    # This just validates that the syntax is correct.


    """
    Example:

    {
        "dimension": "h3",
        "aggregation_scale": 6
    }
    """

    keys = sorted(options.keys())
    if keys != ["aggregation_scale", "dimension"]:
        raise Exception("Keys must be (dimension, aggregation_scale)")
    dim = options["dimension"]
    if dim not in ["h3", "datetime"]:
        raise Exception("Dimension must be one of (h3, datetime)")
    if dim == "h3" and type(options["aggregation_scale"]) is not int:
        raise Exception("h3 aggregation scale must be provided as int")
    if dim == "datetime":
        try:
            a = TimeGranularity[options["aggregation_scale"].upper()]
        except Exception:
            raise Exception(f"Invalid datetime aggregation scale {options['aggregation_scale']}")

def validate_rollup(options: dict):

    """
    Example:

    {
        "dimension": "h3",
        "aggregation_scale": 5
    }
    """

    # Validate the rollup options syntax
    # Can select either h3 or time dimension
    # This just validates that the syntax is correct. 

    keys = sorted(options.keys())
    if keys != ["aggregation_scale", "dimension"]:
        raise Exception("Keys must be (dimension, aggregation_scale)")
    dim = options["dimension"]
    if dim not in ["h3", "datetime"]:
        raise Exception("Dimension must be one of (h3, datetime)")
    if dim == "h3" and type(options["aggregation_scale"]) is not int:
        raise Exception("h3 aggregation scale must be provided as int")
    if dim == "datetime":
        try:
            a = TimeGranularity[options["aggregation_scale"].upper()]
        except Exception:
            raise Exception(f"Invalid datetime aggregation scale {options['aggregation_scale']}")

def validate_slice(options: dict):

    """
    Example: 
    {
        "dimension": "h3",
        "value": 409203482305
    }
    """

    if options["dimension"] == "h3":
        # validate h3
        try:
            h3.h3_is_valid(options["value"])
        except Exception:
            raise Exception("invalid h3 index")

        if sorted(options.keys()) != ["dimension", "value"]:
            raise Exception("for h3, provide (dimension, value)")


    else:
        try:
            TimeGranularity[options["aggregation_scale"].upper()]
        except Exception:
            raise Exception("aggregation_scale invalid")

        if sorted(options.keys()) != ["aggregation_scale", "dimension", "value"]:
            raise Exception("for datetime, provide (dimension, aggregation_scale, value)")
        

    return options


def validate_dice(options: dict):

    """
    Example:
    {
        "dice_dimensions": [
            {
                "dimension": "h3",
                "aggregation_scale": 5,
                "value": 293847294
            },
            {
                "dimension": "datetime",
                "aggregation_scale": "month",
                "value": "2012-06"
            }


        ]
    }
    """

    if sorted(options.keys()) != ["dice_dimensions"]:
        raise Exception("Keys for dice should be (dice_dimensions)")
    if type(options["dice_dimensions"]) is not list:
        raise Exception("Provided value should be list of filters")

    for filter in options["dice_dimensions"]:
        filter = validate_slice(filter)



