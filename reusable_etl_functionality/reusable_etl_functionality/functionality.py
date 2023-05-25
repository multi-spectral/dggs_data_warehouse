
import os
import h3
from pathlib import Path


def make_data_directory(path):
    try:
        os.makedirs(path)
    except OSError as err:
        if "File exists:" in str(err):
            pass


def get_h3(row):

    lat = row.y
    lng = row.x

    return h3.geo_to_h3(lat, lng, H3_RES)


def extract_categorical_variables_as_metadata(df, metadata_cols: list):
    """
    Create metadata indices and map to categorical variables
    """

    # get unique key combos
    tuples = df[metadata_cols].drop_duplicates().reset_index(drop=True)
    # add numbering column
    tuples = tuples.reset_index().rename(columns={"index": "metadata_id"})

    # join to dataframe
    merged = df.merge(tuples, how="left", left_on=metadata_cols, right_on=metadata_cols)

    #metadata_items
    metadata_items = tuples.drop(columns=["metadata_id"]).to_dict(orient="records")

    return merged, metadata_items
    

 