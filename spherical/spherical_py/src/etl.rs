
use std::collections::VecDeque;

use pyo3::types::*;
use polars::prelude::{DataFrame, DataFrameJoinOps};

pub fn py_extract_metadata(df: DataFrame, metadata_cols: &PyList) 
-> Result<(DataFrame,DataFrame), String> {

    // Create a list of dataframes of unique values for each key
    let metadata_cols_list: Vec<&str> = metadata_cols
            .iter()
            .map(|x| {
                let s: &str = x.extract().expect("string slice conversion failed");
                s
            }).collect();

    let mut metadata_options: VecDeque<DataFrame> = metadata_cols_list
            .iter()
            .map(|x| df.column(x).expect("invalid column")
                        .unique().expect("unique failed")
                        .into_frame())
            .collect();

    
    // If there are no items in the list, just return the empty string
    let df_first = metadata_options.pop_front();
    if let None = df_first {

        // TODO: just add column 1 to df, make metadata and return
    }

    let df_first = df_first.unwrap();


    // Find the cross product of metadata_options
    let df_metadata: DataFrame = metadata_options
        .into_iter()
        .fold(df_first, |df_acc, df_next| {

            df_acc.cross_join(&df_next, None, None)
                .expect("cross join failed")


        })
        .with_row_count("metadata_id", None).expect("adding row count failed");


    // Join the dataframes
    let df: DataFrame = df.left_join(&df_metadata, metadata_cols_list.clone(), metadata_cols_list.clone())
                        .expect("left join of df and metadata df failed");

            
    Ok((df, df_metadata))

}