#![allow(dead_code)]

use polars::prelude::*;
use polars_lazy::prelude::*;
use h3o::{LatLng};

use crate::h3_ops::match_resolution;


pub fn h3_on_row(lat: f64, lng: f64, res: usize) ->  Option<String> {

    let resolution = match_resolution(res).unwrap();

    let ll = LatLng::new(lat, lng);

    let id = ll.expect("valid coords").to_cell(resolution).to_string();

    Some(id)


}

pub fn re_agg_df(df: LazyFrame) -> Result<DataFrame, PolarsError> {

    df
    .clone()
    .groupby(
        [col("h3_07")]
    )
    .agg(
        [
            col("value").sum()
        ]
    )
    .collect()
}


pub fn agg_df(df: LazyFrame) -> Result<DataFrame, PolarsError> {
    
    df
    .clone()
    .with_column(
        as_struct(&[col("lat"), col("lng")])
            .apply(|s| {
                //downcast to a struct
                let ca = s.struct_()?;

                //get fields as series
                let ca_lat = &ca.fields()[0].f64()?;
                let ca_lng = &ca.fields()[1].f64()?;

                //iterate the arrays
                let out: Utf8Chunked = ca_lat
                            .into_iter()
                            .zip(ca_lng.into_iter())
                            .map(|(opt_lat, opt_lng)| match (opt_lat, opt_lng) {
                                (Some(lat), Some(lng)) => 
                                    h3_on_row(lat, lng, 7),
                                _ => None,
                            })
                            .collect();

                Ok(Some(out.into_series()))


            },
        GetOutput::from_type(DataType::Utf8),
        )
            .alias("h3_07")
    )
    .groupby(
        [col("h3_07")]
    )
    .agg(
        [
            col("value").sum()
        ]
    )
    /*
    .sort(
        "value",
        SortOptions {
            descending: true,
            nulls_last: true,
            multithreaded: true
        }
    )
    */
    .collect()

}

