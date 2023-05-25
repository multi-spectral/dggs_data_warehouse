
use pyo3::prelude::*;
use numpy::ndarray::{ArrayViewMut1, ArrayViewMut2};
use numpy::{PyArray1, PyArray2};
use polars::prelude::{ChunkedBuilder, IntoSeries, NamedFrom, DataFrame};
use pyo3_polars::{PyDataFrame};
use polars::chunked_array::builder::*;
use polars::datatypes::*;

use std::collections::HashMap;


use spherical::h3sample::*;

//def agg_h3(raster, lat_dim, lng_dim, nodata): ...
pub fn agg_h3(
    raster: ArrayViewMut2<'_,i32>,
    lat_dim: ArrayViewMut1<'_,f64>,
    lng_dim: ArrayViewMut1<'_,f64>,
    lat_dim_len: usize,
    lng_dim_len: usize,
    nodata: i32,
    h3_res: usize
) -> PyResult<PyDataFrame> {

    // access element
    let q: i32 = raster[(0,0)];

    // aggregate the values under h3
    let mut hm = HashMap::new();
    for i in 0..lat_dim_len {
        for j in 0..lng_dim_len {

            let lat = lat_dim[i];
            let lng = lng_dim[j];

            //elementwise access
            let value: i32 = raster[(i,j)];

            // check nodata
            if value == nodata {
                continue;
            }

            // get h3 latlng
            let h3 = h3_on_row(lat, lng, h3_res).unwrap();
            let h3 = h3.to_string();

            // add the value to hashmap
            *hm.entry(h3).or_insert(0) += value;

        }
    }

    let mut h3_builder = Utf8ChunkedBuilder::new("h3", 100_000_000, 15);
    let mut value_builder = PrimitiveChunkedBuilder::<Int32Type>::new("value", 100_000_000);
    for (key, value) in hm {

        h3_builder.append_value(key);
        value_builder.append_value(value);

    };


    let df: DataFrame = DataFrame::new(vec![
        h3_builder.finish().into_series(),
        value_builder.finish().into_series()
        ]).unwrap();

    Ok(PyDataFrame(df))

}

