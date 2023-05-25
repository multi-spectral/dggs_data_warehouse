/*

Python integration adapted from the pyo3 docs

Use np arrays as inputs: see examples https://github.com/PyO3/rust-numpy
Also https://docs.rs/pyo3-polars/0.2.0/pyo3_polars/

*/


use pyo3::types::*;
use pyo3::prelude::*;
use numpy::ndarray::{ArrayViewMut1, ArrayViewMut2};
use numpy::{PyArray1, PyArray2};
use polars::prelude::{NamedFrom, DataFrame};
use pyo3_polars::{PyDataFrame};
use polars::chunked_array::builder::*;
use polars::datatypes::*;

use std::collections::HashMap;

mod etl;
mod pixelwise;
mod sample;


use pixelwise::*;
use etl::*;


use spherical::h3sample::*;
use spherical::raster_ds::RasterStruct;

use crate::sample::sample_polygons_from_raster;

#[pymodule]
fn spherical_py(_py: Python<'_>, m: &PyModule) -> PyResult<()> {


    #[pyfn(m)]
    #[pyo3(name = "pixel_agg")]
    fn pixel_agg<'py> (
        py: Python<'py>, //first argument must be the Python
        raster: &PyArray2<i32>, //mutable
        lat_dim: &PyArray1<f64>, //mutable
        lng_dim: &PyArray1<f64>, //mutable
        lat_dim_len: usize,
        lng_dim_len: usize,
        nodata: i32,
        h3_res: usize
    ) -> PyResult<PyDataFrame> {

        let out: PyResult<PyDataFrame> = unsafe {
            agg_h3(
                raster.as_array_mut(),
                lat_dim.as_array_mut(),
                lng_dim.as_array_mut(),
                lat_dim_len,
                lng_dim_len,
                nodata,
                h3_res
            )
            
        };

        out


    } 

    #[pyfn(m)]
    #[pyo3(name = "sample_h3_from_raster")]
    fn sample_h3_from_raster<'py>(
        py: Python<'py>,
        mode: &PyString,
        h3_ids: &PyList, 
        raster: &PyArray2<i32>, //mutable
        lat_dim: &PyArray1<f64>, //mutable
        lng_dim: &PyArray1<f64>, //mutable
        lat_dim_len: usize,
        lng_dim_len: usize,
        nodata: i32,
        crs: &PyString

    ) -> PyResult<PyDataFrame> {

        let h3_list: Vec<String> = h3_ids.iter()
                                    .map(|obj| obj.extract::<String>().unwrap())
                                    .collect();


        let raster: RasterStruct = RasterStruct::from_data(

            unsafe {raster.as_array_mut()},
            unsafe {lat_dim.as_array_mut()},
            unsafe {lng_dim.as_array_mut()},
            lat_dim_len,
            lng_dim_len,
            crs.to_string(),
            nodata

        ).expect("Error creating RasterStruct from data");
        
        let mode: &str = &(mode.to_str().expect("error in string conversion"));

        let df = sample_polygons_from_raster(
            h3_list,
            raster,
            mode
        ).expect("error sampling from raster");



        Ok(PyDataFrame(df))
    }


    #[pyfn(m)]
    #[pyo3(name = "extract_metadata")]
    fn extract_metadata<'py> (
        df: PyDataFrame,
        metadata_cols: &PyList
    ) -> PyResult<(PyDataFrame, PyDataFrame)> {

        /*

        Create separate metadata records for each unique combination
        of attributes in the metadata_cols list. Then update the dataframe
        with the column name,  and return the list of metadata items.

        */

        let (df, metadata): (DataFrame, DataFrame) = py_extract_metadata(
                            df.0, //extract the dataframe
                            metadata_cols
                        ).expect("metadata extraction failed");

        Ok((PyDataFrame(df), PyDataFrame(metadata)))
    }

    Ok(())
}


