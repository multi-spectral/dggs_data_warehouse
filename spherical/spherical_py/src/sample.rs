use numpy::ndarray::{ArrayViewMut1, ArrayViewMut2};
use polars::prelude::{ChunkedBuilder, IntoSeries, NamedFrom, DataFrame, Series};
use geo::geometry::{Point, Coord,LineString, Rect, Polygon};
use geo::BoundingRect;
use proj::{Proj, Transform};
use h3o::{CellIndex,Resolution};
use h3o::geom::ToGeo;
use hextree::{Cell, HexTreeMap, compaction::EqCompactor};
use std::str::FromStr;


use spherical::h3_ops::*;
use spherical::raster_ds::{RasterStruct, IndexBoundingBox};

pub fn sample_polygons_from_raster(
    h3_list: Vec<String>,
    raster: RasterStruct,
    mode: &str
) -> Result<DataFrame, String> {

    /*

    Given a list of h3 indices as strings,
    and a raster as a RasterStruct,
    as well as a pixel aggregation mode,

    Sample each polygon from the raster according to the given mode,
    And return as a polars dataframe.


    */



    //Create hextree to store h3, value pairs
    let mut hextree_map: HexTreeMap<f64, EqCompactor> 
                    = HexTreeMap::with_compactor(EqCompactor);

    for h3_id in h3_list.into_iter() {


        // Check that the mode is valid
        // TODO: do this with enum instead
        if !(vec!["sum"].contains(&mode)) {

            return Err(format!("{mode} is ot a valid mode").to_string());

        }


        let crs = raster.get_crs();
        let nodata = raster.get_nodata();

        /*

        Step 1:
        Gets the polygon for the h3 id, and transform it into polygon
        Then transform polygon to provided CRS

        */
        //get the h3 and its boundary polygon (in degrees, not radians)
        let index: CellIndex = CellIndex::from_str(&h3_id).expect("Invalid index string");
        let (boundary, _) = index
                                .to_geom(true) // lat/lng
                                .expect("failed to get boundary from cell index")
                                .into_inner();


        // swap to lng/lat
        //reproject all the points in the linestring
        //and collect into new polygon

        let boundary_reproj: Vec<Coord> = boundary.into_iter()
                                    .map(|coord| {

                                        let mut coord = Coord::from((coord.x, coord.y));
                                        coord.transform_crs_to_crs("EPSG:4326", &crs)
                                            .expect("crs transfomration failed");

                                        
                                        coord

                                    })
                                    .collect();
        let poly_reproj: Polygon = Polygon::new(
                                LineString::from(boundary_reproj),
                                vec![] //empty vector of inner rings
        );

        /*
        
        Step 2:
        Get each polygon's bounding box,
        And sample from the raster

        For each pixel in the array limits generated from the bounding rect, 
        Get its centroid, and check if within the polygon,
        And that the value is not nodata.

        If these conditions are met, add it to the sum


        */
        let bounding_rect: Rect = poly_reproj.bounding_rect()
                        .expect("failed to get bounding rectangle");

        
        let mut values: Vec<i32> = Vec::new(); //all values that meet condition
        for p in raster.iter_array_limits(bounding_rect)
                        .expect("iterator creation failed") {

            let v: i32 = raster.get_pixel(p);
            if v != nodata {
                values.push(v);
            }


        };

        /*

        Now aggregate the values based on the mode

        */

        let result: f64 = match mode {
            "sum" => values.iter().sum::<i32>().try_into().unwrap(),
            _ => 0.0
        };

        let h3_cell: Cell = Cell::from_raw(
                hex_string_to_binary(h3_id)
                ).expect("Invalid binary data");

        hextree_map.insert(h3_cell, result);


        

    }

    /*

    Last step: convert the h3 map to a polars dataframe

    */

    let (v, r): (Vec<Cell>, Vec<f64>) = hextree_map.iter().unzip();
    let h3_vec: Vec<String> =  v.into_iter()
                            .map(|x| binary_to_hex_string(x.into_raw()))
                            .collect();
    let df = DataFrame::new(vec![

        Series::new("h3",h3_vec),
        Series::new(mode, r)
    ]);

    match df {
        Ok(x) => Ok(x),
        Err(e) => Err(e.to_string())
    }
}