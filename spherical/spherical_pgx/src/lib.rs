
use pgx::pg_module_magic;
use pgx::prelude::*;
use h3o::{CellIndex, LatLng, Resolution};
use std::str::FromStr;


use byteorder::{LittleEndian, WriteBytesExt};

use spherical::h3ds::h3ds::{RustyH3Array};
use spherical::h3ds::dim::{H3SortedDim};
use spherical::h3_ops::*;



/*
    Functions for manipulating bytea data
    See https://github.com/tcdi/pgx/tree/a96c1b75380cfda210029bc4671b342c4e904fe7/pgx-examples/bytea

*/

pg_module_magic!();


fn to_u64(v: Vec<u8>) -> Vec<u64> {

    v.chunks(8)
    .into_iter()
    .map(|mut x| x.read_u64::<LittleEndian>().unwrap())
    .collect()
}

#[pg_extern]

fn data_cube(input_json: &'static str) -> String {

    return String::from("SELECT 2;");
}
#[pg_extern] //This exports the function
fn get_single_index_in_dim(hex_index: String, bytes: Vec<u8>) ->  i64 {

    //convert to vector
    
    let data_vector: Vec<u64> = to_u64(bytes);
            
    



    dim_get_index(hex_index, &data_vector)

    
    
}

fn dim_get_index(hex_index: String, h3_arr: &Vec<u64>) -> i64 {

    //convert the h3 index
    let h: u64 = hex_string_to_binary(hex_index);

    //binary search for the h3 index (assumes increasing order)
    let idx: i64 = h3_arr
                        .binary_search(&h)
                        .unwrap()
                        .try_into()
                        .unwrap();

    idx


}

#[pg_extern] //This exports the function
fn get_multi_index(indices: Vec<String>, bytes: Vec<u8>) -> Vec<i64>  {

    //reinterpret the h3 data array as a u64 array
    let data_vector: &Vec<u64> = &to_u64(bytes);


    //get the index for each one
    let arr_indices: Vec<i64> = indices.clone()
        .into_iter()
        .map(|h| dim_get_index(h, &data_vector))
        .collect();
    

    arr_indices
    
}



#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::prelude::*;
    use h3o::{CellIndex, LatLng, Resolution};
    use spherical::h3ds::h3ds::{RustyH3Array};
    use spherical::h3ds::dim::{H3SortedDim};
    use spherical::h3_ops::*;



    
    #[pg_test]
    fn test_get_single_index_in_dim() {

         //Specify the h3 res
         /*
        const RES: Resolution = Resolution::Seven;
        const CELL_COUNT: usize = 98_825_162;
        */

        const RES: Resolution = Resolution::Three;
        const CELL_COUNT: usize = 41_162;

    
        
        
        //Generate a dimension
        let dim = H3SortedDim::generate(RES).unwrap();

        //Generate a coord
        let ll = LatLng::new(40.0, -70.0);
        let id: CellIndex = ll.expect("valid coords").to_cell(RES);

        //Baseline: get the index in the generated dim
        let baseline: i64 = dim.find_index_of(id)
                                .unwrap()
                                .try_into().unwrap();


        //Copy data into &[u8]

        let buffer_u64: &mut [u64; CELL_COUNT] = &mut [0; CELL_COUNT];
        for (i,c) in dim.iter_cells().enumerate() {

            if i >= CELL_COUNT {
                break;
            }
            buffer_u64[i] =  cell_to_binary(c);

        }

        
        //reinterpret the h3 data array as a u8 array
        let h3_arr: &[u8] = bytemuck::cast_slice(buffer_u64);
        


        // search using the postgres method
        let compare = crate::get_single_index_in_dim(id.to_string(), h3_arr);
 

        assert_eq!(baseline, compare);


    }
    

    /*
    #[pg_test]
    fn test_get_multi_index() {

 
    
        //Specify the h3 res
        const RES: Resolution = Resolution::Five;
        const CELL_COUNT: usize = 2_016_842;

        

        //Generate a dimension
        let dim = H3SortedDim::generate(RES).expect("Generating H3 dim failed");


        //Generate coords
        let ll = LatLng::new(40.0, -70.0);
        let id: CellIndex = ll.expect("valid coords").to_cell(Resolution::Zero);
        
        let children: Vec<CellIndex> = id.children(RES).collect();

        

        //Baseline: get the index in the generated dim
        let baseline: Vec<i64> = dim.find_indices(children.clone())
                                .expect("Finding indices on struct failed")
                                .into_iter()
                                .map(|x| x as i64)
                                .collect();

        
        //Copy dimension data into a a &[u8]

        //let buffer_u64: &mut [u64; CELL_COUNT] = &mut [0; CELL_COUNT];
        
        for (i,c) in dim.iter_cells().enumerate() {

            //buffer_u64[i] =  cell_to_binary(c);


        }
        
        /*
        

        
        //reinterpret the h3 data array as a u8 array
        let h3_arr: &[u8] = bytemuck::cast_slice(buffer_u64);
     


        // search using the postgres method
        let children: Vec<String> = children
                                        .into_iter()
                                        .map(|x| x.to_string())
                                        .collect();
        let compare = crate::get_multi_index(children, h3_arr);

        assert_eq!(baseline, compare);
        */

    }*/

}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
