#![allow(dead_code)]

//use polars::prelude::*;
use std::time::Instant;
use std::path::Path;
use rand::Rng;

mod h3sample;
mod h3_ops;
use h3_ops::*;

mod h3ds {
    pub mod h3ds;
    pub mod h3ds_reader;
    pub mod dim;
    pub mod dim_reader;
}

use h3ds::h3ds::*;
use h3ds::dim::*;


fn main() {

    //h3 dimension directory:
    let mut rng = rand::thread_rng();

    for i in 1..3 {

        


        let h3_res = match_resolution(i).unwrap();
        let varname = "TEST_VAR".to_string();
        let data_type = DataType::from_str("i32").unwrap();

        //generate the h3 sorted dimension
        let h3_dim_generated: H3SortedDim = H3SortedDim::generate(h3_res).unwrap();

        //generate a vector of i32 with the same dimension
        let i32_out_data: Vec<i32> = (0..h3_dim_generated.len())
                                        .into_iter()
                                        .map(|_| rng.gen_range(0..200))
                                        .collect();
        let header: RustyH3ArrayHeader = RustyH3ArrayHeader::new(
            h3_res, 
            data_type,
            varname.clone()
        ).unwrap();

        //generate the RustyH3Array
        let ra = RustyH3Array::<i32>::from_vec(
            header, i32_out_data
        ).unwrap();


        let filename = Path::new(&out_dir)
                        .join(format!("{:?}.h3", i))
                        .into_os_string()
                        .into_string()
                        .unwrap();

        let meta_filename = Path::new(&out_dir)
                        .join(format!("{:?}.mdjson", i))
                        .into_os_string()
                        .into_string()
                        .unwrap();

        
        //export to file
        /*
        
        println!("{:?}", filename);
        h3_dim.export_to_file(filename);
        */

        //read from file
        let start = Instant::now();

        ra.export_to_file(filename, Some(MetadataOptions { filename: meta_filename, varname: varname.clone() }));

    

        let duration = start.elapsed();
        println!("Resolution: {:?} Time taken to import: {:?}", i, duration);

        //assert that they are same

        

    }

    


    


}
