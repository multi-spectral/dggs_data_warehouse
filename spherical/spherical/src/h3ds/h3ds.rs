#![allow(dead_code)]

use serde_json;
use std::fs;
use std::fs::File;
use h3o::{Resolution};
use serde_json::json;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::h3_ops::*;


#[derive(Clone)]
pub struct RustyH3ArrayHeader {

    pub h3_res: Resolution,
    pub dtype: DataType,
    pub data_len: usize,
    pub var_name: String, //size has upper limit

}

#[derive(Clone)]
pub enum DataType {
    Float64,
    Int32

}

impl DataType {

    pub fn from_str(dtype: &str) -> Result<DataType, String> {

        match dtype {
            "i32" => Ok(DataType::Int32),
            "f64" => Ok(DataType::Float64),
            _ => Err("No applicable dtype".to_string())
        }
    }

    pub fn num_octets(&self) -> usize {

        match *self {
            DataType::Float64 => 8,
            DataType::Int32 => 4
        }

    }
}

pub struct MetadataOptions {

    pub filename: String,
    pub varname: String
}


pub struct RustyH3Array<T: 'static> {
    header: RustyH3ArrayHeader,
    body: Vec<T> //vector of whatever data type
}

impl RustyH3Array<i32> {

    pub fn from_vec(header: RustyH3ArrayHeader, vec: Vec<i32>) -> Result<RustyH3Array<i32>, String> {

        Ok(RustyH3Array::<i32> {
            header: header,
            body: vec
        })


    }

    pub fn export_to_file(&self, filename: String, meta_options: Option<MetadataOptions>) -> Result<(), String> {

        /*

        Export the dimension to file, and the metadata to metadata file if flag

        */

        match meta_options {
            /*

            Write the metadata as json

            file_metadata:
                - h3_res
                - dtype
                - var_name
            */
            Some(meta_options) => { 
                
                let metadata_json: String = json!({
                    "file_metadata" : {
                        "h3_res": format!("{:?}", self.header.h3_res),
                        "dtype": "i32",
                        "var_name": meta_options.varname
                    }
                }).to_string();
        
                fs::write(meta_options.filename, metadata_json).unwrap();
        
            
            }, 
            _ => { }

        }

        /*

        Export the data

        */

        let mut buffer = File::create(filename).unwrap();

        //write all items to file
        for x in self.body.clone().into_iter(){
            buffer.write_i32::<LittleEndian>(x).unwrap();
        }

        Ok(())

    }

}




impl RustyH3ArrayHeader {

    pub fn new(h3_res: Resolution, dtype: DataType, var_name: String) -> Result<RustyH3ArrayHeader, String> {

        // get the data len
        let data_len = h3_res.cell_count().try_into().unwrap();

        Ok(RustyH3ArrayHeader {
            h3_res: h3_res,
            dtype: dtype,
            data_len: data_len,
            var_name: var_name
        })

    }


    pub fn from_file(meta_path: String) -> Result<RustyH3ArrayHeader, String> {

        /* 
        TODO: read the relevant fields from JSON
        These include the data type, h3 resolution, etc.

        */

        let json_raw = fs::read_to_string(&meta_path).unwrap();

        let data: serde_json::Value = serde_json::from_str(&json_raw).unwrap();
        let file_metadata = &data["file_metadata"];

        //get the h3 resolution
        let h3_res = file_metadata["h3_res"].as_u64().expect("h3 res json field error");
        let h3_res = match_resolution(h3_res.try_into().unwrap()).unwrap();

        //get the dtype
        let dtype = file_metadata["dtype"].as_str().expect("dtype json field error");
        let dtype = DataType::from_str(dtype).unwrap();

        

        // get the var name
        let var_name = file_metadata["var_name"]
                            .as_str()
                            .expect("var_name json field error")
                            .to_string();

        //parse h3 res and get cell counts
        
        

        RustyH3ArrayHeader::new(h3_res, dtype,var_name)

    }

}



    