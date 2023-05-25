use std::fs::File;
use std::io::{Read};
use std::path::Path;
use h3o::{Resolution};

use byteorder::{ReadBytesExt, LittleEndian};

use crate::h3ds::dim::*;



pub struct H3SortedDimReader{

    filename: String,
    h3_res: Resolution
}

impl H3SortedDimReader {

    pub fn from_file(filename: String, h3_res: Resolution) -> Result<H3SortedDimReader,String> {

        /*

        Create the validated reader struct

        */

        // First, check if the path is valid
        let path = Path::new(&filename);
        if !path.exists(){
            return Err("Data path does not exist".to_string());
        }


        Ok(H3SortedDimReader {
            filename: filename,
            h3_res: h3_res
        })

    }

    pub fn read(&self) -> Result<H3SortedDim, String> {

        /*

        Actually read the data and return as a H3SortedDim

        */


        //prepare the 
        let data_len: usize = self.h3_res.cell_count().try_into().unwrap();

        //open the file
        let mut f = File::open(&(self.filename)).unwrap();

        //Read in an array of u8s - 8 u8s per u64
        let mut buffer = Vec::<u8>::with_capacity(data_len * 8);
        f.read_to_end(&mut buffer).unwrap();

        //Transform to array of u64
        //Using little endian because it's what my Mac uses
        //Revisit this for PostgreSQL implementation
        let data_vector: Vec<u64> =  buffer.chunks(8)
            .into_iter()
            .map(|mut x| x.read_u64::<LittleEndian>().unwrap())
            .collect();
    

        H3SortedDim::from_binary(self.h3_res, data_vector)

    }
}