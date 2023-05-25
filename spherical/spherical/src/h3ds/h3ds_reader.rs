use std::io::Read;
use std::fs::File;
use std::path::Path;
use byteorder::{ReadBytesExt, LittleEndian};

use crate::h3ds::h3ds::*;

struct RustyH3ArrayReader {
    header: RustyH3ArrayHeader,
    filename: String
}

impl RustyH3ArrayReader {

    pub fn from_file(data_path: String, meta_path: String) -> Result<RustyH3ArrayReader, String> {

        /*
        TODO: determine the data type and check if valid path
        */


        // check if the data path is valid
        let filename = Path::new(&data_path);
        if !filename.exists(){
            return Err("Data path does not exist".to_string());
        }


        // read the header
        let header = RustyH3ArrayHeader::from_file(meta_path.clone())?;


        // prepare the reader with the neccessary data
        Ok(RustyH3ArrayReader {
            filename: data_path,
            header: header
        })
        

    }

    pub fn read_i32(&self) -> Result<RustyH3Array<i32>, String> {

        

        /*
        TODO: read in the binary data from file as a vector,
        read required header fields from metadata json,
        and make the header from the header (using Serde?)

        Then return a RustyH3Array

        See 
        */
        let mut f = File::open(&(self.filename)).unwrap();



        //Read in an array of u8s
        let mut buffer = Vec::<u8>::with_capacity(
            self.header.data_len * self.header.dtype.num_octets()
        ); //with capacity n
        f.read_to_end(&mut buffer).unwrap();

        //Map to i32.
        let vec_i32: Vec<i32> =  buffer.chunks(self.header.dtype.num_octets())
            .into_iter()
            .map(|mut x| x.read_i32::<LittleEndian>().unwrap())
            .collect();


        RustyH3Array::<i32>::from_vec(self.header.clone(), vec_i32)

        }
    }



