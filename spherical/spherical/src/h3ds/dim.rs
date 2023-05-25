use h3o::{Resolution, CellIndex};
use std::fs::File;

use byteorder::{LittleEndian, WriteBytesExt};

use crate::h3_ops::*;


/*

    This file contains tools to generate the dimension, and sort h3 ids.

*/

pub struct H3SortedDim {


    h3_res: Resolution,
    sorted_cells: Vec<CellIndex>

}

impl PartialEq for H3SortedDim {
    fn eq(&self, other: &Self) -> bool {

        if self.h3_res != other.h3_res {
            return false
        }


        for (s, t) in self.sorted_cells
                        .clone()
                        .into_iter()
                        .zip(other.sorted_cells.clone()) {
            if s != t {
                return false
            }
        }

        true
    }
}


impl H3SortedDim {

    pub fn len(&self) -> usize {

        self.sorted_cells.len()

    }

    pub fn generate(h3_res: Resolution) -> Result<H3SortedDim,String> {

        /*

            Generate a struct representing the H3 sorted dimension (axis) for a given h3 index.

        */

        //convert the h3 resolution
    
        //first generate the res0 ids
        let base_cells = CellIndex::base_cells();
    
        //next generate all children of the size
        let mut sorted_cells: Vec<CellIndex> = base_cells
                        .map(|x| x.children(h3_res))
                        .into_iter().flatten()
                        .collect::<Vec<CellIndex>>(); 
                        
        // Sort in place
        sorted_cells.sort(); //Take advantage of CellIndex Ord trait
    
        Ok(H3SortedDim {

            h3_res: h3_res,
            sorted_cells: sorted_cells
        })
    
    }

    pub fn iter_cells(&self) -> std::vec::IntoIter<CellIndex> {

        self.sorted_cells.clone().into_iter()
    }

    pub fn from_binary(h3_res: Resolution, bin_array: Vec<u64>) -> Result<H3SortedDim, String> {

        /*
        Create an H3SortedDim from binary data
        */

        let cells: Vec<CellIndex> = bin_array
                                    .into_iter()
                                    .map(|x| CellIndex::try_from(x).unwrap())
                                    .collect();
        

        Ok(H3SortedDim {
            h3_res: h3_res,
            sorted_cells: cells
        })

    }

    pub fn find_index_of(&self, cell: CellIndex) -> Result<usize, usize> {

        self.sorted_cells.binary_search(&cell) //assumes sorted in ascending order

    }

    pub fn find_indices(&self, cells: Vec<CellIndex>) -> Result<Vec<usize>, String>{

        /*
        Naive implementation: just individually search for each
        TODO: more efficient implementation
        */

        let result = cells
                        .into_iter()
                        .map(|x| 
                            self.find_index_of(x).expect("Find index failed")
                        ).collect();

        Ok(result)



    }

    pub fn export_to_file(&self, filename: String) -> Result<(), String> {
        /*

        Export the dimension to file

        */

        //TODO: endianness
        let u64_list: Vec<u64> = self.sorted_cells
            .clone()
            .into_iter()
            .map(|x| cell_to_binary(x))
            .collect::<Vec<u64>>();
            

        let mut buffer = File::create(filename).unwrap();

        //write all items to file
        for x in u64_list.into_iter(){
            buffer.write_u64::<LittleEndian>(x).unwrap();
        }

        Ok(())

    }

}



