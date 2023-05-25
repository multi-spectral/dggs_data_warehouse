use numpy::ndarray::{ArrayViewMut1, ArrayViewMut2};
use geo::geometry::{Rect, Coord};
use geo::coord;
use float_ord::FloatOrd;


#[derive(Debug)]
pub struct IndexCoord {

    pub x: usize,
    pub y: usize
}

impl IndexCoord {

    fn new(x: usize, y: usize) -> IndexCoord {

        IndexCoord {
            x: x, y: y
        }
    }
}

#[derive(Debug)]
pub struct IndexBoundingBox {

    pub min_x: usize,
    pub min_y: usize,
    pub max_x: usize,
    pub max_y: usize
}

impl IndexBoundingBox {


    pub fn pixel_iterator(&self)
    -> impl Iterator<Item = IndexCoord>{

        /*
        Return an iterator of all the (x,y) indices within the bounding box.

        Need to loop the iterator around, to include pixels 
        */

        let mut v: Vec<IndexCoord> = Vec::new();

        

        for i in self.min_x..self.max_x+1 {
            for j in self.max_y..self.min_y+1 {
                v.push(IndexCoord::new(i,j));
            }
        }



        v.into_iter()


    }
}

pub struct RasterStruct<'a> {

    raster: ArrayViewMut2<'a,i32>,
    lat_dim: Vec<FloatOrd<f64>>,
    lng_dim: Vec<FloatOrd<f64>>,
    crs: String,
    nodata: i32,

}

impl RasterStruct<'_> {

    pub fn from_data<'a>(
        raster: ArrayViewMut2<'a,i32>,
        lat_dim: ArrayViewMut1<'_,f64>,
        lng_dim: ArrayViewMut1<'_,f64>,
        lat_dim_len: usize,
        lng_dim_len: usize,
        crs: String,
        nodata: i32,
    ) -> Result<RasterStruct<'a>, String> {


       let lat: Vec<FloatOrd<f64>> = (0..lat_dim_len)
                            .map(|y| FloatOrd(lat_dim[y]))
                            .collect();

        let lng: Vec<FloatOrd<f64>> = (0..lng_dim_len)
                            .map(|x| FloatOrd(lng_dim[x]))
                            .collect();
                
        let raster: RasterStruct<'a> = RasterStruct {
                        raster: raster,
                        lat_dim: lat,
                        lng_dim: lng,
                        crs: crs,
                        nodata: nodata
                    };

        Ok(raster)

        
    }

    pub fn get_crs(&self) -> String {

        self.crs.clone()

    }

    pub fn get_nodata(&self) -> i32 {

        self.nodata

    }

    pub fn shape(&self) -> (usize, usize) {

        (self.lng_dim.len(), self.lat_dim.len())
    }

    pub fn get_lat_index_of(&self, y: f64) -> usize {

         // Assumes lat is sorted in decreasing order

        match self.lat_dim.binary_search_by(|item| FloatOrd(y).cmp(item)) {
            Ok(x) => x,
            Err(x) => x
        }
    }

    pub fn get_lng_index_of(&self, x: f64) -> usize {

        // Assumes lng is in increasing order

       match self.lng_dim.binary_search_by(|item| item.cmp(&FloatOrd(x))) {
            Ok(x) => x,
            Err(x) => x
       }
    }


    pub fn get_array_limits(&self, bbox: Rect) 
    -> Result<IndexBoundingBox, String>{

        /*

            Get the array limits as a tuple
            min_x, min_y, max_x, max_y

            Assumes both are in same CRS
            Assumes rectangle is contained within the array
            TODO: return Err if rectangle not contained in limits


        */

        // get out the min and max coordinates
        let min: Coord = bbox.min();
        let max: Coord = bbox.max();

        // look up in lat_dim and lng_dim
        // if exact index not found, returns the index where it would be inserted
        let min_x  = self.get_lng_index_of(min.x);
        let min_y = self.get_lat_index_of(min.y);
        
        let mut max_x = self.get_lng_index_of(max.x);
        let mut max_y = self.get_lat_index_of(max.y);


        // get the array dimension
        let (shape_x, shape_y) = self.shape();

        // add 1 to the max values to ensure polygon is contained
        if max_y < shape_y - 2  { max_y = max_y + 1};
        if max_x < shape_x - 2 { max_x = max_x + 1};

        Ok(IndexBoundingBox{min_x, min_y, max_x, max_y})

        
      
    }

    pub fn iter_array_limits(&self, bbox: Rect) 
    -> Result<impl Iterator<Item = IndexCoord>, String> {

        let ibb = self.get_array_limits(bbox)
            .expect("Getting bounding box failed");

        Ok(ibb.pixel_iterator())

    }

    pub fn get_pixel(&self, i: IndexCoord) -> i32 {

        self.raster[(i.y,i.x)]

    }

    


}