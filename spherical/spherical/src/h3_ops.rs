
use h3o::{CellIndex, Resolution};


pub fn bin_string_to_binary(h: String) -> u64 {

    u64::from_str_radix(&h,2).expect("invalid bin formatting in String")
}
pub fn cell_to_binary(h: CellIndex) -> u64 {
   
    //use CellIndex's format method to convert to binary
    let s = format!("{h:b}").to_string();

    bin_string_to_binary(s)
}

pub fn hex_string_to_binary(h: String) -> u64 {

    u64::from_str_radix(&h,16).expect("Invalid hex formatting in String")
}

pub fn binary_to_hex_string(h: u64) -> String {

    CellIndex::try_from(h).expect("Invalid cell index")
        .to_string()
}

pub fn match_resolution(res: usize) -> Option<Resolution> {

    let r = match res {
        0 => Resolution::Zero,
        1 => Resolution::One,
        2 => Resolution::Two,
        3 => Resolution::Three,
        4 => Resolution::Four,
        5 => Resolution::Five,
        6 => Resolution::Six,
        7 => Resolution::Seven,
        8 => Resolution::Eight,
        9 => Resolution::Nine,
        10 => Resolution::Ten,
        11 => Resolution::Eleven,
        12 => Resolution::Twelve,
        13 => Resolution::Thirteen,
        14 => Resolution::Fourteen,
        15 => Resolution::Fifteen,
        _ => {return None}
        
    };

    Some(r)
}