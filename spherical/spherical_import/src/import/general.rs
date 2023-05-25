use serde_json::{Value};
use serde::{Deserialize};

use std::fs::File;
use std::io::{BufReader};

use std::path::Path;

use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about=None)]
pub struct Args {
    #[arg(long, default_value_t=String::from(""))]
    pub db_name: String,
    
    #[arg(long)]
    pub metadata_path: String,

    #[arg(long)]
    pub con_str: String,

    #[arg(long, default_value_t=false)]
    pub ask_password: bool,

    #[arg(long,default_value_t=String::from(""))]
    pub password: String

}

impl Args {

    pub fn get_password(&self) -> Option<String> {

        if self.ask_password {

            let password = rpassword::prompt_password("Enter database password: ").unwrap();
            return Some(password);
        }else if self.password != "" {

            return Some(self.password.clone());
        } else {
            return None
        }
            ;

    }

}

pub fn get_metadata_paths(metadata: Vec<MetadataObject>) -> Vec<String> {


    // get the path stems
    metadata.clone().into_iter().nth(0).expect("no metadata").csv_paths





}


#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum TimeGranularity {
    Instantaneous,
    Year

}


#[derive(Clone, Debug, Deserialize)]
pub struct MetadataObject {

    pub time_granularity: TimeGranularity,
    pub dt_acquired: String,
    pub source_name: String,
    pub original_metadata: Value,
    pub csv_paths: Vec<String>

}

fn get_full_csv_path(mut q: MetadataObject, metadata_path: &str) -> MetadataObject {

    let folder_path = Path::new(metadata_path).parent().expect("could not get parent");

    q.csv_paths = q.csv_paths.clone().into_iter()
                            .map(|p| folder_path.join(p).into_os_string().into_string().expect("could not concatenate path"))
                            .collect();

    q
}





pub enum SupportedDb {

    PostgreSQL,
    ClickHouse,
    DuckDB
}


pub fn parse_metadata(filename: &str) -> Result<Vec<MetadataObject>, String> {

    let file = match File::open(filename) {
        Ok(x) => x,
        Err(e) => {
            return Err(e.to_string());
        }
    };


    let reader = BufReader::new(file);

    // First parse the json into vector of jsons
    // First assume it is a list
    let v: Result<Vec<MetadataObject>,_> = serde_json::from_reader(reader);

    let single_metadata: Result<MetadataObject, _>;

    // handle the case where it's many json first
    // if it fails, move to single json case
    if let Ok(mut q) = v {

        q = q.into_iter().map(|x| get_full_csv_path(x.clone(), filename)).collect();
        return Ok(q);
    }

    // Next try it as a single object
    let file = File::open(filename).expect("opening file failed");
    let reader = BufReader::new(file);
    single_metadata = serde_json::from_reader(reader);


    match single_metadata {
        Err(e) => Err(e.to_string()),
        Ok(mut q) => {
            q = get_full_csv_path(q.clone(), filename);
            Ok(vec![q])
         } //package as vector
    }

}


pub fn get_timestamp(datetime_str: &str) -> Result<chrono::NaiveDateTime, String> {

    let timestamp = chrono::NaiveDateTime::parse_from_str(
        datetime_str,
        "%Y-%m-%d %H:%M:%S").expect(&format!("Date parsing failed for datetime: {:?}", datetime_str));

    Ok(timestamp)
}

pub fn get_time_granularity(mv: Vec<MetadataObject>) -> TimeGranularity {

    //get the time granularity from the first metadata row
    let first_meta: MetadataObject = mv
                                    .clone().remove(0);

    let g: TimeGranularity = first_meta.time_granularity.clone();

    g

}

