use std::thread;
use std::ffi::OsStr;

pub mod general;
pub mod postgresql;
pub mod clickhouse;
pub mod duckdb;



use general::{SupportedDb, MetadataObject, Args};


fn determine_system(con_str: &str) -> Result<SupportedDb, String> {


    let conn_re = regex::Regex::new(r"^[a-z]+://").unwrap();


    if let Some(m) = conn_re.find(con_str) {
        match m.as_str() { 

            "postgres://" => Ok(SupportedDb::PostgreSQL),
            "postgresql://" => Ok(SupportedDb::PostgreSQL),
            "clickhouse://" => Ok(SupportedDb::ClickHouse),
            _ => Err("Database {scheme} is not supported.".to_string())
        }
    } else {

        let ext = std::path::Path::new(con_str).extension().and_then(OsStr::to_str).unwrap();

        match ext {
            "db3" => Ok(SupportedDb::DuckDB),
            _ => Err("Incorrect database connection string".to_string())

        }
           
    }
}



pub async fn import(args: Args)
-> Result<(), String> {

    
    
    let metadata_vec: Result<Vec<MetadataObject>,String>
            = crate::import::general::parse_metadata(&args.metadata_path);

    let metadata_vec = match metadata_vec {
        Ok(x) => x,
        Err(e) => {return Err(
                        format!("{:?}: {:?}",e, &args.metadata_path).to_string()
                    )}
    };

    match determine_system(&args.con_str) {

        Ok(SupportedDb::PostgreSQL) => {


            // Spawn in a separate thread to escape from async environment

            thread::spawn( move || {
                crate::import::postgresql::import(&args, metadata_vec)
            }).join().expect("Thread panicked")

        },
        Ok(SupportedDb::ClickHouse) => {

            if &args.db_name == "" {
                return Err("Please provide database name".to_string())
            }

            crate::import::clickhouse::import(&args, metadata_vec).await
        },

        Ok(SupportedDb::DuckDB) => {

            // Spawn in a separate thread to escape from async environment
            
            thread::spawn( move || {
                crate::import::duckdb::import(&args, metadata_vec)
            }).join().expect("Thread panicked")
            
        }
        Err(e) => Err(e)
    }



}