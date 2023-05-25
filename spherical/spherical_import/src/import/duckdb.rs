
use std::collections::HashMap;

use indicatif::ProgressIterator;
use duckdb::{params, Connection, Config, Result};

use uuid::Uuid;
use std::process::Command;

use format_sql_query::QuotedData;

use crate::import::general::{TimeGranularity, MetadataObject};
use crate::import::general::{get_time_granularity, get_metadata_paths};
use crate::import::general::Args;



fn insert_metadata(
    conn: &Connection,
    metadata_vec: &Vec<MetadataObject>,
) -> Result<HashMap<usize, Uuid>, String> {

    /*
    Insert the metadata and return a map of row numbers to the inserted ID in DB
        */

    let mut meta_index_map: HashMap<usize, Uuid> = HashMap::<usize,Uuid>::new();
    for (i, metadata_obj) in metadata_vec.clone().into_iter()
                .enumerate() {

                    //convert to duckdb format and add to hash map
                    // see https://docs.rs/duckdb/latest/duckdb/struct.Connection.html#method.query_row
                    
                    
		    let uuid = Uuid::new_v4();

                    conn.execute("
			INSERT INTO dataset_metadata
			(id, metadata, dt_acquired, source_name)
                        VALUES (?, ?, ?, ?)",
                        params![
                        uuid.clone(),
			&metadata_obj.original_metadata.to_string(),
                        &metadata_obj.dt_acquired,
                        &metadata_obj.source_name
                        ]
                     ).expect("Inserting metadata failed");
		  
                    meta_index_map.insert(i, uuid);
                }
            

 

    Ok(meta_index_map)
}

fn setup_connection(con_str: &str) -> Result<Connection,String> {

    let config = Config::default()
                    .allow_unsigned_extensions()
                        .expect("could not allow unsigned")
                    .enable_external_access(true) // external csv operations
                        .expect("could not allow csv reader and other external access")
                    ; 

    //connect to db
    let connection = Connection::open_with_flags(con_str, config)
                .expect("connecting to db failed");

    let ddb_ext_path = match std::env::var("H3_DUCKDB_EXT_PATH") {
                    Ok(path) => path,
                    Err(e) => {
                        let err_msg = format!("Could not read env var H3_DUCKDB_EXT_PATH: {:?}",e).to_string();
                        return Err(err_msg);
                    }
                };

    //load lods
    connection.execute("LOAD json;", []).expect("could not load json ext");
    

    
    
    // format as quoted data to prevent sql injection
    // TODO: additionally check that the path provided is actually a path
    let load_stmt = format!("load {}", QuotedData(&ddb_ext_path));
    connection.execute(&load_stmt, [])
                .expect(&format!("could not load h3 ext: {ddb_ext_path}"));

    Ok(connection)


}


pub fn import(args: &Args,
metadata: Vec<MetadataObject>) -> Result<(), String> {

    
    //identify the time granularity
    let g: TimeGranularity = get_time_granularity(metadata.clone());

    let connection = setup_connection(&args.con_str).expect("Error connecting to db");

    // First insert all the metadata rows
    // and get their inserted IDs
    // and save to hash map
    let meta_index_map: HashMap<usize, Uuid> = insert_metadata(&connection, &metadata)
        .expect("could not insert metadata");

    let csv_paths: Vec<String> = get_metadata_paths(metadata);

    for csv_path in csv_paths.into_iter().progress() {

        let connection = setup_connection(&args.con_str).expect("Error connecting to db");


        //Read lines from the file and insert based on time granularity
        //TODO: Due to the limitations of the Rust interface, this is not actually a temp table.
        match g.clone() {
            TimeGranularity::Year => {
                connection.execute("
                CREATE OR REPLACE TABLE temp_fact
                (h3 text, variable text, value DOUBLE, year INTEGER, metadata_id text);
                ", [])
                .expect("could not create temp table");
            },

            TimeGranularity::Instantaneous => {

                match connection.execute("
                CREATE OR REPLACE TABLE temp_fact
                    (h3 text,
                    variable text,
                    value DOUBLE,
                    datetime TIMESTAMP,
                    metadata_id text)
                    ;
                ", []) {
                    Ok(x) => x,
                    Err(e) => {return Err(e.to_string())}
                };
                }
        }

        
        // Read in CSV to temp table
        // TODO: This is the best way to do it given the shortcomings of this interface.
        // It is hacky
        // Nonetheless, it is not open to SQL injection, since the actual SQL contains no format arguments.
        // However, it works...!


        connection.close().unwrap();
        
        println!("{csv_path}");
        let sql_command = format!("COPY temp_fact FROM {} (FORMAT CSV, HEADER)",
                            QuotedData(&csv_path));

        
        let insert_command = format!(
            r#"duckdb {:?} -c "{sql_command}""#,
            &args.con_str
        );
        // TODO: fail on inserted 0 rows
        let output = Command::new("sh")
            .arg("-c")
            .arg(insert_command)
            .output().expect("failed to execute command");

        if !output.status.success() {
            //TODO: delete the temp table and reverse previous work
            return Err(format!("{:?}", std::str::from_utf8(&output.stderr)));
        }

        let connection = setup_connection(&args.con_str).expect("Error connecting to db");
        
        // Process the data

        connection.execute("
            ALTER TABLE temp_fact
            ADD COLUMN db_metadata_id UUID;
            ", [])
            .expect("adding column failed");
        

        // update the db_metadata_id column to contain the reference to metadata table
        for (list_index, db_id) in meta_index_map.clone() {

            connection.execute("
            UPDATE temp_fact
            SET db_metadata_id = ?
            WHERE metadata_id = ?;
            ", [format!("{db_id}"), format!("{list_index}")])
            .expect("matching metadata id failed");
        }

        // copy temp table to real table
        let copy_result = match g {


            TimeGranularity::Year => {
                connection.execute("
                    INSERT INTO main_by_year
                    (h3, value, variable, year, dataset_metadata_id)
                    SELECT h3_string_to_h3(h3),
                        value,
                        variable,
                        year, --year
                        db_metadata_id
                    FROM temp_fact;
                    ", [])
            }, 
            TimeGranularity::Instantaneous => {
                
                // For number of rows affected by insert:
                //https://stackoverflow.com/questions/4038616/get-count-of-records-affected-by-insert-or-update-in-postgresql
                connection.execute("
                    INSERT INTO main_instantaneous
                    (h3, value, variable, datetime, dataset_metadata_id, date_id)
                    SELECT h3_string_to_h3(h3),
                        value,
                        variable,
                        datetime,
                        db_metadata_id,
                        strftime(datetime,'%Y%m%d') as date_id
                    FROM temp_fact;
                    ", [])

            }
        };


        // Delete the temp table

        //TODO: if it didn't work, start a new connection to do it
        connection.execute("DROP TABLE temp_fact", [])
            .expect("could not drop temp table");

        if let Err(e) = copy_result {
            return Err(e.to_string());
        };
        
    };

    Ok(())

}
