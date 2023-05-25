
use postgres::{Client, config::Config, NoTls, Transaction, IsolationLevel};
use std::io::{BufRead, BufReader, Write};
use std::collections::HashMap;
use std::str::FromStr;

use indicatif::ProgressIterator;

use std::fs::File;

use crate::import::general::{TimeGranularity, MetadataObject};
use crate::import::general::{get_time_granularity, get_timestamp, get_metadata_paths};
use crate::import::general::Args;

const BATCH_SIZE: usize = 2_000_000;

/*

    Functionality for importing data from common format
    into the database, when provided the connection info.

    Must insert JSON data, as well as structured data.

    Match provided metadata keys to records.

*/



fn insert_metadata(
    pg_client: &mut Client,
    metadata_index: usize,
    m: MetadataObject,
    hm: &mut HashMap<usize, i32>
) -> Result<(), String> {

    /*

    Schema:

    CREATE TABLE dataset_metadata (
    id serial PRIMARY KEY,
    metadata jsonb,
    dt_acquired timestamp,
    source_name text
    );

    */
    let mut v: Vec<postgres::Row> = pg_client.query(
        "INSERT INTO dataset_metadata
        (dt_acquired, source_name, metadata)
        VALUES ($1, $2, $3)
        RETURNING id
        ", 
        &[
            &get_timestamp(&(m.dt_acquired)).expect("parsing date failed"), 
            &(m.source_name), 
            &(m.original_metadata)
        ]
    ).expect("Returned value");

    let db_index: i32 = v.pop().expect("no value returned")
                    .get::<&str, i32>("id");


    hm.insert(metadata_index, db_index);

    

    Ok(())
}



fn insert_fact_table_data(

    pg_client: &mut Client,
    g: TimeGranularity,
    metadata_index_map: &HashMap<usize, i32>,
    binary_csv_buffer: Vec<u8>


) -> Result<(),String> {

    /*
    
    Insert the fact table data using PostgreSQL csv

    
     */

    





    // Start transaction
    let mut pg_transaction: Transaction = pg_client.build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .start()
        .expect("could not start transaction");

    pg_transaction.execute("
        CREATE TEMP TABLE temp_fact
        (h3 text, variable text, value text, dt_info text, metadata_id text);
        ", &[]).expect("creating temp table failed");

    

    //insert data
    // See https://github.com/vincev/tsdbperf/blob/a19e428e58e9127f19a815bcc48becf06bfe2143/src/db.rs#L203-L207
    let mut writer = pg_transaction.copy_in("
        COPY temp_fact(h3, variable, value, dt_info, metadata_id)
        FROM STDIN WITH CSV HEADER;
        ").expect("creating csv writer failed");
    writer.write_all(&binary_csv_buffer).expect("could not write data");
    writer.finish().expect("could not finish write");


    pg_transaction.execute("
        ALTER TABLE temp_fact
        ADD COLUMN db_metadata_id text;
        ", &[]).expect("inserting csv data failed");
       

 
    // update the db_metadata_id column to contain the reference to metadata table
    for (list_index, db_index) in metadata_index_map {

        pg_transaction.execute("

        UPDATE temp_fact
        SET db_metadata_id = $1
        WHERE metadata_id = $2;
        ", &[
            &format!("{db_index}"),
            &format!("{list_index}")
            ]).expect("matching metadata id failed");
    }

    

    // copy temp table to real table
    let mut insert_query_return = match g {


        TimeGranularity::Year => {
            pg_transaction.query("
                WITH nrows AS (
                    INSERT INTO main_by_year
                    (h3, value, variable, year, dataset_metadata_id)
                    SELECT h3::h3index,
                        value::numeric,
                        variable,
                        dt_info::integer, --year
                        db_metadata_id::integer
                    FROM temp_fact
                    RETURNING 1)
                SELECT count(*)::text AS rows_inserted FROM nrows;
                ", &[]).expect("copying to fact table failed")
        }, 
        TimeGranularity::Instantaneous => {
            
            // For number of rows affected by insert:
            //https://stackoverflow.com/questions/4038616/get-count-of-records-affected-by-insert-or-update-in-postgresql
            pg_transaction.query("
                WITH nrows AS (
                    INSERT INTO main_instantaneous
                    (h3, value, variable, datetime, dataset_metadata_id, date_id)
                    SELECT h3::h3index,
                        value::numeric,
                        variable,
                        dt_info::timestamp, --datetime
                        db_metadata_id::integer,
                        to_char(dt_info::timestamp, 'yyyymmdd')::integer as date_id
                    FROM temp_fact
                    RETURNING 1)
                SELECT count(*)::text AS rows_inserted FROM nrows;
                ", &[]).expect("copying to fact table failed")

        }
    };
    
    let nrows =  insert_query_return.pop().expect("no value returned");
    let _nrows = nrows.get::<&str, &str>("rows_inserted");

    pg_transaction.execute("
        DROP TABLE temp_fact;
        ", &[]).expect("dropping temp table failed");

    pg_transaction.commit().expect("failed to commit transaction");

    Ok(())


}

pub fn import(
    args: &Args,
    metadata: Vec<MetadataObject>) -> Result<(), String> {

    //connect to db
    let mut pg_client: Config = Config::from_str(&args.con_str)
                                .expect("Could not create valid config");

                                
    if let Some(pw) = args.get_password(){
        pg_client.password(pw);
    }
    let pg_client = pg_client.connect(NoTls);

    let mut pg_client = match pg_client {
        Ok(client) => client,
        Err(e) => {return Err(format!("Could not connect to PostgreSQL. Check the DB is running and that the password is includd in the connection string{:?}", e).to_string())}
    };

    

    
    //identify the time granularity
    let g: TimeGranularity = get_time_granularity(metadata.clone());
    
    // First insert all the metadata rows
    // and get their inserted IDs
    // and save to hash map
    let mut meta_index_map: HashMap<usize, i32> = HashMap::<usize,i32>::new();
    for (i, m) in metadata.clone().into_iter().enumerate() {

        insert_metadata(&mut pg_client, i, m, &mut meta_index_map)
                            .expect("error inserting metadata row");
    }

    let csv_paths: Vec<String> = get_metadata_paths(metadata);

    for csv_path in csv_paths.into_iter().progress() {

        
        println!("{csv_path}");

        //Batch read CSV data from buffer
        
        let file = BufReader::new(File::open(csv_path).expect("error reading file"));
        let mut lines = file.lines().peekable();

        //skip the header
        let header = lines.next()
                        .expect("failed to get header")
                        .expect("failed to get header");
        
        let re = regex::Regex::new(r"^h3,variable,value,[a-z]+,metadata_id$").unwrap();
        if !re.is_match(&header.clone()) {
            return Err(format!("Incorrect CSV header: {header}. Please provide CSV header in the form h3,variable,value,[year OR datetime],metadata_id."
        ).to_string());
        }

        let delimiter: u8 = b'\n';
        
        while lines.peek().is_some() {

            let mut binary_csv_buffer: Vec<u8> = Vec::with_capacity(BATCH_SIZE* 50);
            // add the header
            binary_csv_buffer.extend_from_slice(header.as_bytes());
            binary_csv_buffer.push(delimiter);

            //Get the BATCH_SIZE next lines and write to buffer
            for _ in 0..BATCH_SIZE {


                


                let n = lines.next();
                if let Some(Ok(line)) = n {
                    binary_csv_buffer.extend_from_slice(line.as_bytes());
                    binary_csv_buffer.push(delimiter);
                } else if let Some(Err(_e)) = n {
                    panic!("Error reading line");
                } else if let None = n {
                    break;
                }
            };


            // Insert the CSV into temp table, then rename metadata
            insert_fact_table_data(
                &mut pg_client, g.clone(), 
                &meta_index_map, binary_csv_buffer).expect("Inserting CSV batch failed");
        }
    }

    Ok(())
}