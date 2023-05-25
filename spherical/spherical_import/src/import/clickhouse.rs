use clickhouse::{Client};
use std::collections::HashMap;

use indicatif::ProgressIterator;

use uuid::Uuid;

use tokio::process::Command; //replace std::process::Command because of async/await

use crate::import::general::{TimeGranularity, MetadataObject};
use crate::import::general::{get_time_granularity, get_metadata_paths};
use crate::import::general::Args;



async fn insert_metadata(
    client: &mut Client,
    metadata_vec: &Vec<MetadataObject>,
) -> Result<HashMap<usize, Uuid>, String> {

    /*
    let mut inserter = ch_client.insert("dataset_metadata")
        .expect("could not insert to dataset_metadata table");
        */

    let mut meta_index_map: HashMap<usize, Uuid> = HashMap::<usize,Uuid>::new();
    for (i, metadata_obj) in metadata_vec.clone().into_iter()
                .enumerate() {

                    //convert to clickhouse format and add to hash map
 
                    let id = uuid::Uuid::new_v4();
                    
                    meta_index_map.insert(i, id);

                    client.query("
                    INSERT INTO dataset_metadata
                    (id, metadata, dt_acquired, source_name)
                    VALUES (?, ?, ?, ?);")
                    .bind(id)
                    .bind(&metadata_obj.original_metadata.to_string())
                    .bind(&metadata_obj.dt_acquired)
                    .bind(&metadata_obj.source_name)
                    .execute()
                        .await.expect("could not insert. Restarting the Clickhouse instance may help");
                        //TODO: suggest to restart the clickhouse service
                }
            

 

    Ok(meta_index_map)
}


pub async fn import(args: &Args,
metadata: Vec<MetadataObject>) -> Result<(), String> {


    //TODO: check connection is active first

    //rewrite the connection str
    // port should be 8123. https://clickhouse.com/docs/en/interfaces/http/
    let con_str = regex::Regex::new(r"^clickhouse://").unwrap()
                    .replace_all(&args.con_str, "http://");

    

    //connect to db
    let mut client = Client::default().with_url(con_str)
                                        .with_database(&args.db_name);
                             
    
    //identify the time granularity
    let g: TimeGranularity = get_time_granularity(metadata.clone());

    // First insert all the metadata rows
    // and get their inserted IDs
    // and save to hash map
    let meta_index_map: HashMap<usize, Uuid> = insert_metadata(&mut client, &metadata).await
        .expect("could not insert metadata");


    let csv_paths: Vec<String> = get_metadata_paths(metadata);
    for csv_path in csv_paths.into_iter().progress() {
        //Read lines from the file and insert based on time granularity
        //TODO: Due to the limitations of the Rust interface, this is not actually a temp table.


        match g.clone() {
            TimeGranularity::Year => {
                client.query("
                CREATE OR REPLACE TABLE temp_fact
                (h3 text, variable text, value Float64, year UInt16, metadata_id text)
                ENGINE=MergeTree ORDER BY h3;
                ").execute().await
                .expect("could not create temp table");
            },

            TimeGranularity::Instantaneous => {

                client.query("
            CREATE OR REPLACE TABLE temp_fact
            (h3 text, variable text, value Float64, datetime DateTime('UTC'), metadata_id text)
            ENGINE=MergeTree ORDER BY h3;
            ").execute().await
            .expect("could not create temp table");
            }
        }

        
        // Read in CSV to temp table
        // TODO: This is the best way to do it given the shortcomings of this interface.
        // It is hacky
        // Nonetheless, it is not open to SQL injection, since the actual SQL contains no format arguments.
        // However, it works...!
        println!("{csv_path}");
        let query = format!(
            //r#"clickhouse-client --database={:?} --query  "INSERT INTO temp_fact FROM INFILE '{:?}' FORMAT CSVWithNames;""#,
            "clickhouse-client --database={:?} --query 'INSERT INTO temp_fact FORMAT CSVWithNames' < {:?}",
                                    &args.db_name, 
                                    csv_path);
        //sh -c "clickhouse-client --query 'INSERT INTO temp_fact FORMAT CSV' < {quoted-csv-file}" 
        // https://clickhouse.com/docs/en/interfaces/cli/
        // TODO: fail on inserted 0 rows

 
        let output = Command::new("sh")
            .arg("-c")
            .arg(query)
            .output().await.expect("failed to execute command");

        if !output.status.success() {
            return Err(format!("{:?}", output).to_string());
        };


        // Process the data

        client.query("
            ALTER TABLE temp_fact
            ADD COLUMN db_metadata_id UUID;
            ",).execute().await
            .expect("adding column failed");
        

    
        // update the db_metadata_id column to contain the reference to metadata table
        for (list_index, metadata_uuid) in meta_index_map.clone() {

            client.query("
            ALTER TABLE temp_fact
            UPDATE db_metadata_id = ?
            WHERE metadata_id = ?;
            ").bind(metadata_uuid) //Need to quote the uuid
            .bind(format!("{list_index}"))
            .execute().await
            .expect("matching metadata id failed");
        }

        // copy temp table to real table
        let copy_result = match g {


            TimeGranularity::Year => {
                client.query("
                    INSERT INTO main_by_year
                    (h3, value, variable, year, dataset_metadata_id)
                    SELECT stringToH3(h3),
                        value,
                        variable,
                        year, --year
                        db_metadata_id
                    FROM temp_fact;
                    ")
                    .execute().await
            }, 
            TimeGranularity::Instantaneous => {
                
                // For number of rows affected by insert:
                //https://stackoverflow.com/questions/4038616/get-count-of-records-affected-by-insert-or-update-in-postgresql
                client.query("
                    INSERT INTO main_instantaneous
                    (h3, value, variable, datetime, dataset_metadata_id, date_id)
                    SELECT stringToH3(h3),
                        value,
                        variable,
                        datetime,
                        db_metadata_id,
                        formatDateTime(datetime,'%Y%m%d') as date_id
                    FROM temp_fact;
                    ",)
                    .execute().await


        }
        };


        // Delete the temp table

        //TODO: if it didn't work, start a new connection to do it
        client.query("DROP TABLE temp_fact")
            .execute().await
            .expect("could not create temp table");

        if let Err(e) = copy_result {
            return Err(e.to_string());
        };

    };

    return Ok(());

}