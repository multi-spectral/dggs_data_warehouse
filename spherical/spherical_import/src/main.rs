use std::env;

pub mod import;

use clap::Parser;
use import::general::Args;
use import::{import};




#[tokio::main]
async fn main() {

    env::set_var("RUST_BACKTRACE", "1");

    let args = Args::parse();


    match import(args).await {

        Ok(_) => {},
        Err(e) => {println!("{}", e)}
    };
}