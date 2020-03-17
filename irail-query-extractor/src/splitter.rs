use std::fs::{File, create_dir_all};
use std::io::{prelude::*};
use std::collections::HashMap;
use std::path::Path;
use chrono::NaiveDateTime;
use chrono::format::ParseError;

const TRANSFERS_OUTPUT_DIR: &str = "./output/transfers";
const TRAVELLING_TIME_OUTPUT_DIR: &str = "./output/traveling-time";
const CONNECTIONS_OUTPUT_DIR: &str = "./output/connections";
const RANGE_DIVIDER: i64 = 600; // Divide all durations by this to get a time range of 10 min for each group

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
        ParseInt(::std::num::ParseIntError);
        ParseDate(ParseError);
        SerdeJsonError(serde_json::Error);
    }
}

pub fn split_routes(path: &str) -> Result<()> {
    // Split each route of each query into different groups:
    // - By number of transfers
    // - By total travelling time
    // - By number of connections
    let mut transfers_collection: HashMap<i64, Vec<serde_json::Value>> = HashMap::new();
    let mut travelling_time_collection: HashMap<i64, Vec<serde_json::Value>> = HashMap::new();
    let mut connections_collection: HashMap<i64, Vec<serde_json::Value>> = HashMap::new();

    println!("Splitting query: {}", path);
    
    let mut query_file = File::open(path)?;
    let mut json = String::new();
    query_file.read_to_string(&mut json)?;
    let json: serde_json::Value = serde_json::from_str(&json[..]).expect("JSON formatting error!");
    let routes = &json["routes"];

    for r in routes.as_array() {
        for route in r {
            // Travelling time
            if let Some(connections) = route["connections"].as_array() {
                if let Some(departure_stop) = connections.first() {
                    if let Some(arrival_stop) = connections.last() {
                        let departure_time = NaiveDateTime::parse_from_str(departure_stop["departureTime"].as_str().unwrap(), "%Y-%m-%dT%H:%M:%S.00Z")?;
                        let arrival_time = NaiveDateTime::parse_from_str(arrival_stop["arrivalTime"].as_str().unwrap(), "%Y-%m-%dT%H:%M:%S.00Z")?;
                        let total_travelling_time = arrival_time.signed_duration_since(departure_time).num_seconds();
                        println!("Travel time: {:?}", total_travelling_time);
                        travelling_time_collection.entry(total_travelling_time/RANGE_DIVIDER).or_insert(Vec::new()).push(route.clone());
                    }
                }
                else {
                    eprintln!("No connections found");
                    continue; // Ignore queries where no routes were available
                }
            }
            else {
                panic!("Invalid total travelling time in JSON!");
            }

            // Connections
            if let Some(connections) = route["connections"].as_array() {
                let number_of_connections = connections.iter().count() as i64;
                connections_collection.entry(number_of_connections).or_insert(Vec::new()).push(route.clone());
                println!("Number of connections: {}", number_of_connections);
            }
            else {
                panic!("Invalid number of connections in JSON!");
            }

            // Transfers
            if let Some(number_of_transfers) = route["transfers"].as_i64() {
                transfers_collection.entry(number_of_transfers).or_insert(Vec::new()).push(route.clone());
                println!("Number of transfers: {}", number_of_transfers);
            }
            else {
                panic!("Invalid number of transfers in JSON!");
            }

            // Readability 
            println!("\n");

        }
    }

    // Store output
    store_data(TRANSFERS_OUTPUT_DIR, &transfers_collection)?;
    store_data(TRAVELLING_TIME_OUTPUT_DIR, &travelling_time_collection)?;
    store_data(CONNECTIONS_OUTPUT_DIR, &connections_collection)?;


    Ok(())
}

fn store_data(directory: &str, new_data: &HashMap<i64, Vec<serde_json::Value>>) -> Result<()> {
    create_dir_all(directory)?;
    for key in new_data.keys() {
        let path = format!("{}/{}.json", directory, key);
        let mut data;
        if Path::new(&path[..]).exists() {
            let mut json = String::new();
            let mut transfers_file = File::open(&path)?;
            transfers_file.read_to_string(&mut json)?;
            let json: serde_json::Value = serde_json::from_str(&json[..]).expect("JSON formatting error!");
            data = json.as_array().unwrap().clone();
            data.append(&mut new_data[key].clone());
            println!("Extended JSON file: {}", &path);
        }
        else {
            data = new_data[key].clone();
        }
        let serialized = serde_json::to_string_pretty(&data)?;

        let mut file = File::create(format!("{}/{}.json", directory, key))?;
        file.write_all(serialized.as_bytes()).expect("Writing transfers sort to file failed!");
    }

    Ok(())
}
