use std::fs::{File, read_dir};
use std::io::{prelude::*, BufReader};
use chrono::{DateTime, Utc, NaiveDateTime};
use chrono::format::ParseError;
use std::fs::create_dir_all;
use super::dateformatter;
use super::splitter;

extern crate serde;
extern crate serde_json;

const ARCHIVE_DIR: &str = "./archive";
const OUTPUT_DIR: &str = "./output";
const IRAIL_VEHICLE: &str = "https://api.irail.be/vehicle?format=json&id=";
const VEHICLE_URI: &str = "http://irail.be/vehicle";

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
        ParseInt(::std::num::ParseIntError);
        ParseDate(ParseError);
        SerdeJsonError(serde_json::Error);
    }
}

pub fn extend_logs() -> Result<()> {
    // Create output directory
    create_dir_all(OUTPUT_DIR)?;
 
    let mut journey_counter = 0;

    // Sort archive files
    let mut paths: Vec<_> = read_dir(ARCHIVE_DIR)
                                .unwrap()
                                .map(|r| r.unwrap())
                                .collect();
    paths.sort_by_key(|dir| dir.path());
    for entry in paths {
        let path = entry.path();
        println!("Log file: {}", path.to_str().unwrap());
        let logfile = File::open(path)?;
        let reader = BufReader::new(logfile);

        for line in reader.lines() {
            let line = line?;
            let json: serde_json::Value = serde_json::from_str(&line[..]).expect("JSON formatting error!");
            if &json.get("error") != &None {
                println!("Skipping invalid query:\n{}", &json);
                continue;
            }

            match json["querytype"].as_str().unwrap() {
                "connections" => {
                    match add_vehicle_data(&json) {
                        Ok(journey) => {
                            journey_counter = journey_counter + 1;
                            let path = format!("{}/{}.json", OUTPUT_DIR, journey_counter);
                            let mut file = File::create(&path)?;
                            let serialized = serde_json::to_string_pretty(&journey).unwrap();         
                            file.write_all(serialized.as_bytes()).expect("Writing journey to file failed!");
                            println!("Written journey {}", journey_counter);
                            splitter::split_routes(&path).unwrap();
                        }
                        _ => {
                            println!("Skipped query due to incomplete vehicle data:\n{}", &json);
                            continue;
                        }
                    }
                },
                _ => {},
            };
        }
    }
    Ok(())
}

fn add_vehicle_data(json: &serde_json::Value) -> Result<Journey> {
    let journeyoptions = &json["query"]["journeyoptions"];
    let mut id = 0;
    let mut routes: Vec<Route> = Vec::new();
    for option in journeyoptions.as_array() {
        for journey in option {
            let mut connections: Vec<Connection> = Vec::new();
            id = id + 1;
            for legs in journey["journeys"].as_array() {
                for leg in legs {
                    let vehicle = leg["trip"].as_str().unwrap();
                    let mut res = reqwest::get(&format!("{}{}", IRAIL_VEHICLE, vehicle)[..])?;
                    println!("URL: {}{} STATUS: {}", IRAIL_VEHICLE, vehicle, res.status());
                    if res.status() != 200 {
                        return Err("Incomplete vehicle data".into());
                    }
                    let res: serde_json::Value = res.json()?;

                    let mut start = false;
                    let mut previous_stop = String::new();
                    let mut previous_time = Utc::now();
                    for s in res["stops"]["stop"].as_array() {
                        for stop in s {

                            // Stop if we hit the transfer station or arrival station of this vehicle
                            if &stop["stationinfo"]["@id"].as_str().unwrap() == &leg["arrivalStop"].as_str().unwrap() {
                                println!("Found arrival station");
                                break;
                            }

                            // Start from departure station or transfer station of this vehicle
                            if &stop["stationinfo"]["@id"].as_str().unwrap() == &leg["departureStop"].as_str().unwrap() {
                                start = true;
                                println!("Found departure station");
                                previous_stop = String::from(stop["stationinfo"]["@id"].as_str().unwrap());
                                let departure_time = NaiveDateTime::from_timestamp(stop["scheduledDepartureTime"].as_str().unwrap().parse::<i64>()?, 0);
                                previous_time = DateTime::from_utc(departure_time, Utc);
                                continue;
                            }
                            
                            if start {
                                let arrival_time = NaiveDateTime::from_timestamp(stop["scheduledArrivalTime"].as_str().unwrap().parse::<i64>()?, 0);
                                let arrival_time: DateTime<Utc> = DateTime::from_utc(arrival_time, Utc);

                                &connections.push(Connection {
                                    departure_time: previous_time,
                                    arrival_time: arrival_time,
                                    departure_stop: previous_stop,
                                    arrival_stop: String::from(stop["stationinfo"]["@id"].as_str().unwrap()),
                                    vehicle: format!("{}/{}", VEHICLE_URI, res["vehicle"].as_str().unwrap().replace("BE.NMBS.", "")),
                                });       

                                // Departure stop next connection = current stop
                                previous_stop = String::from(stop["stationinfo"]["@id"].as_str().unwrap());
                                let departure_time = NaiveDateTime::from_timestamp(stop["scheduledDepartureTime"].as_str().unwrap().parse::<i64>()?, 0);
                                previous_time = DateTime::from_utc(departure_time, Utc);
                                continue;
                            }
                        }
                    }
                }
            }

            // Count transfers
            let mut number_of_transfers = 0;
            if connections.iter().count() > 0 {
                let mut vehicles: Vec<String> = Vec::new();
                for c in &connections {
                    if !vehicles.contains(&c.vehicle) {
                        vehicles.push(c.vehicle.clone());
                    }
                }
                number_of_transfers = (vehicles.iter().count() - 1) as u32; // 3 vehicles = 2 transfers
            }
            
            // Create route
            let route = Route {
                _connections: connections,
                _transfers: number_of_transfers,
            };
            routes.push(route);
        }
    }

    let query_time: DateTime<Utc> = DateTime::parse_from_rfc3339(&json["querytime"].as_str().unwrap()).unwrap().with_timezone(&Utc);
    let query = Query {
        query_time: query_time,
        query_type: String::from(json["querytype"].as_str().unwrap()),
        user_agent: String::from(json["user_agent"].as_str().unwrap()),
        arrival_stop: String::from(json["query"]["arrivalStop"]["@id"].as_str().unwrap()),
        departure_stop: String::from(json["query"]["departureStop"]["@id"].as_str().unwrap()),
    };

    let journey = Journey {
        query: query,
        routes: routes,
    };

    println!("Journey fully extracted!");
    Ok(journey)
}

#[derive(Debug, Serialize)]
pub struct Query {
    arrival_stop: String,
    departure_stop: String,
    #[serde(with = "dateformatter")]
    query_time: DateTime<Utc>,
    user_agent: String,
    query_type: String,
}

#[derive(Serialize, Debug)]
pub struct Journey {
    query: Query,
    routes: Vec<Route>,
}

#[derive(Serialize, Debug)]
pub struct Route {
    #[serde(rename = "connections")]
    _connections: Vec<Connection>,
    #[serde(rename = "transfers")]
    _transfers: u32,
}

#[derive(Serialize, Debug)]
struct Connection {
    #[serde(with = "dateformatter")]
    #[serde(rename = "departureTime")]
    departure_time: DateTime<Utc>,
    #[serde(with = "dateformatter")]
    #[serde(rename = "arrivalTime")]
    arrival_time: DateTime<Utc>,
    #[serde(rename = "arrivalStop")]
    arrival_stop: String,
    #[serde(rename = "departureStop")]
    departure_stop: String,
    #[serde(rename = "gtfs:vehicle")]
    vehicle: String,
}

