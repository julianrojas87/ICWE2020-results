extern crate reqwest;
extern crate flate2;
extern crate tar;

use flate2::read::GzDecoder;
use tar::Archive;
use std::fs::create_dir_all;

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
    }
}

const IRAIL_LOGS: &str = "https://gtfs.irail.be/logs/";
const ARCHIVE_DIR: &str = "./archive";
const LOG_NAME: &str = "irailapi-201911"; // November 2019
const LOG_EXTENSION: &str = ".log.tar.gz";
const HOLIDAY_WEEKENDS: [i32; 11] = [1, 2, 3, 9, 10, 11, 16, 17, 23, 24, 30];

pub fn fetch_logs() -> Result<()> {
    // Create archive download directory
    create_dir_all(ARCHIVE_DIR)?;

    // 30 days in November, :02 leading zero
    for day in 1..31 {
        let day = day as i32;
        // Skip weekends and holidays due to a different schedule
        if HOLIDAY_WEEKENDS.contains(&day) {
            println!("Skipped holiday: {:02} November 2019", &day);
            continue;
        }

        let logfile = &format!("{}{:02}{}", LOG_NAME, day, LOG_EXTENSION)[..];
        println!("Log file: {}", logfile);

        // Get archive
        let url = &format!("{}{}", IRAIL_LOGS, logfile)[..];
        let res = reqwest::get(url)?;
        println!("URL: {} STATUS: {}", url, res.status());

        // Untar it
        let tar = GzDecoder::new(res);
        let mut archive = Archive::new(tar);
        archive.unpack(ARCHIVE_DIR)?;
    }

    Ok(())
}
