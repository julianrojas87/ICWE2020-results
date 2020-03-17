#[macro_use] 
extern crate error_chain;

use irail_query_extractor::*;

error_chain! {
    foreign_links {
        Io(std::io::Error);
        HttpRequest(reqwest::Error);
        ParseInt(::std::num::ParseIntError);
    }
}

fn main() -> Result<()> {
    println!("*** Fetching iRail.be logs ***");
    fetch_logs().unwrap();

    println!("*** Adding route information to logs and splitting based on parameters ***");
    extend_logs().unwrap();

    Ok(())
}
