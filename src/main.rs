use std::{env};
use std::io::{stdout, Write};
use std::path::Path;
use colored::Colorize;
use logwatcher::{LogWatcher, LogWatcherAction};

// headers
pub mod server;
mod logger;

use server::Server;
use crate::logger::{Logger, valid_log};
use crate::server::*;

#[tokio::main]
async fn main() {

    #[allow(non_snake_case)]
    // Default values
    let BULK_SIZE = 500;

    let args: Vec<String> = env::args().collect();

    // Possible default servers
    // First priority from top to bottom
    let mut servers : Vec<Server> = vec![
        Server::new("http://127.0.0.1:9200/logger"),
        Server::new("http://192.168.1.137:9200/logger")
    ];

    // Possible default locations
    // First priority from top to bottom
    let mut locations : Vec<&str> = vec![
        "/var/log/nginx/access.log",
        "/mnt/incognito/log/nginx/access.log"
    ];

    // Iterate arguments, skip executable
    let mut new_locations: Vec<&str> = vec![];
    let mut new_servers: Vec<Server> = vec![];
    for arg in &args[1..]{
        if Path::new(arg).exists() {
            new_locations.push(arg);
        }
        else if server::is_url(String::from(arg)){
            new_servers.push(Server::new(arg));
        }
    }

    new_locations.reverse();
    locations.reverse();
    locations.extend(new_locations);
    locations.reverse();

    new_servers.reverse();
    servers.reverse();
    servers.extend(new_servers);
    servers.reverse();


    // Choosing a file path
    let mut location : String = String::from("");
    println!("Checking file location ({}: {}, {}: {}, {}: {}): ", "✓".green(), "chosen".green(), "-".yellow(), "skip".yellow(), "X".red(), "Not found".red());
    for loc in &locations {
        print!("[ ] {} ...", loc);
        stdout().flush().unwrap();
        if !location.is_empty() && Path::new(loc).exists() {
            print!("{}", "\r[-]\n".yellow());
        }else if valid_log(loc) {
            print!("{}", "\r[✓]\n".green());
            location = String::from(*loc);
        }else{
            print!("{}", "\r[X]\n".red());
        }
    }
    if location.is_empty() {
        println!("{}", "No log file found to log data from".red());
        std::process::exit(1);
    }
    println!();

    // Choosing a server
    let mut _server : Option<Server> = None;
    println!("Checking Servers ({}: {}, {}: {}, {}: {}): ", "✓".green(), "chosen".green(), "-".yellow(), "skip".yellow(), "X".red(), "Failed".red());
    for ser in servers {
        print!("[ ] {} ...", ser);
        stdout().flush().unwrap();
        if _server.is_some(){
            print!("{}", " (Not bothering checking)".yellow());
            print!("{}", "\r[-]\n".yellow());
        }else if db_exists(ser.clone()).await {
            print!("{}", "\r[✓]\n".green());
            _server = Some(ser.clone());
        }else{
            print!("{}", "\r[X]\n".red());
        }
    }
    println!();

    if _server.is_some() == false{
        println!("{}", "No server found to log data to".red());
        std::process::exit(1);
    }

    let server = _server.unwrap();


    // And then for the actual logging
    let mut log_watcher = LogWatcher::register(location).unwrap();
    let mut counter = 0;
    let mut log : Vec<Logger> = vec![];

    log_watcher.watch(&mut move |line: String| {
        let logger : Option<Logger> = Logger::new(line.clone());
        if logger.is_none() {
            println!("Failed? {}", line);
            return LogWatcherAction::None;
        }

        log.push(logger.unwrap());
        counter += 1;

        // Send the bulk
        if counter >= BULK_SIZE {
            futures::executor::block_on(async {
                server.bulk(log.clone()).await;
            });

            counter = 0;
            log.clear();
        }

        LogWatcherAction::None
    });
}