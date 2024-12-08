use chrono::{Local, NaiveTime};
use colored::Colorize;
use logwatcher::{LogWatcher, LogWatcherAction};
use std::io::{stdout, Write};
use std::path::Path;
use std::{env, sync::Arc, sync::Mutex, thread};

// headers
mod logger;
pub mod server;

use crate::logger::{beautify_path, valid_archive, valid_log, Logger};
use crate::server::*;
use server::Server;

fn epoch_days_ago(days: i64) -> i64 {
    let time = Local::now() + chrono::Duration::days(-days);
    let epoch = time
        .date()
        .and_time(NaiveTime::from_num_seconds_from_midnight(0, 0))
        .unwrap()
        .timestamp();
    return epoch;
}

// Default values
const BULK_SIZE: u16 = 500;
//const ARCHIVE_TIME: u16 = 30; // Days
const ARCHIVE_TIME: u16 = 30; // Days

fn main() {
    #[allow(non_snake_case)]
    let mut archive_enable = true;

    let args: Vec<String> = env::args().collect();

    // Possible default servers
    // First priority from top to bottom
    let mut servers: Vec<Server> = vec![Server::new("http://127.0.0.1:9200/logger")];

    // Possible default locations
    // First priority from top to bottom
    let mut locations: Vec<&str> = vec!["/var/log/nginx/access.log", "/tmp/test.log"];

    // Possible default archiving locations
    // First priority from top to bottom
    let mut archiving: Vec<&str> = vec!["/var/log/nginx", "/tmp", "/root"];

    // Possible default archiving locations
    // First priority from top to bottom
    let mut archive_file_prefix: &str = "nginx";

    // Iterate arguments, skip executable
    let mut new_locations: Vec<&str> = vec![];
    let mut new_servers: Vec<Server> = vec![];
    let mut new_archiving: Vec<&str> = vec![];
    for arg in &args[1..] {
        if Path::new(arg).is_dir() {
            // specifying a directory sets it to the archiving directory
            new_archiving.push(arg);
        } else if Path::new(arg).exists() {
            // specifying a file sets the file we are reading from
            new_locations.push(arg);
        } else if server::is_url(String::from(arg)) {
            // specifying the url sets the elasticsearch url
            new_servers.push(Server::new(arg));
        } else {
            archive_file_prefix = arg;
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

    new_archiving.reverse();
    archiving.reverse();
    archiving.extend(new_archiving);
    archiving.reverse();

    // Choosing a file path
    let mut location: String = String::from("");
    println!(
        "Checking file location ({}: {}, {}: {}, {}: {}): ",
        "✓".green(),
        "chosen".green(),
        "-".yellow(),
        "skip".yellow(),
        "X".red(),
        "Not found".red()
    );
    for loc in &locations {
        print!("[ ] {} ...", loc);
        stdout().flush().unwrap();
        if !location.is_empty() && Path::new(loc).exists() {
            print!("{}", "\r[-]\n".yellow());
        } else if valid_log(loc) {
            print!("{}", "\r[✓]\n".green());
            location = String::from(*loc);
        } else {
            print!("{}", "\r[X]\n".red());
        }
    }
    if location.is_empty() {
        println!("{}", "No log file found to log data from".red());
        std::process::exit(1);
    }
    println!();

    // Choosing a server
    let mut server: Option<Server> = None;
    println!(
        "Checking Servers ({}: {}, {}: {}, {}: {}): ",
        "✓".green(),
        "chosen".green(),
        "-".yellow(),
        "skip".yellow(),
        "X".red(),
        "Failed".red()
    );
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            for ser in servers {
                print!("[ ] {} ...", ser);
                stdout().flush().unwrap();
                if server.is_some() {
                    print!("{}", " (Not bothering checking)".yellow());
                    print!("{}", "\r[-]\n".yellow());
                } else if db_exists(ser.clone()).await {
                    print!("{}", "\r[✓]\n".green());
                    server = Some(ser.clone());
                } else {
                    print!("{}", "\r[X]\n".red());
                }
            }
            println!();
        });

    let server: Server = if let Some(s) = server {
        s
    } else {
        println!("{}", "No server found to log data to".red());
        std::process::exit(1);
    };

    // Choosing an archiving path
    let mut archive_path: String = String::from("");
    println!(
        "Checking archiving output directory ({}: {}, {}: {}, {}: {}): ",
        "✓".green(),
        "chosen".green(),
        "-".yellow(),
        "skip".yellow(),
        "X".red(),
        "Not found".red()
    );
    for loc in &archiving {
        print!("[ ] {} ...", loc);
        stdout().flush().unwrap();
        if !archive_path.is_empty() && Path::new(loc).exists() {
            print!("{}", "\r[-]\n".yellow());
        } else if valid_archive(loc) {
            print!("{}", "\r[✓]\n".green());
            archive_path = beautify_path(String::from(*loc));
        } else {
            print!("{}", "\r[-]\n".yellow());
        }
    }
    if archive_path.is_empty() {
        println!("{}", "No archiving directory found to log data to".yellow());
        println!("{}", "No archiving will be done".yellow());
        archive_enable = false;
    }
    println!();

    if archive_enable {
        println!("Archive file prefix:");
        println!(
            "{} {} ({}{}-YYYY-MM-DD.log.zz)",
            "[✓]".green(),
            archive_file_prefix,
            archive_path,
            archive_file_prefix
        )
    }
    println!();

    // And then for the actual logging
    let mut log_watcher = LogWatcher::register(location).unwrap();
    let mut counter = 0;
    let mut log: Vec<Logger> = vec![];
    let run = Arc::new(Mutex::new(false));

    // Get time epoch since midnight 30 days ago
    let mut epoch = epoch_days_ago(ARCHIVE_TIME.into());

    log_watcher.watch(&mut move |line: String| {
        let logger: Option<Logger> = Logger::new(line.clone());
        if logger.is_none() {
            println!("Failed? {}", line);
            return LogWatcherAction::None;
        }

        log.push(logger.unwrap());
        counter += 1;

        if counter >= BULK_SIZE {
            // Check if new day and archiving is not happening
            let run1 = Arc::clone(&run);
            let mut running = run1.lock().unwrap();
            if epoch != epoch_days_ago(ARCHIVE_TIME.into()) && !*running && archive_enable {
                epoch = epoch_days_ago(ARCHIVE_TIME.into());
                *running = true;
                let mut count = 0;
                println!("Checking ARCHIVE_TIME");
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async {
                        count = server.count_before(epoch).await;
                    });

                if count > 0 {
                    println!("Documents to archive: {}", count);

                    // Setting up variables to be sent to thread
                    let server = server.clone();
                    let run2 = Arc::clone(&run);
                    let archive_path = archive_path.clone();
                    let archive_file_name = archive_file_prefix.to_string();
                    thread::spawn(move || {
                        server.archive(archive_path, archive_file_name, epoch);
                        let mut running = run2.lock().unwrap();
                        *running = false;
                    });
                } else {
                    println!(
                        "Nothing to archive_path. No documents older than {} days.",
                        ARCHIVE_TIME
                    );
                    *running = false;
                }
            }
            //else {
            //    println!("Already running, can't do this now");
            //}

            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    // Send the bulk
                    server.bulk(&log).await;
                });

            counter = 0;
            log.clear();
        }

        LogWatcherAction::None
    });
}
