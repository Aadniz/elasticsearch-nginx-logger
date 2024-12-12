use chrono::{Local, NaiveTime};
use logwatcher::{LogWatcher, LogWatcherAction};
use std::{env, sync::Arc, sync::Mutex, thread};

// headers
mod config;
mod logger;
pub mod server;
mod utils;

use crate::logger::Logger;
use config::Config;
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
const ARCHIVE_TIME: u16 = 30; // Days

fn main() {
    let args: Vec<String> = env::args().collect();

    let config = Config::new(args);

    if config.nginx_sources.len() > 1 {
        eprintln!("No support for multiple nginx sources yet");
        std::process::exit(1);
    }

    // And then for the actual logging
    let mut log_watcher = LogWatcher::register(config.nginx_sources[0].clone()).unwrap();
    let mut counter = 0;
    let mut log: Vec<Logger> = vec![];
    let run = Arc::new(Mutex::new(false));

    // Get time epoch since midnight 30 days ago
    let mut epoch = epoch_days_ago(ARCHIVE_TIME.into());

    log_watcher.watch(&mut move |line: String| {
        let logger = match Logger::new(line.clone()) {
            Ok(l) => l,
            Err(e) => {
                println!("{e}");
                println!("Failed? {}", line);
                return LogWatcherAction::None;
            }
        };

        log.push(logger);

        counter += 1;

        if counter >= BULK_SIZE {
            // Check if new day and archiving is not happening
            let run1 = Arc::clone(&run);
            let mut running = run1.lock().unwrap();
            if epoch != epoch_days_ago(ARCHIVE_TIME.into()) && !*running {
                if let Some(ap) = config.archive_folder.clone() {
                    epoch = epoch_days_ago(ARCHIVE_TIME.into());
                    *running = true;
                    let mut count = 0;
                    println!("Checking ARCHIVE_TIME");
                    tokio::runtime::Builder::new_multi_thread()
                        .enable_all()
                        .build()
                        .unwrap()
                        .block_on(async {
                            count = config.server.count_before(epoch).await;
                        });

                    if count > 0 {
                        println!("Documents to archive: {}", count);

                        // Setting up variables to be sent to thread
                        let run2 = Arc::clone(&run);
                        let server = config.server.clone();
                        let archive_file = config.archive_file_prefix.clone();
                        thread::spawn(move || {
                            let response = server.archive(&ap, archive_file.to_string(), epoch);
                            if let Err(r) = response {
                                eprintln!("WARNING: {}", r);
                            }
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
                    config.server.bulk(&log).await;
                });

            counter = 0;
            log.clear();
        }

        LogWatcherAction::None
    });
}
