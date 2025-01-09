use chrono::{Local, NaiveTime};
use logwatcher::{LogWatcher, LogWatcherAction};
use std::{env, sync::Arc, sync::Mutex};

// headers
mod cert;
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

/// archive time in days
const ARCHIVE_TIME: u16 = 30;

fn main() {
    let args: Vec<String> = env::args().collect();
    let config_arc = Arc::new(Mutex::new(Config::new(args)));

    let log_watchers: Vec<LogWatcher> = {
        let config = config_arc.lock().unwrap();
        config
            .nginx_sources
            .iter()
            .filter_map(|s| LogWatcher::register(s).ok())
            .collect()
    };

    let log_arc: Arc<Mutex<Vec<Logger>>> = Arc::new(Mutex::new(vec![]));
    let mut epoch = epoch_days_ago(ARCHIVE_TIME.into());

    // Create a single Tokio runtime
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for mut lw in log_watchers {
            let log_arc = Arc::clone(&log_arc);
            let config_arc = Arc::clone(&config_arc);
            lw.watch(&mut move |line: String| {
                let log_arc = Arc::clone(&log_arc);
                let config_arc = Arc::clone(&config_arc);
                let logger = match Logger::from_line(&line) {
                    Ok(l) => l,
                    Err(e) => {
                        eprintln!("{e}");
                        eprintln!("Failed? {}", line);
                        return LogWatcherAction::None;
                    }
                };

                {
                    let mut log = log_arc.lock().unwrap();
                    log.push(logger);
                    let config = config_arc.lock().unwrap();
                    let server = config.server.clone();
                    let config = config.clone();
                    if log.len() as u32 >= config.bulk_size {
                        let log_clone = log.clone();
                        tokio::task::spawn(async move {
                            server.bulk(log_clone).await;
                        });
                        log.clear();

                        if epoch != epoch_days_ago(ARCHIVE_TIME.into()) {
                            epoch = epoch_days_ago(ARCHIVE_TIME.into());
                            tokio::task::spawn(async move {
                                archive(config.clone()).await;
                            });
                        }
                    }
                }

                LogWatcherAction::None
            });
        }
    });
}

async fn archive(config: Config) {
    if let Some(ap) = config.archive_folder.clone() {
        let epoch = epoch_days_ago(ARCHIVE_TIME.into());

        println!("Checking ARCHIVE_TIME");
        let count = config.server.count_before(epoch).await;

        if count > 0 {
            println!("Documents to archive: {}", count);

            // Setting up variables to be sent to thread
            let server = config.server.clone();
            let archive_file = config.archive_file_prefix.clone();

            let response = server.archive(&ap, archive_file.to_string(), epoch);
            if let Err(r) = response {
                eprintln!("WARNING: {}", r);
            }
        } else {
            println!(
                "Nothing to archive_path. No documents older than {} days.",
                ARCHIVE_TIME
            );
        }
    }
}
