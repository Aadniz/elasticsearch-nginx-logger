use chrono::{Local, NaiveTime};
use logwatcher::{LogWatcher, LogWatcherAction};
use std::{
    env,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

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
const ARCHIVE_AFTER_DAYS: u16 = 30;

fn main() {
    let args: Vec<String> = env::args().collect();
    let config = Arc::new(Config::new(args));

    let log_watchers: Vec<LogWatcher> = {
        config
            .nginx_sources
            .iter()
            .filter_map(|s| LogWatcher::register(s).ok())
            .collect()
    };

    let log_arc: Arc<Mutex<Vec<Logger>>> = Arc::new(Mutex::new(vec![]));

    let mut handles = vec![];

    // Archive thread
    if config.archive_folder.is_some() {
        let config = config.clone();
        let handle = thread::spawn(move || {
            // Creates Tokio runtime scope
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let mut epoch = epoch_days_ago(ARCHIVE_AFTER_DAYS.into());
                loop {
                    // Check if new day
                    if epoch != epoch_days_ago(ARCHIVE_AFTER_DAYS.into()) {
                        epoch = epoch_days_ago(ARCHIVE_AFTER_DAYS.into());
                        println!("Checking archive task");
                        archive(&config).await;
                    }
                    thread::sleep(Duration::from_secs(60));
                }
            });
        });

        handles.push(handle);
    }

    for mut lw in log_watchers {
        let log_arc = Arc::clone(&log_arc);
        let config = config.clone();
        let handle = thread::spawn(move || {
            // Creates Tokio runtime scope
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                lw.watch(&mut move |line: String| {
                    let log_arc = Arc::clone(&log_arc);
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
                        if log.len() as u32 >= config.bulk_size {
                            {
                                let log_clone = log.clone();
                                let config = config.clone();
                                tokio::task::spawn(async move {
                                    config.server.bulk(log_clone).await;
                                });
                                log.clear();
                            }
                        }
                    }

                    LogWatcherAction::None
                });
            });
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }
}

async fn archive(config: &Config) {
    if let Some(ap) = config.archive_folder.clone() {
        let count = config.server.count_before(ARCHIVE_AFTER_DAYS).await;

        if count > 0 {
            println!("Documents to archive: {}", count);

            let response = config
                .server
                .archive(&ap, &config.archive_file_prefix, ARCHIVE_AFTER_DAYS)
                .await;
            if let Err(r) = response {
                eprintln!("WARNING: {}", r);
            }
        } else {
            println!(
                "Nothing to archive. No documents older than {} days.",
                ARCHIVE_AFTER_DAYS
            );
        }
    }
}
