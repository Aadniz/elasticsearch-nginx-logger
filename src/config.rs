use std::{
    io::{stdout, Write},
    path::{Path, PathBuf},
};

use colored::Colorize;

use crate::{cert::Cert, logger::valid_log, server};
use crate::{
    server::Server,
    utils::{beautify_path, valid_archive},
};

const DEFAULT_SERVERS: [&str; 1] = ["http://127.0.0.1:9200/logger"];
const DEFAULT_LOCATIONS: [&str; 2] = ["/var/log/nginx/access.log", "/tmp/test.log"];
const DEFAULT_ARCHIVE_FILE_PREFIX: &str = "nginx";

pub struct Config {
    pub nginx_sources: Vec<PathBuf>,
    pub server: Server,
    pub archive_folder: Option<PathBuf>,
    pub archive_file_prefix: String,
}

impl Config {
    pub fn new(args: Vec<String>) -> Self {
        let mut locations = DEFAULT_LOCATIONS.to_vec();
        let mut servers = DEFAULT_SERVERS.to_vec();
        let mut archiving = vec![];
        let mut archive_file_prefix = DEFAULT_ARCHIVE_FILE_PREFIX.to_string();
        let mut cert_path: Option<PathBuf> = None;

        let mut new_locations: Vec<&str> = vec![];
        let mut new_servers: Vec<&str> = vec![];
        let mut new_archiving: Vec<&str> = vec![];

        // Iterate arguments, skip executable
        for arg in &args[1..] {
            if Path::new(arg).is_dir() {
                // specifying a directory sets it to the archiving directory
                new_archiving.push(arg);
            } else if Path::new(arg).exists() {
                if let Ok(_) = Cert::new(PathBuf::from(arg)) {
                    cert_path = Some(PathBuf::from(arg));
                } else {
                    // specifying a file sets the file we are reading from
                    new_locations.push(arg);
                }
            } else if server::is_url(String::from(arg)) {
                // specifying the url sets the elasticsearch url
                new_servers.push(arg);
            } else {
                archive_file_prefix = arg.to_string();
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
        let mut nginx_sources: Vec<PathBuf> = vec![];
        println!(
            "Checking file nginx_sources ({}: {}, {}: {}, {}: {}): ",
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
            if !nginx_sources.is_empty() && Path::new(loc).exists() {
                print!("{}", "\r[-]\n".yellow());
            } else if valid_log(loc) {
                print!("{}", "\r[✓]\n".green());
                nginx_sources.push(PathBuf::from(loc));
            } else {
                print!("{}", "\r[X]\n".red());
            }
        }
        if nginx_sources.is_empty() {
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
                    let ser = Server::new(ser, cert_path.clone());
                    print!("[ ] {} ...", ser);
                    stdout().flush().unwrap();
                    if server.is_some() {
                        print!("{}", " (Not bothering checking)".yellow());
                        print!("{}", "\r[-]\n".yellow());
                    } else if let Err(e) = ser.db_exists().await {
                        print!("{e}");
                        print!("{}", "\r[X]\n".red());
                    } else {
                        if ser.cert.is_some() {
                            if let Some(cp) = cert_path
                                .as_ref()
                                .and_then(|p| p.to_str())
                                .map(|s| s.to_string())
                            {
                                print!(" (cert: {})", cp);
                            }
                        }
                        print!("{}", "\r[✓]\n".green());
                        server = Some(ser.clone());
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
        let mut archive_folder: Option<PathBuf> = None;
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
            if archive_folder.is_some() && Path::new(loc).exists() {
                print!("{}", "\r[-]\n".yellow());
            } else if valid_archive(loc).is_ok() {
                let path = beautify_path(loc.to_string());
                archive_folder = Some(PathBuf::from(&path));
                print!("{}", "\r[✓]\n".green());
            } else {
                print!("{}", "\r[-]\n".yellow());
            }
        }

        if let Some(ap) = archive_folder.as_deref().clone().and_then(|ap| ap.to_str()) {
            println!();
            println!("Archive file prefix:");
            println!(
                "{} {} ({}{}-YYYY-MM-DD.log.zz)",
                "[✓]".green(),
                archive_file_prefix,
                ap,
                archive_file_prefix
            )
        } else {
            println!("{}", "No archiving directory found to log data to".yellow());
            println!("{}", "No archiving will be done".yellow());
        }
        println!();

        Self {
            nginx_sources,
            server,
            archive_folder,
            archive_file_prefix,
        }
    }
}
