use std::{env, thread};
use std::io::{empty, stdout, Write};
use std::path::Path;
use std::time::Duration;
use thread::sleep;

pub mod server; use server::Server;
use crate::server::is_url;


fn main() {
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
        else if server::is_url(arg){
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
    println!("Checking file location (✓: chosen, -: skip, X: Not found): ");
    for loc in &locations {
        print!("[ ] {} ...", loc);
        stdout().flush().unwrap();
        if !location.is_empty() && Path::new(loc).exists() {
            print!("\r[-]\n");
        }else if Path::new(loc).exists() {
            print!("\r[✓]\n");
            location = String::from(*loc);
        }else{
            print!("\r[X]\n");
        }
    }

    // Choosing a server
    let mut server : Option<Server> = None;
    println!("Checking file location (✓: chosen, -: skip, X: Not found): ");
    for ser in servers {
        print!("[ ] {} ...", ser);
        stdout().flush().unwrap();
        if server.is_some(){
            print!("\r[-]\n");
        }else if is_url(ser.get_hostname()) {
            print!("\r[✓]\n");
            server = Some(ser.clone());
        }else{
            print!("\r[X]\n");
        }
    }
}