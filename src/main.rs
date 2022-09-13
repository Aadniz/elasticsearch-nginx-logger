use std::env;
use std::path::Path;

pub mod server; use server::Server;

fn main() {
    let args: Vec<String> = env::args().collect();

    /// Possible default servers
    /// First priority from top to bottom
    let mut servers : Vec<Server> = vec![
        // "http://127.0.0.1:9200",
        // "http://192.168.1.137:9200"
    ];

    let mut server = Server::new();
    servers.push(server);

    /// Possible default locations
    /// First priority from top to bottom
    let mut locations : Vec<&str> = vec![
        "/var/log/nginx/access.log"
    ];

    // Iterate arguments, skip executable
    let mut new_locations: Vec<&str> = vec![];
    for arg in &args[1..]{
        if Path::new(arg).exists() {
            new_locations.push(arg);
        }
    }
    new_locations.reverse();
    locations.reverse();
    locations.extend(new_locations);
    locations.reverse();

    println!("Servers:");
    for server in servers{
        println!("{}", server);
    }
    println!("Locations:");
    for location in locations{
        println!("{}", location);
    }
}