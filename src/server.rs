use std::{fmt, io};
use std::io::Write;
use std::time::Duration;
use regex::Regex;
use reqwest;
use reqwest::Client;
use serde_json::{json, Result, Value};
use colored::Colorize;
use elasticsearch::{BulkParts, Elasticsearch};
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::Transport;

use crate::logger::Logger;


/// Checks if the string is an URL with regex
pub fn is_url(str1 : String) -> bool{
    let str = str1.as_str();
    let re = Regex::new(r#"(http|https)://([^/ :]+):?([^/ ]*)/?(/?[^ #?]*)\x3f?([^ #]*)#?([^ ]*)"#).unwrap();
    return re.is_match(str);
}

/// Checks if the string is a valid JSON
pub fn is_json(str : &str) -> Result<()>{
    let _res: Value = serde_json::from_str(str)?;
    Ok(())
}

/// Checks if the server is an elasticsearch server
pub async fn is_es(ser : Server) -> bool{
    let indexes = ["name", "cluster_name", "cluster_uuid", "version", "tagline"];

    let url = format!("{}://{}:{}", ser.protocol, ser.hostname, ser.port);
    if is_up(url.clone()).await == false {
        return false;
    }
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(16))
        .build()
        .unwrap();
    let response = client
        .get(url.as_str())
        .send()
        .await;
    let text = response
        .unwrap()
        .text()
        .await;
    if is_json(text.as_ref().unwrap().as_str()).is_ok() == false {
        print!("{}", " (Response is not json)".red());
        return false;
    }
    let res: Value = serde_json::from_str(text.unwrap().as_str()).unwrap();
    let mut fails @ mut count = 0;
    for index in indexes {
        if res[index].is_null() {
            fails += 1;
        }
        count += 1;
    }
    let success_rate = (count - fails) as f64 / count as f64;
    if 0.75 > success_rate {
        print!("{}", " (This does not look like an Elasticsearch DB)".red());
        return false;
    }
    true
}

/// Checks if Elasticsearch database exists
pub async fn db_exists(ser : Server) -> bool {
    if ser.db == "" {
        print!("{}", " (No db specified)".red());
        return false;
    }
    if is_es(ser.clone()).await == false {
        return false;
    }
    let url = format!("{}://{}:{}/{}", ser.protocol, ser.hostname, ser.port, ser.db);
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(16))
        .build()
        .unwrap();
    let response = client.get(url.as_str()).send().await;
    let res = response.unwrap();
    if res.status() != reqwest::StatusCode::OK {
        println!();
        println!("  Found elasticsearch database, but DB ({}) does not exist.", ser.db);
        println!("  Do you want to create {} at {}://{}:{} ?", ser.db, ser.protocol, ser.hostname, ser.port);
        print!("({}/{}/{}) > ", "y".green(), "n".red(), "q".yellow());
        let _ = io::stdout().flush();
        let mut user_input = String::new();
        let stdin = io::stdin();
        stdin.read_line(&mut user_input).expect("Expect input");
        user_input = String::from(user_input.trim());
        if user_input != "y" && user_input != "q" { // if n or something else
            return false;
        } else if user_input == "q" {
            println!("Quitting...");
            std::process::exit(0);
        } else if user_input == "y" {
            if Logger::create_mapping(ser).await == None {
                return false;
            }
            return true;
        }
        return false;
    }
    if Logger::valid_mapping(ser.db.clone(), res).await == false {
        print!("{}", " (Elasticsearch was found, but it has the incorrect mapping)".yellow());
        return false;
    }
    true
}

/// Checks if host is reachable
pub async fn is_up(str1 : String) -> bool{
    if is_url(str1.clone()) == false{
        return false;
    }
    let client = Client::builder()
        .connect_timeout(Duration::from_secs(16))
        .build()
        .unwrap();
    let response = client.head(str1).send().await;
    if response.is_ok() {
        return true;
    }
    print!("{}", " (Port not open, or device is down)".red());
    false
}

/// Server, containing protocol, hostname, port and db
pub struct Server{
    protocol : String,
    hostname : String,
    port : u16,
    db : String,
    client: Elasticsearch
}
impl Server{
    pub fn new(str : &str) -> Self {
        let re = Regex::new(r#"(http|https)://([^/ :]+):?([^/ ]*)/?(/?[^ #?]*)\x3f?([^ #]*)#?([^ ]*)"#).unwrap();
        let cap = re.captures(str).expect("Expected valid url");

        let protocol = String::from(&cap[1]);
        let hostname = String::from(&cap[2]);
        let port = cap[3].parse::<u16>().unwrap_or(9200);
        let db = String::from(&cap[4]);

        let transport = Transport::single_node(format!("{}://{}:{}", protocol, hostname, port).as_str());
        let client = Elasticsearch::new(transport.unwrap());

        Server {
            protocol,
            hostname,
            port,
            db,
            client
        }
    }

    pub fn get_url(&self) -> String {
        format!("{}://{}:{}/{}", self.protocol, self.hostname, self.port, self.db)
    }
    pub fn get_host(&self) -> String {
        format!("{}://{}:{}", self.protocol, self.hostname, self.port)
    }

    pub async fn bulk(&self, log : Vec<Logger>) {
        let mut body: Vec<JsonBody<Value>> = vec![];

        let mut ids : Vec<String> = vec![];
        for elm in log {
            let id = elm.get_id();
            if !ids.contains(&id) {
                body.push(json!({"index": {"_id": id}}).into());
                body.push(json!(elm).into());
                ids.push(id);
            }
        }

        if body.is_empty() {
            println!("{}", "body is empty?".red());
            return;
        }

        let _response = self.client
            .bulk(BulkParts::Index(self.db.as_str()))
            .body(body)
            .send()
            .await;

        if !_response.is_ok(){
            println!("{}", "Failed to create bulk".red());
            return;
        }

        let response = _response.unwrap().json::<Value>().await;
        if !response.is_ok() {
            println!("{}", "Responded with a non-ok message!".red());
            return;
        }
        let response_body = response.unwrap();
        let successful = response_body["errors"].as_bool().unwrap_or(false) == false;
        if !successful {
            println!("{}", "Bulk had errors!".red());
        }

        let _items = response_body["items"].as_array();
        if _items.is_none() {
            println!("{}", "Indexed 0 documents??".red());
            return;
        }
        let mut counter = 0;
        for item in _items.unwrap() {
            if item.get("index").is_none() {
                continue;
            }
            if item["index"].get("result").is_none() {
                println!("{:?}", item);
                continue;
            }
            if item["index"]["result"].as_str().unwrap() != "created" {
                continue;
            }
            counter += 1;
        }
        if counter == 0 {
            println!("{}", "0 documents was indexed!".red());
            return;
        }
        println!("Successfully indexed {} documents", counter);
    }
}
impl fmt::Display for Server{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let hostname = self.get_url();
        write!(f, "{}", hostname)
    }
}
impl Clone for Server{
    fn clone(&self) -> Server {
        let url = self.get_url();
        let server : Server = Server::new(url.as_str());
        server
    }
}