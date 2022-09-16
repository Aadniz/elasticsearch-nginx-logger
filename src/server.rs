use std::fmt;
use std::time::Duration;
use regex::Regex;
use reqwest;
use reqwest::Client;
use serde_json::{Result, Value};
use colored::Colorize;

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
    let response = client.get(url.as_str()).send().await;
    let text = response.unwrap().text().await;
    if is_json(text.as_ref().unwrap().as_str()).is_ok() == false {
        eprint!("{}", " (Response is not json)".red());
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
        print!("{}", " (No db set?)".red());
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
    if response.unwrap().status() != reqwest::StatusCode::OK {
        print!("{} {}{}", " (No db called".red(), ser.db.as_str().red(), ")".red());
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
    print!("{}", " (No connection to device)".red());
    false
}

/// Server, containing protocol, hostname, port and db
pub struct Server{
    protocol : String,
    hostname : String,
    port : u16,
    db : String
}
impl Server{
    pub fn new(str : &str) -> Self {
        let re = Regex::new(r#"(http|https)://([^/ :]+):?([^/ ]*)/?(/?[^ #?]*)\x3f?([^ #]*)#?([^ ]*)"#).unwrap();
        let cap = re.captures(str).expect("Expected valid url");

        Server {
            protocol: String::from(&cap[1]),
            hostname: String::from(&cap[2]),
            port: cap[3].parse::<u16>().unwrap_or(9200),
            db: String::from(&cap[4])
        }
    }

    pub fn get_url(&self) -> String {
        format!("{}://{}:{}/{}", self.protocol, self.hostname, self.port, self.db)
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