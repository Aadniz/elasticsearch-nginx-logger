use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use colored::Colorize;
use serde_json;
use regex::Regex;
use reqwest::Response;
use chrono::{DateTime};
use sha1::{Sha1, Digest};
use std::net::{Ipv4Addr, Ipv6Addr};


use serde_derive::{Deserialize, Serialize};
use serde_json::{Value};
use crate::Server;


///
/// When will nested structs be supported
#[derive(Serialize, Deserialize)]
struct Mapping{
    mappings: Mappings
}
#[derive(Serialize, Deserialize)]
struct Mappings{
    dynamic: String,
    properties: Properties
}
#[derive(Serialize, Deserialize)]
struct Properties{
    ip: Ip,
    alt_ip: Ip,
    request: Text,
    refer: Text,
    status_code: Short,
    size: Integer,
    user_agent: Text,
    time: EpochS
}
#[derive(Serialize, Deserialize)]
struct Ip{
    r#type: String
}
#[derive(Serialize, Deserialize)]
struct Text{
    r#type: String,
    fields: TextFields
}
#[derive(Serialize, Deserialize)]
struct TextFields{
    keyword: Keyword
}
#[derive(Serialize, Deserialize)]
struct Keyword{
    r#type: String,
    ignore_above: u16
}
#[derive(Serialize, Deserialize)]
struct Short{
    r#type: String
}
#[derive(Serialize, Deserialize)]
struct Integer{
    r#type: String
}
#[derive(Serialize, Deserialize)]
struct EpochS {
    r#type: String,
    format: String
}
impl Mapping{
    pub fn new() -> Self {
        Mapping {
            mappings: Mappings {
                dynamic: "false".to_string(),
                properties: Properties {
                    ip: Ip {
                        r#type: "ip".to_string()
                    },
                    alt_ip: Ip {
                        r#type: "ip".to_string()
                    },
                    request: Text {
                        r#type: "text".to_string(),
                        fields: TextFields {
                            keyword: Keyword {
                                r#type: "keyword".to_string(),
                                ignore_above: 256
                            }
                        }
                    },
                    refer: Text {
                        r#type: "text".to_string(),
                        fields: TextFields {
                            keyword: Keyword {
                                r#type: "keyword".to_string(),
                                ignore_above: 256
                            }
                        }
                    },
                    status_code: Short {
                        r#type: "short".to_string()
                    },
                    size: Integer {
                        r#type: "integer".to_string()
                    },
                    user_agent: Text {
                        r#type: "text".to_string(),
                        fields: TextFields {
                            keyword: Keyword {
                                r#type: "keyword".to_string(),
                                ignore_above: 256
                            }
                        }
                    },
                    time: EpochS {
                        r#type: "date".to_string(),
                        format: "epoch_second".to_string()
                    }
                }
            }
        }
    }
}
///
///

/// This function expects a string like this
/// ```
/// 17/Sep/2022:23:39:19 +0200
/// ```
fn date_to_epoch(str : &str) -> u32{
    let datetime = DateTime::parse_from_str(str, "%d/%b/%Y:%H:%M:%S %z");
    if datetime.is_ok() == false {
        return 0;
    }

    datetime.unwrap().timestamp() as u32
}

/// Checks if Nginx log has valid format
pub fn valid_log(loc : &str) -> bool {
    if Path::new(loc).exists() == false {
        return false;
    }

    // Check if able to read file
    let res = File::options()
        .read(true)
        .write(false)
        .open(loc);

    if res.is_ok() == false {
        return false;
    }

    // Check the first 4 lines
    let file = File::open(loc).unwrap();
    let reader = BufReader::new(file);

    let mut counter = 0;
    let mut fails = 0;
    for line in reader.lines() {
        let result = Logger::new(line.unwrap().clone());
        if counter > 10 {
            break;
        }
        if result.is_none() {
            fails += 1;
        }
        counter += 1;
    }

    let mut error = false;
    let mut success_rate = 0.00;
    if counter == 0 {
        println!("  Found file, but it's empty: {}", loc);
        error = true;
    }else if 4 > counter {
        println!("  Found file, but it contains less than 4 lines: {}", loc);
        error = true;
    }else{
        success_rate = (counter - fails) as f64 / counter as f64;
    }
    if 0.75 > success_rate && success_rate != 0.00 {
        println!("  Format errors in this file: ~{}%", (success_rate*100.0).round());
        error = true;
    }

    if error {
        println!("  Do you still wish to continue without fully verifying ?");
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
        }
    }

    true
}

/// Server, containing protocol, hostname, port and db
#[derive(Serialize, Deserialize, Debug)]
pub struct Logger{
    ip : String,
    alt_ip : Option<String>,
    request : String,
    refer : Option<String>,
    status_code : u16,
    size: u32,
    user_agent: Option<String>,
    time: u32 // Who knows if this program lives to be 83 years old
}
impl Logger{
    pub fn new(line : String) -> Option<Self> {
        let re = Regex::new(r#"(.*) .* .* \[(.*)\] "(.*)" (\d+) (\d+) "(.*)" "(.*)""#).ok()?;
        if re.is_match(line.as_str()) == false {
            return None;
        }

        // 127.0.0.1, 84.213.100.23 - - [20/Jul/2022:22:12:47 +0200] "GET /index.html HTTP/1.1" 403    153    "https://google.com/q=test" "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0"
        // cap[1]                        cap[2]                       cap[3]                    cap[4] cap[5]  cap[6]                      cap[7]
        let cap = re.captures(line.as_str())?;

        // Getting ip(s)
        let mut ip = &cap[1];
        let mut alt_ip : Option<String> = None;
        if ip.contains(",") {
            let split: Vec<&str> = ip.split(",").collect();
            ip = split[0].trim();
            alt_ip = Some(String::from(split[1].trim()));
        }

        // verify ip addresses
        if !ip.parse::<Ipv4Addr>().is_ok() && !ip.parse::<Ipv6Addr>().is_ok(){
            println!("Not an ip :P");
            return None;
        }

        // Getting the date
        let time = date_to_epoch(&cap[2]);
        if time == 0 {
            return None;
        }

        let request = &cap[3];
        let status_code_res = &cap[4].parse::<u16>();
        if !status_code_res.is_ok() {
            return None;
        }
        let status_code = status_code_res.clone().unwrap();
        let size_res = &cap[5].parse::<u32>();
        if !size_res.is_ok() {
            return None;
        }
        let size = size_res.clone().unwrap();
        let mut refer : Option<String> = None;
        if &cap[6] != "-" {
            refer = Some(String::from(&cap[6]));
        }
        let mut user_agent : Option<String> = None;
        if &cap[7] != "-" {
            user_agent = Some(String::from(&cap[7]));
        }

        Some(Logger {
            ip: String::from(ip),
            alt_ip,
            request: String::from(request),
            refer,
            status_code,
            size,
            user_agent,
            time
        })
    }

    /// Use the dummy data for testing,
    /// use the new() function for actual new logging
    pub fn dummy_data() -> Self {
        Logger {
            ip: "127.0.0.1".to_string(),
            alt_ip: None,
            request: "".to_string(),
            refer: None,
            status_code: 200,
            size: 420,
            user_agent: None,
            time: 0
        }
    }

    /// This function is to check if the author of this application has matching mapping
    pub fn double_check_mapping() -> bool{
        let logger = Self::dummy_data();
        let mapping : Mapping = Mapping::new();
        let keys = serde_json::to_value(mapping.mappings.properties)
            .unwrap()
            .as_object()
            .unwrap()
            .clone();
        let keys2 = serde_json::to_value(logger)
            .unwrap()
            .as_object()
            .unwrap()
            .clone();
        for elm in keys.iter() {
            if keys2.contains_key(elm.0) == false {
                panic!("{} Does not exist in struct", elm.0)
            }
        }
        for elm in keys2.iter() {
            if keys.contains_key(elm.0) == false {
                panic!("{} Does not exist in mapping", elm.0)
            }
        }
        true
    }

    pub async fn valid_mapping(db: String, res : Response) -> bool{
        if Logger::double_check_mapping() == false {
            return false;
        }
        let j : Value = res.json().await.expect("Expected valid JSON");
        if j[db.clone()]["mappings"]["properties"].is_null() {
            return false;
        }
        if j[db.clone()]["mappings"]["properties"].as_object().is_some() == false {
            return false;
        }
        let keys = j[db]["mappings"]["properties"]
            .as_object()
            .unwrap()
            .keys();
        let mapping : Mapping = Mapping::new();
        let keys2 = serde_json::to_value(mapping.mappings.properties)
            .unwrap()
            .as_object()
            .unwrap()
            .clone();

        for elm in keys {
            if keys2.contains_key(elm) == false{
                print!(" Should not contain: {}", elm);
                return false;
            }
        }
        true
    }

    pub async fn create_mapping(server : Server) -> Option<bool> {
        if Logger::double_check_mapping() == false {
            return None;
        }
        let mapping : Mapping = Mapping::new();
        let request = reqwest::Client::new()
            .put(server.get_url())
            .json(&mapping)
            .send()
            .await.ok()?
            .text()
            .await.ok()?;

        let res : Value = serde_json::from_str(request.as_str()).unwrap();
        if res["acknowledged"].is_boolean() == false || res["acknowledged"].as_bool().unwrap() == false {
            print!("[X] {}", request);
            return None;
        }

        print!("[ ] Created: {}", server);
        Some(true)
    }

    /// This function will generate the id for the document
    /// It's sha1(epoch + ip)
    pub fn get_id(&self) -> String {
        let mut hasher = Sha1::new();
        let raw = format!("{}{}", self.time, self.ip);
        hasher.update(raw.into_bytes());
        format!("{:X}", hasher.finalize())
    }
}
impl Clone for Logger{
    fn clone(&self) -> Logger {
        let alt_ip : Option<String>;
        if self.alt_ip.is_none() {
            alt_ip = None;
        }else{
            alt_ip = Option::from(self.alt_ip.as_ref().unwrap().clone())
        }
        let refer : Option<String>;
        if self.refer.is_none() {
            refer = None;
        }else{
            refer = Option::from(self.refer.as_ref().unwrap().clone())
        }
        let user_agent : Option<String>;
        if self.user_agent.is_none() {
            user_agent = None;
        }else{
            user_agent = Option::from(self.user_agent.as_ref().unwrap().clone())
        }
        Logger {
            ip: self.ip.clone(),
            alt_ip,
            request: self.request.clone(),
            refer,
            status_code: self.status_code.clone(),
            size: self.size.clone(),
            user_agent,
            time: self.time.clone()
        }
    }
}