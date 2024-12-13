use anyhow::{bail, Context, Error, Result};
use chrono::{DateTime, Utc};
use colored::Colorize;
use regex::Regex;
use reqwest::Response;
use serde_json;
use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::{fmt, io};

use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use crate::utils::epoch_to_datetime;
use crate::Server;

///
/// When will nested structs be supported
#[derive(Serialize, Deserialize)]
struct Mapping {
    mappings: Mappings,
}
#[derive(Serialize, Deserialize)]
struct Mappings {
    dynamic: String,
    properties: Properties,
}
#[derive(Serialize, Deserialize)]
struct Properties {
    ip: Ip,
    alt_ip: Ip,
    host: Text,
    request: Text,
    refer: Text,
    status_code: Short,
    size: Integer,
    user_agent: Text,
    time: EpochS,
}
#[derive(Serialize, Deserialize)]
struct Ip {
    r#type: String,
}
#[derive(Serialize, Deserialize)]
struct Text {
    r#type: String,
    fields: TextFields,
}
#[derive(Serialize, Deserialize)]
struct TextFields {
    keyword: Keyword,
}
#[derive(Serialize, Deserialize)]
struct Keyword {
    r#type: String,
    ignore_above: u16,
}
#[derive(Serialize, Deserialize)]
struct Short {
    r#type: String,
}
#[derive(Serialize, Deserialize)]
struct Integer {
    r#type: String,
}
#[derive(Serialize, Deserialize)]
struct EpochS {
    r#type: String,
    format: String,
}
impl Mapping {
    pub fn new() -> Self {
        Mapping {
            mappings: Mappings {
                dynamic: "false".to_string(),
                properties: Properties {
                    ip: Ip {
                        r#type: "ip".to_string(),
                    },
                    alt_ip: Ip {
                        r#type: "ip".to_string(),
                    },
                    host: Text {
                        r#type: "text".to_string(),
                        fields: TextFields {
                            keyword: Keyword {
                                r#type: "keyword".to_string(),
                                ignore_above: 256,
                            },
                        },
                    },
                    request: Text {
                        r#type: "text".to_string(),
                        fields: TextFields {
                            keyword: Keyword {
                                r#type: "keyword".to_string(),
                                ignore_above: 256,
                            },
                        },
                    },
                    refer: Text {
                        r#type: "text".to_string(),
                        fields: TextFields {
                            keyword: Keyword {
                                r#type: "keyword".to_string(),
                                ignore_above: 256,
                            },
                        },
                    },
                    status_code: Short {
                        r#type: "short".to_string(),
                    },
                    size: Integer {
                        r#type: "integer".to_string(),
                    },
                    user_agent: Text {
                        r#type: "text".to_string(),
                        fields: TextFields {
                            keyword: Keyword {
                                r#type: "keyword".to_string(),
                                ignore_above: 256,
                            },
                        },
                    },
                    time: EpochS {
                        r#type: "date".to_string(),
                        format: "epoch_second".to_string(),
                    },
                },
            },
        }
    }
}

/// Checks if Nginx log has valid format
pub fn valid_log(loc: &str) -> bool {
    if Path::new(loc).exists() == false {
        return false;
    }

    if Path::new(loc).is_dir() {
        return false;
    }

    // Check if able to read file
    let res = File::options().read(true).write(false).open(loc);

    if res.is_ok() == false {
        print!("No read permission");
        return false;
    }

    // Check the first 4 lines
    let file = File::open(loc).unwrap();
    let reader = BufReader::new(file);

    let mut counter = 0;
    let mut fails = 0;
    for line in reader.lines() {
        if let Ok(l) = line {
            let result = Logger::new(l);
            if counter > 10 {
                break;
            }
            if result.is_err() {
                fails += 1;
            }
            counter += 1;
        }
    }

    let mut error = false;
    let mut success_rate = 0.00;
    if counter == 0 {
        println!("  Found file, but it's empty: {}", loc);
        error = true;
    } else if 4 > counter {
        println!("  Found file, but it contains less than 4 lines: {}", loc);
        error = true;
    } else {
        success_rate = (counter - fails) as f64 / counter as f64;
    }
    if 0.75 > success_rate && success_rate != 0.00 {
        println!(
            "  Format errors in this file: ~{}%",
            (success_rate * 100.0).round()
        );
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
        if user_input != "y" && user_input != "q" {
            // if n or something else
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
pub struct Logger {
    ip: IpAddr,
    alt_ip: Option<IpAddr>,
    host: Option<String>,
    request: String,
    refer: Option<String>,
    status_code: u16,
    size: u64,
    user_agent: Option<String>,
    time: u64, // Who knows if this program lives to be 83 years old
}
impl Logger {
    pub fn new(line: String) -> Result<Self, Error> {
        let re = Regex::new(r#"(.*) .* .* \[(.*)\] "(.*)" "(.*)" (\d+) (\d+) "(.*)" "(.*)""#)?;
        //if re.is_match(line.as_str()) == false {
        //    bail!("Regex did not match line");
        //}

        // 127.0.0.1, 84.213.100.23 - - [20/Jul/2022:22:12:47 +0200] "example.com" "GET /index.html HTTP/1.1" 403    153    "https://google.com/q=test" "Mozilla/5.0 (X11; Linux x86_64; rv:102.0) Gecko/20100101 Firefox/102.0"
        // cap[1]                        cap[2]                       cap[3]      cap[4]                    cap[5] cap[6]  cap[7]                      cap[8]
        let cap = re
            .captures(&line)
            .context("Regex did not get any captures")?;

        // Getting ip(s)
        let mut ip_arg = &cap[1];
        let mut alt_ip_arg: Option<String> = None;
        if ip_arg.contains(",") {
            let split: Vec<&str> = ip_arg.split(",").collect();
            ip_arg = split[0].trim();
            alt_ip_arg = Some(String::from(split[1].trim()));
        }

        // verify ip addresses
        let ip: IpAddr = ip_arg.parse::<IpAddr>()?;
        let alt_ip: Option<IpAddr> = alt_ip_arg.and_then(|ip| ip.parse::<IpAddr>().ok());

        // Getting the date
        // The format from nginx looks like this: 17/Sep/2022:23:39:19 +0200
        // It will fail if it isn't this format
        let time = DateTime::parse_from_str(&cap[2], "%d/%b/%Y:%H:%M:%S %z")?
            .with_timezone(&Utc)
            .timestamp() as u64;

        // Getting the domain
        let host = if &cap[3] != "-" {
            Some(cap[3].to_string())
        } else {
            None
        };

        let request = cap[4].to_string();
        let status_code = (&cap[5]).parse::<u16>()?;
        let size = (&cap[6]).parse::<u64>()?;
        let refer = if &cap[7] != "-" {
            Some(cap[7].to_string())
        } else {
            None
        };
        let user_agent = if &cap[8] != "-" {
            Some(cap[8].to_string())
        } else {
            None
        };

        Ok(Logger {
            ip,
            host,
            alt_ip,
            request,
            refer,
            status_code,
            size,
            user_agent,
            time,
        })
    }

    pub fn from_es(es: &Value) -> Option<Self> {
        // These values are required
        let ip: IpAddr = es.get("ip")?.as_str()?.parse().ok()?;
        let request = es.get("request")?.as_str()?.to_string();
        let status_code = es.get("status_code")?.as_u64()? as u16;
        let time = es.get("time")?.as_u64()?;
        let size = es.get("size")?.as_u64()?;

        // Option field for alt_ip
        let alt_ip: Option<IpAddr> = es
            .get("alt_ip")
            .and_then(|ai_j| ai_j.as_str())
            .and_then(|ai_str| ai_str.parse().ok());

        // Option field for host
        let host = es.get("host").and_then(|s| Some(s.to_string()));

        // Option field for refer
        let refer = es.get("refer").and_then(|s| Some(s.to_string()));

        // Option field for user agent
        let user_agent = es.get("user_agent").and_then(|s| Some(s.to_string()));

        Some(Logger {
            ip,
            alt_ip,
            host,
            request,
            refer,
            status_code,
            size,
            user_agent,
            time,
        })
    }

    /// Use the dummy data for testing,
    /// use the new() function for actual new logging
    pub fn dummy_data() -> Self {
        Logger {
            ip: IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            alt_ip: None,
            host: None,
            request: String::new(),
            refer: None,
            status_code: 200,
            size: 420,
            user_agent: None,
            time: 0,
        }
    }

    /// This function is to check if the author of this application has matching mapping
    pub fn double_check_mapping() -> Result<(), Error> {
        let logger = Self::dummy_data();
        let mapping: Mapping = Mapping::new();
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
                bail!("{} Does not exist in struct", elm.0)
            }
        }

        for elm in keys2.iter() {
            if keys.contains_key(elm.0) == false {
                bail!("{} Does not exist in mapping", elm.0)
            }
        }
        Ok(())
    }

    pub async fn valid_mapping(db: String, res: Response) -> anyhow::Result<(), anyhow::Error> {
        Logger::double_check_mapping()?;

        let j: Value = res.json().await?;
        let keys = j
            .get(db.clone())
            .and_then(|db_json| db_json.get("mappings"))
            .and_then(|mappings_json| mappings_json.get("properties"))
            .and_then(|prop_json| prop_json.as_object())
            .with_context(|| format!("Unable to find {}.mappings.properties", db))?;
        let mapping: Mapping = Mapping::new();
        let keys2 = serde_json::to_value(mapping.mappings.properties)
            .unwrap()
            .as_object()
            .unwrap()
            .clone();

        for elm in keys.keys() {
            if keys2.contains_key(elm) == false {
                bail!("Should not contain: {}", elm);
            }
        }
        for elm in keys2.keys() {
            if keys.contains_key(elm) == false {
                bail!("DB does not contain: {}", elm);
            }
        }
        Ok(())
    }

    pub async fn create_mapping(server: Server) -> Result<(), Error> {
        Logger::double_check_mapping()?;
        let mapping: Mapping = Mapping::new();
        let request = reqwest::Client::new()
            .put(server.get_url())
            .json(&mapping)
            .send()
            .await?
            .text()
            .await?;

        let res: Value = serde_json::from_str(request.as_str()).unwrap();
        if res["acknowledged"].is_boolean() == false
            || res["acknowledged"].as_bool().unwrap() == false
        {
            bail!(request);
        }

        Ok(())
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
impl Clone for Logger {
    fn clone(&self) -> Logger {
        Logger {
            ip: self.ip.clone(),
            alt_ip: self.alt_ip.clone(),
            host: self.host.clone(),
            request: self.request.clone(),
            refer: self.refer.clone(),
            status_code: self.status_code.clone(),
            size: self.size.clone(),
            user_agent: self.user_agent.clone(),
            time: self.time.clone(),
        }
    }
}

impl fmt::Display for Logger {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let ip = &self.ip;

        let alt_ip = if let Some(ai) = self.alt_ip {
            ai.to_string()
        } else {
            "None".to_string()
        };

        let host = if let Some(h) = &self.host {
            h.to_string()
        } else {
            "None".to_string()
        };

        let size = self.size;
        let status_code = self.status_code;
        let request = &self.request;
        let refer = if let Some(r) = &self.host {
            r.to_string()
        } else {
            "None".to_string()
        };
        let user_agent = if let Some(ua) = &self.user_agent {
            ua.to_string()
        } else {
            "None".to_string()
        };
        let time = epoch_to_datetime(self.time as i64);

        let line = format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            time, ip, alt_ip, host, status_code, request, refer, user_agent, size
        );
        write!(f, "{}", line)
    }
}
