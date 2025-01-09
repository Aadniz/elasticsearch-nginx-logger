use anyhow::{bail, Error, Result};
use chrono::{NaiveDate, TimeZone, Utc};
use colored::Colorize;
use elasticsearch::auth::Credentials;
use elasticsearch::cert::CertificateValidation;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{BulkParts, CountParts, DeleteByQueryParts, Elasticsearch, SearchParts};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use regex::Regex;
use reqwest::Client;
use reqwest::{self, Url};
use serde_json::{json, Value};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fmt, io, thread, time};

use crate::cert::Cert;
use crate::logger::Logger;

/// Checks if the string is an URL with regex
pub fn is_url(str1: String) -> bool {
    let str = str1.as_str();
    let re = Regex::new(r#"(http|https)://([^/ :]+):?([^/ ]*)/?(/?[^ #?]*)\x3f?([^ #]*)#?([^ ]*)"#)
        .unwrap();
    return re.is_match(str);
}

/// Checks if the string is a valid JSON
pub fn is_json(str: &str) -> Result<()> {
    let _res: Value = serde_json::from_str(str)?;
    Ok(())
}

fn epoch_to_date(epoch: i64) -> NaiveDate {
    return Utc.timestamp(epoch, 0).date_naive();
}

/// Server, containing protocol, hostname, port and db
#[derive(Clone)]
pub struct Server {
    protocol: String,
    username: Option<String>,
    password: Option<String>,
    hostname: String,
    port: u16,
    index: String,
    client: Elasticsearch,
    pub cert: Option<Cert>,
}

impl Server {
    pub fn new(url: &str, cert_path: Option<PathBuf>) -> Self {
        let parsed_url = Url::parse(url).expect("Expected valid url");

        let protocol = parsed_url.scheme().to_string();
        let username = Some(parsed_url.username().to_string());
        let password = parsed_url.password().map(str::to_string);
        let hostname = parsed_url.host_str().unwrap().to_string();
        let port = parsed_url.port().unwrap_or(9200);
        let index = parsed_url
            .path()
            .to_string()
            .trim_start_matches("/")
            .to_string();

        let pool = SingleNodeConnectionPool::new(
            Url::parse(&format!("{}://{}:{}", protocol, hostname, port)).unwrap(),
        );

        let mut transport = TransportBuilder::new(pool);
        let mut cert = None;
        if let Some(path) = cert_path {
            if let Ok(c) = Cert::new(path) {
                cert = Some(c.clone());
                transport =
                    transport.cert_validation(CertificateValidation::Certificate(c.es_cert));
            }
        }

        if let (Some(u), Some(p)) = (username.clone(), password.clone()) {
            transport = transport.auth(Credentials::Basic(u.to_string(), p.to_string()));
        }

        let client = Elasticsearch::new(transport.build().unwrap());

        Server {
            protocol,
            username,
            password,
            hostname,
            port,
            index,
            client,
            cert,
        }
    }

    pub fn get_url_with_credentials(&self) -> String {
        if let (Some(u), Some(p)) = (&self.username, &self.password) {
            format!(
                "{}://{}:{}@{}:{}/{}",
                self.protocol, u, p, self.hostname, self.port, self.index
            )
        } else {
            self.get_url()
        }
    }

    pub fn get_url(&self) -> String {
        format!(
            "{}://{}:{}/{}",
            self.protocol, self.hostname, self.port, self.index
        )
    }
    pub fn get_host(&self) -> String {
        format!("{}://{}:{}", self.protocol, self.hostname, self.port)
    }

    pub async fn count_before(&self, epoch: i64) -> i64 {
        let search_response = self
            .client
            .count(CountParts::Index(&[self.index.as_str()]))
            .body(json!({
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "time": {
                                        "lt": epoch
                                    }
                                }
                            }
                        ]
                    }
                }
            }))
            .send()
            .await;

        if !search_response.is_ok() {
            println!("{}", "Failed to send count request".red());
            return -1;
        }

        let response = search_response.unwrap().json::<Value>().await;

        if !response.is_ok() {
            println!("{}", "Responded with a non-ok message!".red());
            return -1;
        }

        let response_body = response.unwrap();
        if response_body.get("count").is_none() {
            println!("{}", "\"count\" not in body response!".red());
            return -1;
        }

        return response_body.get("count").unwrap().as_i64().unwrap();
    }

    async fn delete_before(&self, epoch: i64) {
        let delete_query = self
            .client
            .delete_by_query(DeleteByQueryParts::Index(&[self.index.as_str()]))
            .body(json!({
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "time": {
                                        "lt": epoch
                                    }
                                }
                            }
                        ]
                    }
                }
            }))
            .send()
            .await;

        if !delete_query.is_ok() {
            println!("{}", "Failed to delete by query!".red());
            thread::sleep(time::Duration::from_secs(6));
            return;
        }

        let response = delete_query.unwrap().json::<Value>().await;

        if !response.is_ok() {
            println!(
                "{}",
                "Delete by query responded with a non-zero response!".red()
            );
            thread::sleep(time::Duration::from_secs(6));
            return;
        }

        let response_body = response.unwrap();
        println!("{:?}", response_body);
    }

    /// This function archives all documents before epoch time to an archive directory
    pub fn archive(&self, path: &Path, file_name: String, epoch: i64) -> Result<(), Error> {
        let file_name = format!("{}-{}.log.zz", file_name, epoch_to_date(epoch));
        let full_path = if let Some(p) = path.to_str() {
            format!("{}{}", p, file_name)
        } else {
            bail!("Failed to convert path: `{:?}` to a string", path)
        };

        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // Get the count of amount of documents to archive
                let total = self.count_before(epoch).await;
                let mut now: u64 = 0;
                let mut prev_now: u64 = 0;
                let mut last500: Vec<String> = vec![];
                // Just in case
                if 0 >= total {
                    return;
                }

                let mut e = ZlibEncoder::new(Vec::new(), Compression::best());
                print!("Running");

                // The main loop
                loop {
                    print!(".");
                    // if on the last few documents to archive
                    let mut last_run = false;
                    let search_response = self
                        .client
                        .search(SearchParts::Index(&[self.index.as_str()]))
                        .body(json!({
                            "from": 0,
                            "size": 500,
                            "query": {
                                "bool": {
                                    "must": [
                                        {
                                            "range": {
                                                "time": {
                                                    "lt": epoch,
                                                    "gte": now
                                                }
                                            }
                                        }
                                    ]
                                }
                            },
                            "sort": {
                                "time": {
                                    "order": "ASC"
                                }
                            }
                        }))
                        .send()
                        .await;

                    if !search_response.is_ok() {
                        println!("{}", "Failed to search archive".red());
                        thread::sleep(time::Duration::from_secs(6));
                        continue;
                    }

                    let response = search_response.unwrap().json::<Value>().await;

                    if !response.is_ok() {
                        println!(
                            "{}",
                            "Archive search responded with a non-zero response!".red()
                        );
                        thread::sleep(time::Duration::from_secs(6));
                        continue;
                    }

                    let response_body = response.unwrap();

                    let failed = response_body.get("error");
                    if !failed.is_none() {
                        println!("{}", "Archiving search had errors!".red());
                        println!("{:?}", response_body);
                        thread::sleep(time::Duration::from_secs(6));
                        continue;
                    }

                    let items = response_body["hits"]["hits"].as_array().unwrap();
                    if 500 > items.len() {
                        println!("Finishing off archiving last {} documents", items.len());
                        last_run = true;
                    }

                    // Loop through response
                    let mut last: Vec<String> = vec![];
                    for item in items {
                        if item.get("_source").is_none() {
                            println!("Dcument doesn't have _source ?");
                            continue;
                        }
                        if item.get("_id").is_none() {
                            println!("Dcument doesn't have _id ?");
                            continue;
                        }
                        if item["_source"].get("time").is_none() {
                            println!("Dcument doesn't have time ?");
                            continue;
                        }
                        now = item["_source"]["time"].as_u64().unwrap_or(0);
                        let id = String::from(item["_id"].as_str().unwrap_or("0"));
                        last.push(id.clone());
                        if last500.contains(&id) {
                            continue;
                        }

                        // Actually writing the line
                        let log = Logger::from_es(&item["_source"]).unwrap();
                        let line = format!("{}\n", log);
                        e.write_all(line.as_bytes()).unwrap();
                    }
                    last500 = last;

                    if last_run {
                        let compressed_bytes = e.finish();

                        let mut output = File::create(full_path.clone()).unwrap();
                        output.write_all(&compressed_bytes.unwrap()).unwrap();

                        println!("Saved archive: {}", full_path);
                        println!("Deleting {} documents...", total);
                        self.delete_before(epoch).await;
                        break;
                    }

                    // In case it loops through 500 documents, all with the same timestamp
                    if now == prev_now {
                        print!("+");
                        now += 1;
                    }
                    prev_now = now;
                }
            });

        Ok(())
    }

    pub async fn bulk(&self, log: Vec<Logger>) {
        let mut body: Vec<JsonBody<Value>> = vec![];

        let mut ids: Vec<String> = vec![];
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

        let response = self
            .client
            .bulk(BulkParts::Index(self.index.as_str()))
            .body(body)
            .request_timeout(Duration::from_secs(25))
            .send()
            .await;

        if !response.is_ok() {
            println!("{}", "Failed to create bulk".red());
            return;
        }

        let response = response.unwrap().json::<Value>().await;

        if !response.is_ok() {
            println!("{}", "Responded with a non-ok message!".red());
            return;
        }

        let response_body = response.unwrap();

        let successful = response_body["errors"].as_bool().unwrap_or(false) == false;
        if !successful {
            println!("{}", "Bulk had errors!".red());
        }

        let items = response_body["items"].as_array();
        if items.is_none() {
            println!("{}", "Indexed 0 documents??".red());
            return;
        }
        let mut counter = 0;
        for item in items.unwrap() {
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

    /// Checks if Elasticsearch database exists
    pub async fn db_exists(&self) -> Result<(), Error> {
        if self.index == "" {
            bail!("No index specified");
        }
        self.is_es().await?;
        let url = format!(
            "{}://{}:{}/{}",
            self.protocol, self.hostname, self.port, self.index
        );

        let mut client_builder = Client::builder().connect_timeout(Duration::from_secs(16));

        if let Some(cp) = &self.cert {
            client_builder = client_builder.add_root_certificate(cp.cert.clone())
        }

        let client = client_builder.build()?;

        let response = if let (Some(u), Some(p)) = (&self.username, &self.password) {
            client.get(url.as_str()).basic_auth(u, Some(p)).send().await
        } else {
            client.get(url.as_str()).send().await
        };
        let res = response.unwrap();
        if res.status() != reqwest::StatusCode::OK {
            println!(
                "  Found elasticsearch database, but index ({}) does not exist.",
                self.index
            );
            println!(
                "  Do you want to create {} at {}://{}:{} ?",
                self.index, self.protocol, self.hostname, self.port
            );
            print!("({}/{}/{}) > ", "y".green(), "n".red(), "q".yellow());
            let _ = io::stdout().flush();
            let mut user_input = String::new();
            let stdin = io::stdin();
            stdin.read_line(&mut user_input).expect("Expect input");
            user_input = String::from(user_input.trim());
            if user_input != "y" && user_input != "q" {
                // if n or something else
                bail!("Cancelled due to user input");
            } else if user_input == "q" {
                println!("Quitting...");
                std::process::exit(0);
            } else if user_input == "y" {
                Logger::create_mapping(self.clone()).await?;
                return Ok(());
            }
            bail!("Nothing happened");
        }
        Logger::valid_mapping(self.index.clone(), res).await?;
        Ok(())
    }

    /// Checks if the server is an elasticsearch server
    pub async fn is_es(&self) -> Result<(), Error> {
        let indexes = ["name", "cluster_name", "cluster_uuid", "version", "tagline"];

        let url = format!("{}://{}:{}", self.protocol, self.hostname, self.port);

        let mut client_builder = Client::builder().connect_timeout(Duration::from_secs(16));
        if let Some(cp) = &self.cert {
            client_builder = client_builder.add_root_certificate(cp.cert.clone());
        }

        let client = client_builder.build()?;

        let response = if let (Some(u), Some(p)) = (self.username.clone(), self.password.clone()) {
            client
                .get(url.as_str())
                .basic_auth(u, Some(p))
                .send()
                .await?
        } else {
            client.get(url.as_str()).send().await?
        };
        if response.status() != 200 {
            bail!("Returned non-200 response: {:?}", response.status())
        }
        let text = response.text().await;
        if is_json(text.as_ref().unwrap().as_str()).is_ok() == false {
            bail!("Response is not json");
        }
        let res: Value = serde_json::from_str(text.unwrap().as_str()).unwrap();
        let mut fails @ mut count = 0;
        for index in indexes {
            if res.get(index).is_none() {
                fails += 1;
            }
            count += 1;
        }
        let success_rate = (count - fails) as f64 / count as f64;
        if 0.75 > success_rate {
            bail!("This does not lookasd like an Elasticsearch DB",);
        }
        Ok(())
    }
}
impl fmt::Display for Server {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let hostname = self.get_url();
        write!(f, "{}", hostname)
    }
}
