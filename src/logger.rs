use serde_json;
use elasticsearch::{
    Elasticsearch, Error,
    http::transport::Transport
};
use reqwest::Response;

#[macro_use]
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


/// Server, containing protocol, hostname, port and db
#[derive(Serialize, Deserialize)]
pub struct Logger{
    ip : String,
    alt_ip : Option<String>,
    request : String,
    refer : String,
    status_code : u16,
    size: u32,
    user_agent: String,
    time: u32 // Who knows if this program lives to be 83 years old
}
impl Logger{
    pub fn new() -> Self {
        return Self::dummy_data();
    }

    /// Use the dummy data for testing,
    /// use the new() function for actual new logging
    pub fn dummy_data() -> Self {
        Logger {
            ip: "127.0.0.1".to_string(),
            alt_ip: None,
            request: "".to_string(),
            refer: "".to_string(),
            status_code: 200,
            size: 420,
            user_agent: "".to_string(),
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

    pub fn create_mapping(server : Server) {
        let mapping : Mapping = Mapping::new();
        let j = serde_json::to_string(&mapping).expect("Expected correct mapping");

        //print!("{}", j);
    }
}