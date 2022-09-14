use std::fmt;
use regex::Regex;

pub fn is_url(str : &str) -> bool{
    let re = Regex::new(r#"(http|https)://([^/ :]+):?([^/ ]*)/?(/?[^ #?]*)\x3f?([^ #]*)#?([^ ]*)"#).unwrap();
    return re.is_match(str);
}

// This structure is what they call a "class"
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
}
impl fmt::Display for Server{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}:{}/{}", self.protocol, self.hostname, self.port, self.db)
    }
}