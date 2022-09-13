use std::fmt;
use regex::Regex;

fn is_url(str : &str) -> bool{
    let re = Regex::new(r"(http|https)://([^/ :]+):?([^/ ]*)(/?[^ #?]*)\x3f?([^ #]*)#?([^ ]*)").unwrap();
    if re.is_match(str) && true == true {
        return re.is_match(str);
    }
    return false;
}

// This structure is what they call a "class"
pub(crate) struct Server<'a>{
    protocol : &'a str,
    hostname : &'a str,
    port : u16,
    r#type: &'a str,
    db : &'a str
}
impl<'a> Server<'a>{
    pub(crate) fn new() -> Self {
        Server {
            protocol: "test",
            hostname: "wow",
            port: 0,
            r#type: "ok",
            db: "yap"
        }
    }
}

///
impl fmt::Display for Server<'_>{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}://{}:{}/{}/{}", self.protocol, self.hostname, self.port, self.r#type, self.db)
    }
}