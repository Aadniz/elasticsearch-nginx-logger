use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use anyhow::{bail, Error};

pub struct Cert {
    pub cert: reqwest::Certificate,
    pub es_cert: elasticsearch::cert::Certificate,
    pem_data: Vec<u8>,
}

impl Clone for Cert {
    fn clone(&self) -> Self {
        let es_cert = elasticsearch::cert::Certificate::from_pem(&self.pem_data).unwrap();
        Self {
            cert: self.cert.clone(),
            // ??
            //es_cert: self.es_cert.clone(),
            es_cert,
            pem_data: self.pem_data.clone(),
        }
    }
}
impl Cert {
    pub fn new(t: PathBuf) -> Result<Self, Error> {
        let pem_data = path_to_cert_data(t)?;
        let cert = reqwest::Certificate::from_pem(&pem_data)?;
        let es_cert = elasticsearch::cert::Certificate::from_pem(&pem_data)?;

        return Ok(Cert {
            cert,
            es_cert,
            pem_data,
        });
    }
}

pub fn path_to_cert_data(path: PathBuf) -> Result<Vec<u8>, Error> {
    // Old method
    // let mut buf = Vec::new();
    // File::open(path).ok()?.read_to_end(&mut buf).ok()?;
    // let cert = reqwest::Certificate::from_pem(&buf).ok()?;
    // Some(cert)

    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut found_cert = false;
    let mut cert_data = Vec::new();

    for line in reader.lines() {
        let line = line?.trim().to_string();
        if line.starts_with("-----BEGIN CERTIFICATE-----") {
            found_cert = true;
        }
        if !line.is_empty() && !found_cert {
            // Exit early preventing reading of HUGE files
            bail!("Malformed cert file");
        }
        if found_cert {
            cert_data.extend_from_slice(line.as_bytes());
            cert_data.push(b'\n');
            if line.starts_with("-----END CERTIFICATE-----") {
                break;
            }
        }
    }

    if found_cert {
        Ok(cert_data)
    } else {
        bail!("No certificate found in file")
    }
}
