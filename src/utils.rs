use std::{
    fs::{self, File},
    path::Path,
};

use anyhow::{bail, Error};
use chrono::{DateTime, Local, TimeZone, Utc};
use std::io::Write;

/// Remove extra slashes in path
/// From /home///chiya//something → /home/chiya/something/
pub fn beautify_path(path: String) -> String {
    let mut new_path: String = String::new();
    let mut is_slash = false;
    for (_, c) in path.chars().enumerate() {
        if c == '/' && is_slash {
            continue;
        } else if c == '/' {
            is_slash = true;
        } else {
            is_slash = false;
        }
        new_path.push(c);
    }
    if new_path.chars().last().unwrap() != '/' {
        new_path.push('/');
    }
    return new_path;
}

/// Checks if Nginx log has valid format
pub fn valid_archive(loc: &str) -> Result<(), Error> {
    let loc2 = beautify_path(loc.to_string());
    if Path::new(loc2.as_str()).exists() == false {
        bail!("The path does not exist");
    }

    if Path::new(loc2.as_str()).is_dir() == false {
        bail!("The path is not a directory");
    }

    // Check if write permissions in directory
    //let md = fs::metadata(loc).unwrap();
    //let permissions = md.permissions();
    //if permissions.readonly() {
    //    print!("The directory is not writable!");
    //    return false;
    //}
    // いつから。。。 https://stackoverflow.com/questions/74129865/how-to-check-if-a-directory-has-write-permissions-in-rust/74130122
    // Doing it the stupid way instead
    if !dir_write_permission(loc2) {
        bail!("Probably not write permission");
    }

    Ok(())
}

pub fn epoch_to_datetime(epoch: i64) -> String {
    let naive = Local.timestamp(epoch, 0).naive_local();
    let datetime = DateTime::<Utc>::from_local(naive, Utc);
    let newdate = datetime.format("%Y-%m-%d %H:%M:%S").to_string();
    return newdate;
}

pub fn dir_write_permission(path: String) -> bool {
    let file_path = format!("{}tmp.swp", path);

    // Try creating a file, and then deleting it right afterwards
    let file_res = File::create(file_path.clone());
    if !file_res.is_ok() {
        return false;
    }

    // Write a &str in the file (ignoring the result).
    let res = writeln!(&mut file_res.unwrap(), ":)");
    if !res.is_ok() {
        return false;
    }
    res.unwrap();

    fs::remove_file(file_path.clone()).expect(
        format!(
            "The program crashed, you need to go delete {} manually",
            file_path
        )
        .as_str(),
    );
    true
}
