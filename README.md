*My first project in rust ever*

# elasticsearch-nginx-logger

An application to actively monitor access.log and bulk them to elasticsearch

![screenshot](./screenshots/Screenshot_2022-09-18_09:12:55.png "Checking process")

### Basic usage
```shell
$ rust-logger [access.log file(s)] [database(s)] [archive directory] [archive filename] [certificate file] [bulk size]
```
**Example**
```shell
$ rust-logger /var/log/nginx/access.log http://127.0.0.1:9200/logger /var/log/archives archive 200
```
It doesn't matter what order the arguments are provided. If the argument is a path to a file, it will interperate that as the log file to read. If the argument is a folder, it will become the archive directory. If the argument is an URL, it will be the database url. If it's none of these, it will become the archive prefix name, lastly, if the argument is a number, it will specify the bulk size.

If none are provided, rust-logger will try some default paths and servers. These are:

`http://127.0.0.1:9200/logger` and `/var/log/nginx/access.log`

And archive to default location `/var/log/nginx/nginx-YYYY-MM-DD.log.zz`

Default bulk size is `500`, it means it needs to read 500 lines before it stores it to the elasticsearch database.

**Auth**

With the newer elasticsearch versions, it's become a standard to use TLS, and basic auth. Below is an example on how to provide the nessesary arguments for that:

```shell
$ rust-logger \
    /var/log/nginx/access.log \
    https://elastic:myPassword@127.0.0.1:9200/logger \
    /var/log/archives \
    archive \
    /etc/elasticsearch/certs/http_ca.crt \
    300
```
Here both `access.log` and `http_ca.crt` are files, but are able to differentiate them based of which one is a valid certificate file or not (eg. checking for `-----BEGIN CERTIFICATE-----`)

---

### Elasticsearch mapping

*I'm hoping to change this to a more dynamic approach in the future. Like with a config file or something, read the [notes](#notes) for more info*

The default mapping for elasticsearch is this:

```json
{
  "mappings": {
    "dynamic": "false",
    "properties": {
      "ip": {
        "type": "ip"
      },
      "alt_ip": {
        "type": "ip"
      },
      "host": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "request": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "refer": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "status_code": {
        "type": "short"
      },
      "size": {
        "type": "integer"
      },
      "user_agent": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "ignore_above": 256
          }
        }
      },
      "time": {
        "type": "date",
        "format": "epoch_second"
      }
    }
  }
}
```

### Nginx structure

*I wish to change this to a more dynamic approach in the future!*

The default structure rust-logger looks for is something like this:

**nginx.conf**
```
log_format combined_realip '$http_x_forwarded_for - $remote_user [$time_local] '
                           '"$host" "$request" $status $body_bytes_sent '
                           '"$http_referer" "$http_user_agent"';

access_log /var/log/nginx/access.log combined_realip;
```

**access.log**
```
192.85.89.104, 127.0.0.1 - - [17/Sep/2022:18:07:59 +0200] "domain.org" "GET /browse/1/0/Date HTTP/1.1" 200 10981 "-" "Mozilla/5.0 (Linux; Android 12; SM-P615) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
172.217.138.229 - - [17/Sep/2022:18:07:59 +0200] "domain.org" "POST /s/?search/Charmed/8/99/0 HTTP/1.1" 200 13137 "https://google.com/?q=charmed" "Mozilla/5.0 (Linux; Android 12; SM-P615) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
180.100.254.227, 127.0.0.1 - - [17/Sep/2022:18:07:59 +0200] "domain.org" "GET /index.php HTTP/1.1" 200 7535 "https://yandex.ru/?q=test" "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0"
```
* **ip addresses:** The seperated ip addresses are the `ip` and `alt_ip`. `alt_ip` can be `None`
* **Date:** Next in the log, there is the time
* **Request:** The GET/POST/PUT request including its path
* **Host:** The sender's destination host
* **Status code:** Status code, 200, 404, 403 etc...
* **Bytes:** Size of the response
* **Refer:** Refer URL
* **User agent:** Lastly, it's the user agent

## TODO
### Arguments

`-c` | `--count` [number] :

How many days before it moves documents to physical disk. Useful in case there are millions of requests within days.
Set `-c 0` to never move to disk, default is 30 days.

When saving to disk, it will only keep a log of unique requests within that day.

---

`--zip` :

If you want to save to disk as a compressed zip, default is `true`

`--raw` :

Saves to disk in raw text, default is `false`

---

`-d` | `--delete` [number] :

Days for when to completely delete the log. If this is shorter than `-c`, it won't save to disk at all, but just delete from DB.

---

`-y` | `--yes` :

Continue without asking for confirmation. You would need to provide this if you're planning to run this application with systemd for example.

## NOTES

As of right now, there is no support for custom nginx logs. It only supports the default layout.
It would be desirable to be able to provide the mapping, and matching in some kind of config file.

Not skilled enough in rust to do this.
