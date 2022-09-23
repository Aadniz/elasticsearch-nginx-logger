# elasticsearch-nginx-logger

An application to actively monitor access.log and bulk them to elasticsearch

![screenshot](./screenshots/Screenshot_2022-09-18_09:12:55.png "Checking process")

### Basic usage
```bash
$ rust-logger [access.log file(s)] [database(s)] 
```
**Example**
```bash
$ rust-logger /var/log/nging/access.log http://127.0.0.1:9200/logger
```
It doesn't matter what order the arguments are provided. Just if it's a path, and a HTTP server address.

If none are provided, rust-logger will try some default paths and servers. These are:

`http://127.0.0.1:9200/logger` and `/var/log/nginx/access.log`

### Supported arguments:

---
`-b` | `--bulk` [number] :

Amount of requests before performing bulk request. The higher this number is, the more will be held in ram at once.

The less this number is, the more individual requests are done towards the DB.

Default is 2000

---

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
174.85.87.104, 127.0.0.1 - - [17/Sep/2022:18:07:59 +0200] "domain.org" "GET /browse/1/0/Date HTTP/1.1" 200 10981 "-" "Prowlarr/0.4.4.1947 (freebsd 13.1-release-p2)"
248.217.138.209 - - [17/Sep/2022:18:07:59 +0200] "domain.org" "POST /s/?search/Charmed/8/99/0 HTTP/1.1" 200 13137 "https://google.com/?q=charmed" "Mozilla/5.0 (Linux; Android 12; SM-P615) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36"
242.100.253.127, 127.0.0.1 - - [17/Sep/2022:18:07:59 +0200] "domain.org" "GET /index.php HTTP/1.1" 200 7535 "https://yandex.ru/?q=test" "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:104.0) Gecko/20100101 Firefox/104.0"
```
* **ip addresses:** The seperated ip addresses are the `ip` and `alt_ip`. `alt_ip` can be `None`
* **Date:** Next in the log, there is the time
* **Request:** The GET/POST/PUT request including its path
* **Host:** The sender's destination host
* **Status code:** Status code, 200, 404, 403 etc...
* **Bytes:** Size of the response
* **Refer:** Refer URL
* **User agent:** Lastly, it's the user agent

## NOTES

As of right now, there is no support for custom nginx logs. It only supports the default layout.
It would be desirable to be able to provide the mapping, and matching in some kind of config file.

Not skilled enough in rust to do this.