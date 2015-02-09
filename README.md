# butteredscones [![Build Status](https://travis-ci.org/alindeman/butteredscones.svg?branch=master)](https://travis-ci.org/alindeman/butteredscones)

**butteredscones** watches log files on disk and forwards them to **logstash**
via the **lumberjack** protocol. It is similar to
[**logstash-forwarder**](https://github.com/elasticsearch/logstash-forwarder).

> I'm a lumberjack and I'm OK

> I sleep all night and I work all day.

> [...]

> On Wednesdays I go shoppin'

> And have **buttered scones** for tea.

## Configuration

Like **logstash-forwarder**, **butteredscones** is configured via a JSON file.

```json
{
  "state": "/var/lib/butteredscones/state.db",

  "network": {
    "servers": [
      {
        "addr": "192.168.0.1:5043",
        "name": "logstash.internal.example.com",
      }
    ],
    "certificate":  "/etc/butteredscones/forwarder.crt",
    "key":          "/etc/butteredscones/forwarder.key",
    "ca":           "/etc/butteredscones/ca.crt",
    "timeout":      15
  },

  "statistics": {
    "addr": "127.0.0.1:8088"
  },

  "files": [
    {
      "paths":  ["/var/log/messages", "/var/log/*.log"],
      "fields": {"type": "syslog"}
    }
  ]
}
```

**state** is where **butteredscones** keeps information about how far it has
read into each file. The directory where it lives must be writable by the
user that runs the **butteredscones** process.

**network/servers** can include one or more servers. If multiple servers are
present, **butteredscones** will send to all servers concurrently. Specifying
an **name** for a server is _optional_. If specified, the **addr** will be used
to connect, but the **name** will be used to verify the certificate. This
allows butteredscones to connect properly even if DNS is broken.

The SSL certificate presented by the remote logstash server must be signed by
the specified CA, if the `"ca"` option is specified. Otherwise,
**butteredscones** will not communicate with the remote server.

If given, **statistics/addr** specifies a socket address where an HTTP server
will listen. Statistics about what **butteredscones** is doing will be written
in JSON format. Use these statistics to debug problems or write automated
monitoring tools. For example: `curl -si http://localhost:8088`

**files** supports glob patterns. **butteredscones** will periodically check
for new files that match the glob pattern and tail them.

Currently, **butteredscones** does _not_ support log files that are truncated
or renamed. This is not a use case the original developers had. However, if it
interests you, pull requests are welcomed.

## Development & Packaging

To build the static binary, `butteredscones`:

```
script/build
```

To run the tests:

```
script/test
```

To package `butteredscones` into a debian package:

```
GOOS=linux GOARCH=amd64 VERSION=0.0.1 script/deb
```

## Future Work

* Support input from standard in
* Support files which are truncated in place
