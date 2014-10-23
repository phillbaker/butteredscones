# buttered-scones

**buttered-scones** watches log files on disk and forwards them to **logstash**
via the **lumberjack** protocol. It is similar to **logstash-forwarder**.

## Configuration

Like **logstash-forwarder**, **buttered-scones** is configured via a JSON file.

```json
{
  "state": "/var/lib/buttered-scones/state.db",

  "network": {
    "server":       "logstash.internal.example.com:5043",
    "certificate":  "/etc/buttered-scones/forwarder.crt",
    "key":          "/etc/buttered-scones/forwarder.key",
    "ca":           "/etc/buttered-scones/ca.crt",
    "timeout":      15
  },

  "files": [
    {
      "paths":  ["/var/log/messages", "/var/log/*.log"],
      "fields": {"type": "syslog"}
    }
  ]
}
```

**state** is where **buttered-scones** keeps information about how far it has
read into each file. The directory where it lives must be writable by the
user that runs the **buttered-scones** process.

The SSL certificate presented by the remote logstash server must be signed
by the specified CA. Otherwise, **buttered-scones** will not communicate with
the remote server.

**files** supports glob patterns.

## Future Work

* Support multiple servers
* Support input from standard in
* Support files which are truncated in place
