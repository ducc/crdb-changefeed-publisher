# CockroachDB Changefeed Publisher
Reads changefeeds from CockroachDB then sends them to a message queue!

# Usage
```
crdb-changefeed-publisher 0.1.0
Joe Burnard <github.com/ducc>
Reads changefeeds from CockroachDB then sends them to a message queue.

USAGE:
    crdb-changefeed-publisher [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
        --cursor-frequency <cursor-frequency>
            How often cursors should be received from cockroachdb and saved in the cursor store (e.g. 10s).

        --cursor-store <cursor-store>            Where cursor values should be stored (cockroachdb).
        --queue <queue>                          The message queue to send row changes to (rabbitmq).
```

