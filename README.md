# CockroachDB Changefeed Publisher
Reads changefeeds from CockroachDB then sends them to a message queue!

## What is a changefeed?
..

## How does it work?
1. The cursor store is read to check if a cursor is already stored.
1. A changefeed is created with the `EXPERIMENTAL CHANGEFEED FOR {table}` expression with the following options:
    1. `frequency` - this is how often a new cursor should be returned from cockroachdb.
    1. `cursor` - this is the earliest time a change should be returned.
1. Response rows are iterated to find rows that describe a change and rows which have the latest resolved cursor.
1. If the row describes a change:
    1. The arguments are parsed into the `table`, `key` and `value`.
    1. These are wrapped into a new struct and encoded into JSON.
    1. The new JSON payload is sent to the message queue.
1. If the row is a new resolved cursor:
    1. The `value` argument is parsed as the others should be null.
    1. The `resolved` JSON field is pulled from the `value` response.
    1. This is inserted/updated in the cursor store.

## Install
...

## Usage
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

## Environment Variables
...

## Metrics
...

## License
Licensed under MIT. See the LICENSE file in the repository root for the full text.
This is a third party application and is not affiliated with CockroachDB. It is not a direct replacement for the Enterprise Changefeeds offering.
