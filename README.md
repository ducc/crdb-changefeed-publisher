# CockroachDB Changefeed Publisher
Reads row-level changes from CockroachDB then sends them to a message queue!

## What is a changefeed?
This app utilises [Core changefeeds](https://www.cockroachlabs.com/docs/stable/change-data-capture.html) "which stream row-level changes to the client indefinitely until the underlying connection is closed or the changefeed is canceled".

## Supported messages queues
- RabbitMQ

## Supported cursor stores
- CockroachDB

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

## Example published message
SQL:
```sql
CREATE TABLE foo (a INT PRIMARY KEY, b int);
INSERT INTO foo VALUES (54);
```

Published message:
```json
{"table":"foo","key":"[54]","value":{"after": {"a": 54, "b": null}}}
```

## Install
Tested on Rust version `1.46.0-nightly (346aec9b0 2020-07-11)`.

### Docker
`docker run duccos/crdb-changefeed-publisher:latest`

### Kubernetes
Example deployments can be found in the [.kube directory](https://github.com/ducc/crdb-changefeed-publisher/tree/master/.kube).

### From source
1. `git clone git@github.com:ducc/crdb-changefeed-publisher.git`
1. `cd crdb-changefeed-publisher`
1. `cargo build --release`
1. `{environment vars} target/release/crdb-changefeed-publisher`

## Usage
### Command line arguments
| Argument           | Help                                     | Options             | Default     |
| ------------------ | ---------------------------------------- | ------------------- | ----------- |
| --help             | Shows available arguments                |                     |             |
| --version          | Shows the running version                |                     |             |
| --table            | Name of the table to watch changes in    | Table name e.g. foo |             |
| --queue            | The message queue to send row changes to | rabbitmq            | rabbitmq    |
| --cursor-store     | Where cursor values should be stored     | cockroachdb         | cockroachdb |
| --cursor-frequency | How often cursors should be received     | Duration e.g. 10s   | 10s         |

### Environment variables
| Variable        | Help                            | Options                                 | Default      |
| --------------- | ------------------------------- | --------------------------------------- | ------------ |
| RUST_LOG        | Logging level                   | trace, debug, info, error               | info         |
| PROMETHEUS_ADDR | Adress of the promethues server | ip:port                                 | 0.0.0.0:8001 |
| DATABASE_URL    | URL of the cockroachdb server   | postgresql://user:pass@ip:port/database |              |
| AMQP_ADDR       | RabbitMQ server address         | amqp://ip:port                          |              |
| AMQP_QUEUE      | RabbitMQ queue topic name       | e.g. changes                            |              |

## Metrics
Prometheus metrics are exposed on the PROMETHEUS_ADDR env var address at /metrics.
| Metric                 | Type    |
| ---------------------- | ------- |
| rabbitmq_messages_sent | Counter |

## License
Licensed under MIT. See the LICENSE file in the repository root for the full text.
This is a third party application and is not affiliated with CockroachDB. It is not a direct replacement for the Enterprise Changefeeds offering.

## Contributions
Feel free to submit a merge request! Your changes MUST be submitted under the MIT license.
