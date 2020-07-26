#![feature(async_closure)]
#![feature(try_trait)]

mod error;

use error::Error;

use tokio_amqp::*;
use lapin::{
    Connection, 
    ConnectionProperties, 
    options::{
        QueueDeclareOptions,
        BasicPublishOptions,
    }, 
    types::FieldTable,
    Channel,
    BasicProperties,
};
use tracing::{trace, debug, info, error};
use tracing_subscriber::{FmtSubscriber, EnvFilter};
use std::{
    str::from_utf8,
    net::SocketAddr,
    convert::Infallible,
    vec::Vec,
    string::String,
};
use prometheus::{self, IntCounter, register_int_counter, TextEncoder, Encoder};
use lazy_static::lazy_static;
use warp::Filter;
use sqlx::{
    postgres::PgPool,
    prelude::*,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use async_trait::async_trait;

// initialize the prometheus metrics
lazy_static! {
    static ref MESSAGES_RECEIVED_COUNTER: IntCounter = register_int_counter!(
        "rabbitmq_messages_received", 
        "Number of messages received from RabbitMQ"
    ).unwrap();

    static ref MESSAGES_ACKED_COUNTER: IntCounter = register_int_counter!(
        "rabbitmq_messages_acked", 
        "Number of messages acked from RabbitMQ"
    ).unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // setup our logging and tracing
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("unable to set default trace subscriber");

    env_logger::init();

    // get the prometheus server address
    let prom_addr_raw = std::env::var("PROMETHEUS_ADDR").unwrap_or_else(|_| "0.0.0.0:8001".into());
    let prom_addr: SocketAddr = prom_addr_raw.parse().expect("cannot parse prometheus address");

    // start the prometheus server
    let warp_handle = tokio::spawn(run_warp(prom_addr));

    // get the rabbitmq address
    let mq_addr = std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672/%2f".into());
    info!(address = &mq_addr[..], "connecting to rabbitmq");

    // get the queue name
    let queue_name = std::env::var("AMQP_QUEUE").expect("queue name is required");

    // connect to rabbitmq
    let mq_conn = Connection::connect(&mq_addr, ConnectionProperties::default().with_tokio()).await?; 

    let mq_chan = mq_conn.create_channel().await?;

    let mq_declare_chan = mq_conn.create_channel().await?;
    let _queue = mq_declare_chan.queue_declare(
        &queue_name,
        QueueDeclareOptions::default(),
        FieldTable::default(),
    )
    .await?;

    let pool = PgPool::builder()
        .max_size(5)
        .build(&std::env::var("DATABASE_URL")?).await?;

    if let Err(e) = init_crdb_cursor_store(&pool).await {
        error!("init_crdb_cursor_store: {:?}", e);
        return Ok(());
    }

    //let cursor_store = Box::new(NoopCursorStore{});
    let cursor_store = Box::new(CrdbCursorStore::new(pool.clone()));

    let cf_handle = tokio::spawn(process_changefeed(pool, mq_chan, queue_name, cursor_store));

    // start our rabbitmq consumer and the prometheus exporter
    match tokio::try_join!(cf_handle, warp_handle) {
        Ok((cf_result, warp_result)) => {
            info!("cf result: {:?}", cf_result);
            info!("warp result: {:?}", warp_result);
        },
        Err(e) => {
            error!("err on join: {}", e);
        },
    }

    Ok(())
}

async fn run_warp(prom_addr: SocketAddr) -> Result<(), Error> {
    let exporter = warp::path!("metrics").and_then(serve_metrics);
    warp::serve(exporter).run(prom_addr).await;
    Ok(())
}

async fn serve_metrics() -> Result<impl warp::Reply, Infallible> {
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    let metric_families = prometheus::gather();
    encoder.encode(&metric_families, &mut buf).expect("encoding prometheus metrics as text");
    let text = from_utf8(&buf).expect("converting bytes to utf8");
    Ok(text.to_owned())
}

#[async_trait]
trait CursorStore {
    async fn get(&self) -> Result<Option<String>, Error>;
    async fn set(&self, cursor: String) -> Result<(), Error>;
}

struct NoopCursorStore;

#[async_trait]
impl CursorStore for NoopCursorStore {
    async fn get(&self) -> Result<Option<String>, Error> {
        Ok(None)
    }

    async fn set(&self, _cursor: String) -> Result<(), Error> {
        Ok(())
    }
}

struct CrdbCursorStore {
    pool: PgPool,
}

impl CrdbCursorStore {
    fn new(pool: PgPool) -> Self {
        Self {
            pool: pool,
        }
    }
}
 
#[async_trait]
impl CursorStore for CrdbCursorStore {
    async fn get(&self) -> Result<Option<String>, Error> {
        let query = sqlx::query("SELECT cursor FROM cursor_store WHERE key = 'key';");
        let mut fetched = query.fetch(&self.pool); 
        
        let row = match fetched.next().await {
            Ok(v) => v,
            Err(e) => return Err(Error::SqlxError(e)),
        };

        match row {
            Some(v) => {
                let s: String = v.get(0);
                Ok(Some(s))
            },
            None => Ok(None),
        }
    }

    async fn set(&self, cursor: String) -> Result<(), Error> {
        let result = sqlx::query(&format!("UPSERT INTO cursor_store (key, cursor) VALUES ('key', '{}');", cursor))
            .execute(&self.pool)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::SqlxError(e))
        }
    }
}

async fn init_crdb_cursor_store(pool: &PgPool) -> Result<(), Error> {
    let result = sqlx::query("CREATE TABLE IF NOT EXISTS cursor_store (key STRING NOT NULL PRIMARY KEY, cursor STRING NOT NULL);").execute(pool).await;
    match result {
        Ok(_) => Ok(()),
        Err(e) => Err(Error::SqlxError(e)),
    }
}

async fn process_changefeed(pool: PgPool, mq_chan: Channel, queue_name: String, cursor_store: Box<dyn CursorStore + Send>) -> Result<(), Error> {
    let query = {
        let cursor_result = cursor_store.get();

        let cursor_opt = match cursor_result.await {
            Ok(result) => result,
            Err(e) => {
                error!("getting cursor result: {:?}", e);
                return Ok(());
            }
        };

        if let Some(cursor) = cursor_opt {
            format!("EXPERIMENTAL CHANGEFEED FOR foo WITH resolved = '10s', cursor = '{}';", cursor)
        } else {
            "EXPERIMENTAL CHANGEFEED FOR foo WITH resolved = '10s';".to_owned()
        }
    };
    debug!("query: {}", &query);

    let mut cursor = sqlx::query(&query).fetch(&pool);

    while let Some(row) = cursor.next().await? {
        let table_opt: Option<&str> = row.try_get(0)?;
        let key_bytes: Option<Vec<u8>> = row.try_get(1)?;
        let value_bytes: Option<Vec<u8>> = row.try_get(2)?;

        let change = Change::new(table_opt, key_bytes, value_bytes);

        match process_change(change)? {
            ProcessedChange::Row(row) => {
                let payload = ChangePayload::new(row.table, row.key, row.value)?;
                let payload = serde_json::to_string(&payload)?;
                trace!("{}", &payload);

                let _ = mq_chan.basic_publish(
                    "",
                    &queue_name,
                    BasicPublishOptions::default(),
                    payload.into_bytes(),
                    BasicProperties::default(),
                ).await?.await?;
            },
            ProcessedChange::Cursor(cursor) => {
                let parsed: JsonCursor = serde_json::from_str(&cursor.cursor)?;
                trace!("cursor={}", &parsed.resolved);

                let cursor_set_handle = cursor_store.set(parsed.resolved);
                match cursor_set_handle.await {
                    Ok(_) => {},
                    Err(e) => error!("setting cursor handle: {:?}", e),
                };
            },
        }
    }

    Ok(())
}

struct Change<'a> {
    table: Option<&'a str>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

impl<'a> Change<'a> {
    fn new(table: Option<&'a str>, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self {
            table: table,
            key: key,
            value: value,
        }
    }
}

fn process_change(change: Change) -> Result<ProcessedChange, Error> {
    let value = String::from_utf8(change.value?)?;

    if change.table.is_none() && change.key.is_none() {
        return Ok(ProcessedChange::Cursor(ChangeCursor::new(value)));
    }

    let table = change.table?.to_owned();
    let key = String::from_utf8(change.key?)?;

    Ok(ProcessedChange::Row(ChangeRow::new(table, key, value)))
}

struct ChangeRow {
    table: String,
    key: String,
    value: String,
}

impl ChangeRow {
    fn new(table: String, key: String, value: String) -> Self {
        Self {
            table: table,
            key: key,
            value: value,
        }
    }
}

struct ChangeCursor {
    cursor: String,
}

impl ChangeCursor {
    fn new(cursor: String) -> Self {
        Self { cursor: cursor, }
    }
}

enum ProcessedChange {
    Row(ChangeRow),
    Cursor(ChangeCursor),
}

#[derive(Deserialize)]
struct JsonCursor {
    resolved: String,
} 

#[derive(Serialize)]
struct ChangePayload {
    table: String,
    key: String,
    value: Box<RawValue>,
}

impl ChangePayload {
    fn new(table: String, key: String, value: String) -> Result<Self, Error> {
        let value = RawValue::from_string(value)?;
        Ok(Self {
            table: table,
            key: key,
            value: value,
        })
    }
}
