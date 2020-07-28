#![feature(async_closure)]
#![feature(try_trait)]

mod error;
mod model;
mod metrics;
mod cursors;
mod queues;

use error::Error;
use model::{ChangeRow, ChangeCursor, ProcessedChange, JsonCursor, ChangePayload, Change, QueueType, CursorStoreType};
use metrics::{RABBITMQ_MESSAGES_SENT_COUNTER, run_warp};
use cursors::{init_crdb_cursor_store, CursorStore, CrdbCursorStore};
use queues::{MessageQueue, RabbitMQ, init_rabbitmq_channel};

use tokio_amqp::*;
use lapin::{
    Connection, 
    ConnectionProperties,
};
use tracing::{trace, debug, error};
use tracing_subscriber::{FmtSubscriber, EnvFilter};
use std::{
    string::String, 
    net::SocketAddr,
};
use sqlx::{
    postgres::PgPool,
    prelude::*,
};
use clap::{load_yaml, App};

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

    let yaml = load_yaml!("../cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let table_value = matches.value_of("table").expect("unable to get table name");
    let queue_value = QueueType::from_name(matches.value_of("queue").unwrap_or("rabbitmq")).expect("unable to get queue type");
    let cursor_store_value = CursorStoreType::from_name(matches.value_of("cursor-store").unwrap_or("cockroachdb")).expect("unable to get cursor store type");
    let cursor_frequency_value = matches.value_of("cursor-frequency").unwrap_or("10s");

    // get the environment variables
    let prom_addr_raw = std::env::var("PROMETHEUS_ADDR").unwrap_or_else(|_| "0.0.0.0:8001".into());
    let database_url = std::env::var("DATABASE_URL").expect("database url is required");

    // start the prometheus server
    let prom_addr: SocketAddr = prom_addr_raw.parse().expect("cannot parse prometheus address");
    let warp_handle = tokio::spawn(run_warp(prom_addr));

    let message_queue: Box<dyn MessageQueue + Send> = match queue_value {
        QueueType::RabbitMQ => {
            let mq_addr = std::env::var("AMQP_ADDR").expect("amqp addr is required");
            let queue_name = std::env::var("AMQP_QUEUE").expect("queue name is required");
            let mq_conn = Connection::connect(&mq_addr, ConnectionProperties::default().with_tokio()).await?; 
            let mq_chan = mq_conn.create_channel().await?;
            let message_queue = Box::new(RabbitMQ::new(mq_chan, queue_name));
            let _ = init_rabbitmq_channel(&message_queue).await?;
            message_queue
        },
    };

    // connect to cockroachdb
    let pool = PgPool::builder()
        .max_size(5)
        .build(&database_url).await?;

    let cursor_store: Box<dyn CursorStore + Send> = match cursor_store_value {
        CursorStoreType::CockroachDB => {
            let _ = init_crdb_cursor_store(&pool).await?;
            Box::new(CrdbCursorStore::new(pool.clone()))
        },
    };

    // begin processing cockroachdb changefeeds
    let cf_handle = tokio::spawn(process_changefeed(pool, message_queue, cursor_store, table_value.to_owned(), cursor_frequency_value.to_owned()));

    tokio::select! {
        cf_result = cf_handle => {
            if let Err(e) = cf_result {
                panic!(e);
            }
        }

        warp_result = warp_handle => {
            if let Err(e) = warp_result {
                panic!(e);
            }
        }
    };

    Ok(())
}

fn build_changefeed_query(table_name: String, cursor_frequency_value: String, cursor: Option<String>) -> String {
    let mut query = format!("EXPERIMENTAL CHANGEFEED FOR {} WITH resolved = '{}'", table_name, cursor_frequency_value);
    
    if let Some(cursor) = cursor {
        query = format!("{}, cursor = '{}'", query, cursor);
    }

    format!("{}", query)
} 

async fn process_changefeed(pool: PgPool, message_queue: Box<dyn MessageQueue + Send>, cursor_store: Box<dyn CursorStore + Send>, table_name: String, cursor_frequency_value: String) {
    let query = {
        let cursor_result = cursor_store.get();

        let cursor_opt = match cursor_result.await {
            Ok(result) => result,
            Err(e) => {
                error!("getting cursor result: {:?}", e);
                return;
            }
        };

        build_changefeed_query(table_name, cursor_frequency_value, cursor_opt)
    };
    
    debug!("query: {}", &query);


    match execute_changefeed(query, pool, message_queue, cursor_store).await {
        Ok(_) => {},
        Err(e) => {
            error!("error executing changefeed: {:?}", e);
        },
    };
}

async fn execute_changefeed(query: String, pool: PgPool, message_queue: Box<dyn MessageQueue + Send>, cursor_store: Box<dyn CursorStore + Send>) -> Result<(), Error> {
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
                trace!("change={}", &payload);

                let publish_handle = message_queue.publish(payload.into_bytes());
                publish_handle.await?;
            },
            ProcessedChange::Cursor(cursor) => {
                let parsed: JsonCursor = serde_json::from_str(&cursor.cursor)?;
                trace!("cursor={}", &parsed.resolved);

                let cursor_handle = cursor_store.set(parsed.resolved);
                cursor_handle.await?;
            },
        }
    }

    Ok(())
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

