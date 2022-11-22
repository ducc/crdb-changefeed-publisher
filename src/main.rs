#![feature(async_closure)]

use std::panic::panic_any;
use std::{net::SocketAddr, string::String, sync::Arc};

use clap::{load_yaml, App};
use futures_util::StreamExt;
use log::{info, warn};
use sqlx::{postgres::PgPool, prelude::*, PgConnection, Pool, Postgres};
use tracing::{debug, error};
use tracing_subscriber::{EnvFilter, FmtSubscriber};
use uuid::Uuid;

use crate::queues::{SQSQueue, StdoutDump};
use cursors::CrdbCursorStore;
use error::Error;
use metrics::run_warp;
use model::{
    Change, ChangeCursor, ChangePayload, ChangeRow, CursorStoreType, JsonCursor, ProcessedChange,
    QueueType,
};
use queues::RabbitMQ;

mod cursors;
mod error;
mod metrics;
mod model;
mod queue_buffer;
mod queues;

type MessageQueue = Arc<dyn queues::MessageQueue + Send + Sync>;
type CursorStore = Arc<dyn cursors::CursorStore + Send + Sync>;

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

    tracing_log::LogTracer::init()?;

    debug!("test");
    let yaml = load_yaml!("../cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    let table_value = matches.value_of("table").expect("unable to get table name");
    let queue_value = QueueType::from_name(matches.value_of("queue").unwrap_or("stdout"))
        .expect("unable to get queue type");
    let cursor_store_value =
        CursorStoreType::from_name(matches.value_of("cursor-store").unwrap_or("cockroachdb"))
            .expect("unable to get cursor store type");
    let cursor_frequency_value = matches.value_of("cursor-frequency").unwrap_or("10s");

    // get the environment variables
    let prom_addr_raw = std::env::var("PROMETHEUS_ADDR").unwrap_or_else(|_| "0.0.0.0:8001".into());
    let database_url = std::env::var("DATABASE_URL").expect("database url is required");
    let changefeed_id =
        std::env::var("CHANGEFEED_ID").unwrap_or_else(|_| Uuid::new_v4().to_string());

    info!("Preparting to start changefeed '{}'", changefeed_id);

    // start the prometheus server
    let prom_addr: SocketAddr = prom_addr_raw
        .parse()
        .expect("cannot parse prometheus address");
    let warp_handle = tokio::spawn(run_warp(prom_addr));

    let message_queue: MessageQueue = match queue_value {
        QueueType::Stdout => Arc::new(StdoutDump {}),
        QueueType::Sqs => {
            let queue_url = std::env::var("SQS_QUEUE_URL").expect("SQS queue url is required");
            Arc::new(SQSQueue::new(queue_url).await?)
        }
    };

    // connect to cockroachdb
    // PgPool::
    let pool = PgPool::connect(&database_url).await?;

    let cursor_store: CursorStore = match cursor_store_value {
        CursorStoreType::CockroachDB => {
            Arc::new(CrdbCursorStore::new(pool.clone(), changefeed_id.to_string()).await?)
        }
    };

    // begin processing cockroachdb changefeeds
    let cf_handle = tokio::spawn(process_changefeed(
        pool,
        message_queue,
        cursor_store,
        table_value.to_owned(),
        cursor_frequency_value.to_owned(),
    ));

    tokio::select! {
        cf_result = cf_handle => {
            if let Err(e) = cf_result {
                panic_any(e);
            }
        }

        warp_result = warp_handle => {
            if let Err(e) = warp_result {
                panic_any(e);
            }
        }
    };

    Ok(())
}

fn build_changefeed_query(
    table_name: String,
    cursor_frequency_value: String,
    cursor: Option<String>,
) -> String {
    let mut query = format!(
        "EXPERIMENTAL CHANGEFEED FOR {} WITH resolved = '{}', updated, mvcc_timestamp",
        table_name, cursor_frequency_value
    );

    if let Some(cursor) = cursor {
        query = format!("{}, cursor = '{}'", query, cursor);
    } else {
        query = format!("{}, no_initial_scan", query);
    }

    format!("{};", query)
}

async fn process_changefeed(
    pool: Pool<Postgres>,
    message_queue: MessageQueue,
    cursor_store: CursorStore,
    table_name: String,
    cursor_frequency_value: String,
) {
    let mut ignore_cursor = false;

    loop {
        let query = {
            let cursor_result = cursor_store.get();

            let cursor_opt = if ignore_cursor {
                None
            } else {
                match cursor_result.await {
                    Ok(result) => result,
                    Err(e) => {
                        error!("getting cursor result: {:?}", e);
                        return;
                    }
                }
            };

            build_changefeed_query(
                table_name.clone(),
                cursor_frequency_value.clone(),
                cursor_opt,
            )
        };

        debug!("query: {}", &query);

        let retry = match execute_changefeed(
            query.clone(),
            pool.clone(),
            message_queue.clone(),
            cursor_store.clone(),
        )
        .await
        {
            Ok(_) => {
                println!("Query exiting early for some reason");
                RetryReason::None
            }
            Err(e) => {
                error!("error executing changefeed: {:?}", &e);
                should_retry(e)
            }
        };

        match retry {
            RetryReason::InvalidCursor => {
                warn!("Existing changefeed cursor is outside the table gc window. Data loss is highly likely!");
                ignore_cursor = true;
            }
            RetryReason::None => {
                break;
            }
            _ => continue,
        }
    }
}

enum RetryReason {
    InvalidCursor,
    ServerDisconnect,
    None,
}

fn should_retry(e: Error) -> RetryReason {
    match e {
        Error::SqlxError(sqlx::Error::Database(dbe)) => match dbe.code() {
            Some(e) => match e.to_string().as_str() {
                "57014" => RetryReason::ServerDisconnect,
                "XXUUU" => match dbe {
                    _ if dbe.message().contains("must be after replica GC threshold") => {
                        RetryReason::InvalidCursor
                    }
                    _ => RetryReason::None,
                },
                _ => RetryReason::None,
            },
            _ => RetryReason::None,
        },
        Error::SqlxError(sqlx::Error::Io(ioe)) => match ioe.kind() {
            std::io::ErrorKind::UnexpectedEof => RetryReason::ServerDisconnect,
            std::io::ErrorKind::BrokenPipe => RetryReason::ServerDisconnect,
            _ => RetryReason::None,
        },
        _ => RetryReason::None,
    }
}

async fn get_node_for_connection(con: &mut PgConnection) -> Result<String, Error> {
    let result = sqlx::query("SHOW node_id;").fetch_one(con).await?;
    let node_id: Option<&str> = result.try_get(0)?;

    match node_id {
        Some(id) => Ok(id.into()),
        None => Err(Error::NotFound()),
    }
}

async fn execute_changefeed(
    query: String,
    pool: Pool<Postgres>,
    message_queue: MessageQueue,
    cursor_store: CursorStore,
) -> Result<(), Error> {
    let mut con = pool.acquire().await?;

    let node_id = get_node_for_connection(&mut *con).await?;
    info!("Starting changefeed against node: {}", node_id);

    let mut cursor = sqlx::query(&query).fetch(&mut *con);
    let mut buffer = queue_buffer::QueueBuffer::new(message_queue, 100);

    while let Some(row) = cursor.next().await {
        let value = row?;
        let table_opt: Option<&str> = value.try_get(0)?;
        let key_bytes: Option<Vec<u8>> = value.try_get(1)?;
        let value_bytes: Option<Vec<u8>> = value.try_get(2)?;

        let change = Change::new(table_opt, key_bytes, value_bytes);

        match process_change(change)? {
            ProcessedChange::Row(row) => {
                let payload = ChangePayload::new(row.table, row.key, row.value)?;
                buffer.push(payload).await?;
            }
            ProcessedChange::Cursor(cursor) => {
                let parsed: JsonCursor = serde_json::from_str(&cursor.cursor)?;
                debug!("cursor={}", &parsed.resolved);

                buffer.flush().await?;
                let cursor_handle = cursor_store.set(parsed.resolved);
                cursor_handle.await?;
            }
        }
    }

    Ok(())
}

fn process_change(change: Change) -> Result<ProcessedChange, Error> {
    let value = String::from_utf8(change.value.unwrap())?;

    if change.table.is_none() && change.key.is_none() {
        return Ok(ProcessedChange::Cursor(ChangeCursor::new(value)));
    }

    let table = change.table.unwrap().to_owned();
    let key = String::from_utf8(change.key.unwrap())?;

    Ok(ProcessedChange::Row(ChangeRow::new(table, key, value)))
}
