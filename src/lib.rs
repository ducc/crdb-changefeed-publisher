#![feature(async_closure)]

use std::borrow::Borrow;
use std::panic::panic_any;
use std::{net::SocketAddr, string::String, sync::Arc};

use futures_util::StreamExt;
use lazy_static::lazy_static;
use log::{info, warn};
use sqlx::error::DatabaseError;
use sqlx::{postgres::PgPool, prelude::*, PgConnection, Pool, Postgres};
use tracing::{debug, error};
use tracing_subscriber::fmt::format;

use cursors::CrdbCursorStore;
use error::Error;
use metrics::run_warp;
use model::{Change, ChangeCursor, ChangePayload, ChangeRow, JsonCursor, ProcessedChange};

mod cursors;
pub mod error;
mod metrics;
pub mod model;
mod queue_buffer;
pub mod queues;

pub type MessageQueue = Arc<dyn queues::MessageQueue + Send + Sync>;
pub type CursorStore = Arc<dyn cursors::CursorStore + Send + Sync>;

pub async fn startup(
    changefeed_id: String,
    database_url: String,
    table_value: String,
    prom_addr: SocketAddr,
    message_queue: MessageQueue,
    cursor_frequency_value: String,
) -> Result<(), Error> {
    let warp_handle = tokio::spawn(run_warp(prom_addr));

    // connect to cockroachdb
    // PgPool::
    let pool = PgPool::connect(&database_url).await?;

    let cursor_store: CursorStore =
        Arc::new(CrdbCursorStore::new(pool.clone(), changefeed_id.to_string()).await?);

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
    }

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
        query = format!("{}, initial_scan", query);
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
    let mut cursor_override: Option<String> = None;

    loop {
        let query = {
            let cursor_result = cursor_store.get();

            let cursor_opt = if ignore_cursor {
                None
            } else if let Some(cur) = cursor_override.as_ref() {
                Some(cur.clone())
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
            RetryReason::InvalidCursor(new_cur) => {
                warn!("Existing changefeed cursor is outside the table gc window. Data loss is highly likely!");
                match new_cur {
                    Some(new_cur) => {
                        // Add one nanosecond to the cursor to ensure we pass the threshold
                        let new_cur = new_cur + 1;

                        warn!("Using new cursor: {}", new_cur);
                        cursor_override = Some(new_cur.to_string());
                    }
                    None => {
                        warn!("No new cursor provided. Ignoring cursor and starting from scratch");
                        ignore_cursor = true;
                        cursor_override = None;
                    }
                }
            }
            RetryReason::None => {
                break;
            }
            _ => continue,
        }
    }
}

enum RetryReason {
    InvalidCursor(Option<u64>),
    ServerDisconnect,
    None,
}

fn extract_replica_threshold(err: &dyn DatabaseError) -> Option<u64> {
    lazy_static! {
        static ref ER: regex::Regex =
            regex::Regex::new(r"must be after replica GC threshold ([0-9\.]*?),").unwrap();
    }
    let msg = err.message();

    let caps = ER.captures(msg)?;

    if let Some(cap) = caps.get(1) {
        let cap = cap.as_str().split_once('.')?;
        let num_str = format!("{}{}", cap.0, cap.1);
        let num = num_str.parse::<u64>().ok()?;
        return Some(num);
    }

    None
}

fn should_retry(e: Error) -> RetryReason {
    match e {
        Error::SqlxError(sqlx::Error::Database(dbe)) => match dbe.code() {
            Some(e) => match e.to_string().as_str() {
                "57014" => RetryReason::ServerDisconnect,
                "XXUUU" => match dbe {
                    _ if dbe.message().contains("must be after replica GC threshold") => {
                        RetryReason::InvalidCursor(extract_replica_threshold(dbe.borrow()))
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

    let node_id = get_node_for_connection(&mut con).await?;
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
