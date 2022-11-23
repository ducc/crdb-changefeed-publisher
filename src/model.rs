use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::Error;

pub enum QueueType {
    Stdout,
    Sqs,
}

impl QueueType {
    pub fn from_name(name: &str) -> Option<QueueType> {
        match name {
            "stdout" => Some(QueueType::Stdout),
            "sqs" => Some(QueueType::Sqs),
            _ => None,
        }
    }
}

pub enum CursorStoreType {
    CockroachDB,
}

impl CursorStoreType {
    pub fn from_name(name: &str) -> Option<CursorStoreType> {
        match name {
            "cockroachdb" => Some(CursorStoreType::CockroachDB),
            _ => None,
        }
    }
}

pub struct ChangeRow {
    pub table: String,
    pub key: String,
    pub value: String,
}

impl ChangeRow {
    pub fn new(table: String, key: String, value: String) -> Self {
        Self { table, key, value }
    }
}

pub struct ChangeCursor {
    pub cursor: String,
}

impl ChangeCursor {
    pub fn new(cursor: String) -> Self {
        Self { cursor }
    }
}

pub enum ProcessedChange {
    Row(ChangeRow),
    Cursor(ChangeCursor),
}

#[derive(Deserialize)]
pub struct JsonCursor {
    pub resolved: String,
}

#[derive(Serialize, Clone)]
pub struct ChangePayload {
    pub table: String,
    pub key: Box<RawValue>,
    pub value: Box<RawValue>,
}

impl ChangePayload {
    pub fn new(table: String, key: String, value: String) -> Result<Self, Error> {
        let value = RawValue::from_string(value)?;
        let key = RawValue::from_string(key)?;
        Ok(Self { key, value, table })
    }
}

pub struct Change<'a> {
    pub table: Option<&'a str>,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

impl<'a> Change<'a> {
    pub fn new(table: Option<&'a str>, key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> Self {
        Self { table, key, value }
    }
}
