use std::string::String;

use async_trait::async_trait;
use futures_util::StreamExt;
use log::error;
use sqlx::postgres::PgPool;
use sqlx::Row;

use crate::Error;

#[async_trait]
pub trait CursorStore {
    async fn get(&self) -> Result<Option<String>, Error>;
    async fn set(&self, cursor: String) -> Result<(), Error>;
}

pub struct CrdbCursorStore {
    pool: PgPool,
    key: String,
}

impl CrdbCursorStore {
    pub async fn new(pool: PgPool, key: String) -> Result<Self, Error> {
        let _ = sqlx::query("CREATE TABLE IF NOT EXISTS cursor_store (key STRING NOT NULL PRIMARY KEY, cursor STRING NOT NULL);").execute(&pool).await?;

        Ok(Self { pool, key })
    }
}

#[async_trait]
impl CursorStore for CrdbCursorStore {
    async fn get(&self) -> Result<Option<String>, Error> {
        let query = sqlx::query("SELECT cursor FROM cursor_store WHERE key = $1;").bind(&self.key);
        let mut fetched = query.fetch(&self.pool);

        let row = match fetched.next().await {
            Some(Ok(v)) => v,
            Some(Err(v)) => {
                error!("{:?}", v);
                return Ok(None);
            }
            None => return Ok(None),
        };

        let value: String = row.get(0);
        Ok(Some(value))
    }

    async fn set(&self, cursor: String) -> Result<(), Error> {
        let result = sqlx::query("UPSERT INTO cursor_store (key, cursor) VALUES ($1, $2);")
            .bind(&self.key)
            .bind(cursor)
            .execute(&self.pool)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::SqlxError(e)),
        }
    }
}
