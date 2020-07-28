use crate::Error;

use std::string::String;
use sqlx::{
    postgres::PgPool,
    prelude::*,
};
use async_trait::async_trait;

#[async_trait]
pub trait CursorStore {
    async fn get(&self) -> Result<Option<String>, Error>;
    async fn set(&self, cursor: String) -> Result<(), Error>;
}

pub struct CrdbCursorStore {
    pool: PgPool,
}

impl CrdbCursorStore {
    pub async fn new(pool: PgPool) -> Result<Self, Error> {
        let _ = sqlx::query("CREATE TABLE IF NOT EXISTS cursor_store (key STRING NOT NULL PRIMARY KEY, cursor STRING NOT NULL);").execute(&pool).await?;

        Ok(Self {
            pool: pool,
        })
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
        let result = sqlx::query(&format!("UPSERT INTO cursor_store (key, cursor) VALUES ('key', $1);"))
            .bind(cursor)
            .execute(&self.pool)
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Error::SqlxError(e))
        }
    }
}