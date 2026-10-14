pub mod clusters;
pub mod lag_rules;

use sqlx::{
    SqlitePool,
    migrate::Migrator,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::str::FromStr;

pub static MIGRATOR: Migrator = sqlx::migrate!();

#[derive(thiserror::Error)]
pub enum RepositoryError {
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
    #[error("secret encryption or authentication failed")]
    Crypto(#[from] crate::crypto::CryptoError),
    #[error("invalid stored cluster settings")]
    InvalidStoredConfig,
    #[error("invalid settings")]
    InvalidInput,
    #[error("cluster does not exist")]
    ClusterNotFound,
}

impl std::fmt::Debug for RepositoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

pub async fn connect(url: &str) -> Result<SqlitePool, sqlx::Error> {
    let options = SqliteConnectOptions::from_str(url)?
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await?;
    MIGRATOR.run(&pool).await?;
    Ok(pool)
}
