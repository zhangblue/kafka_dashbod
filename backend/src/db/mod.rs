pub mod clusters;
pub mod feishu_alerts;
pub mod lag_rules;

use sqlx::{
    SqlitePool,
    migrate::{MigrateError, Migrator},
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
};
use std::str::FromStr;

pub static MIGRATOR: Migrator = sqlx::migrate!();

#[derive(thiserror::Error)]
pub enum RepositoryError {
    #[error("database operation failed")]
    Database(#[from] sqlx::Error),
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

#[derive(Debug, thiserror::Error)]
pub enum DatabaseInitError {
    #[error("configuration database is incompatible; delete it and configure clusters again")]
    Incompatible,
    #[error("failed to initialize configuration database")]
    Database,
}

pub async fn connect(url: &str) -> Result<SqlitePool, DatabaseInitError> {
    let options = SqliteConnectOptions::from_str(url)
        .map_err(|_| DatabaseInitError::Database)?
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(8)
        .connect_with(options)
        .await
        .map_err(|_| DatabaseInitError::Database)?;
    MIGRATOR.run(&pool).await.map_err(|error| match error {
        MigrateError::VersionMismatch(_) => DatabaseInitError::Incompatible,
        _ => DatabaseInitError::Database,
    })?;
    Ok(pool)
}
