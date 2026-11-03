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
    // 在仓储错误被调试格式化时复用安全展示文本，避免打印底层数据库上下文。
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

// 在服务启动或测试初始化时建立启用外键的连接池并执行迁移，旧结构不兼容时安全失败。
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
    // 连接成功后先完成迁移再交出池；版本不兼容只报错，不能转换或自动删除旧数据库。
    MIGRATOR.run(&pool).await.map_err(|error| match error {
        MigrateError::VersionMismatch(_) => DatabaseInitError::Incompatible,
        _ => DatabaseInitError::Database,
    })?;
    Ok(pool)
}
