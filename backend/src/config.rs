use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("KAFKA_DASHBOARD_WEB_DIR must be an existing directory containing index.html")]
    InvalidWebDirectory,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub web_dir: Option<PathBuf>,
}

impl Config {
    // 在测试或显式配置初始化时，根据数据库地址构造不提供静态页面的基础配置。
    pub fn from_values(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            web_dir: None,
        }
    }

    // 在服务启动时读取环境配置并校验静态资源目录，使无效页面配置在打开数据库前失败。
    pub fn from_env() -> Result<Self, ConfigError> {
        let url =
            std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://kafka-dashboard.db".into());
        let mut config = Self::from_values(url);
        if let Some(path) = std::env::var_os("KAFKA_DASHBOARD_WEB_DIR") {
            if path.is_empty() {
                return Err(ConfigError::InvalidWebDirectory);
            }
            let root = std::fs::canonicalize(path).map_err(|_| ConfigError::InvalidWebDirectory)?;
            let index = std::fs::canonicalize(root.join("index.html"))
                .map_err(|_| ConfigError::InvalidWebDirectory)?;
            if !root.is_dir() || !index.is_file() || !index.starts_with(&root) {
                return Err(ConfigError::InvalidWebDirectory);
            }
            // 必须在数据库初始化前复用静态服务的符号链接检查，避免无效资源配置留下数据库副作用。
            crate::api::static_assets::StaticRoot::new(&root)
                .map_err(|_| ConfigError::InvalidWebDirectory)?;
            config.web_dir = Some(root);
        }
        Ok(config)
    }
}
