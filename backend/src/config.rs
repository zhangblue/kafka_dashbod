use crate::crypto::{CryptoError, SecretCipher};
use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("KAFKA_DASHBOARD_MASTER_KEY is required and must be valid Unicode")]
    MissingMasterKey,
    #[error(transparent)]
    InvalidMasterKey(#[from] CryptoError),
    #[error("KAFKA_DASHBOARD_WEB_DIR must be an existing directory containing index.html")]
    InvalidWebDirectory,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub cipher: SecretCipher,
    pub web_dir: Option<PathBuf>,
}

impl Config {
    pub fn from_values(
        database_url: impl Into<String>,
        master_key: &str,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            database_url: database_url.into(),
            cipher: SecretCipher::from_base64_key(master_key)?,
            web_dir: None,
        })
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let key = std::env::var("KAFKA_DASHBOARD_MASTER_KEY")
            .map_err(|_| ConfigError::MissingMasterKey)?;
        let url =
            std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://kafka-dashboard.db".into());
        let mut config = Self::from_values(url, &key)?;
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
            config.web_dir = Some(root);
        }
        Ok(config)
    }
}
