use crate::crypto::{CryptoError, SecretCipher};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("KAFKA_DASHBOARD_MASTER_KEY is required and must be valid Unicode")]
    MissingMasterKey,
    #[error(transparent)]
    InvalidMasterKey(#[from] CryptoError),
}

#[derive(Debug, Clone)]
pub struct Config {
    pub database_url: String,
    pub cipher: SecretCipher,
}

impl Config {
    pub fn from_values(
        database_url: impl Into<String>,
        master_key: &str,
    ) -> Result<Self, ConfigError> {
        Ok(Self {
            database_url: database_url.into(),
            cipher: SecretCipher::from_base64_key(master_key)?,
        })
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let key = std::env::var("KAFKA_DASHBOARD_MASTER_KEY")
            .map_err(|_| ConfigError::MissingMasterKey)?;
        let url =
            std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://kafka-dashboard.db".into());
        Self::from_values(url, &key)
    }
}
