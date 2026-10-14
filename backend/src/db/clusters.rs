use super::RepositoryError;
use crate::{
    crypto::SecretCipher,
    domain::cluster::{ClusterDto, ClusterInput, DecryptedClusterConfig, SecurityProtocol},
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::time::Duration;

#[derive(Clone)]
pub struct ClusterRepository {
    pool: SqlitePool,
    cipher: SecretCipher,
}

#[derive(Serialize, Deserialize, Default)]
struct ClusterSecrets {
    password: Option<String>,
    ca_pem: Option<String>,
    client_certificate_pem: Option<String>,
    client_key_pem: Option<String>,
}

impl From<&ClusterInput> for ClusterSecrets {
    fn from(input: &ClusterInput) -> Self {
        Self {
            password: input.password.clone(),
            ca_pem: input.ca_pem.clone(),
            client_certificate_pem: input.client_certificate_pem.clone(),
            client_key_pem: input.client_key_pem.clone(),
        }
    }
}

fn merge_secret(previous: Option<String>, replacement: &Option<String>) -> Option<String> {
    replacement
        .as_ref()
        .filter(|value| !value.is_empty())
        .cloned()
        .or(previous)
}

#[derive(sqlx::FromRow)]
struct ClusterRow {
    id: String,
    name: String,
    bootstrap_servers: String,
    security_protocol: String,
    sasl_mechanism: Option<String>,
    username: Option<String>,
    connection_timeout_ms: i64,
    default_normal_lag_max: i64,
    default_high_lag_min: i64,
    default_over_limit_seconds: i64,
    created_at: String,
    updated_at: String,
}

impl ClusterRow {
    fn into_dto(self) -> Result<ClusterDto, RepositoryError> {
        let security_protocol = match self.security_protocol.as_str() {
            "PLAINTEXT" => SecurityProtocol::Plaintext,
            "SSL" => SecurityProtocol::Ssl,
            "SASL_PLAINTEXT" => SecurityProtocol::SaslPlaintext,
            "SASL_SSL" => SecurityProtocol::SaslSsl,
            _ => return Err(RepositoryError::InvalidStoredConfig),
        };
        Ok(ClusterDto {
            id: self.id,
            name: self.name,
            bootstrap_servers: self.bootstrap_servers,
            security_protocol,
            sasl_mechanism: self.sasl_mechanism,
            username: self.username,
            connection_timeout_ms: self.connection_timeout_ms,
            default_normal_lag_max: self.default_normal_lag_max,
            default_high_lag_min: self.default_high_lag_min,
            default_over_limit_seconds: self.default_over_limit_seconds,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

// Public reads deliberately do not fetch encrypted secrets.
macro_rules! public_columns {
    ($prefix:literal, $suffix:literal) => {
        concat!($prefix, "id, name, bootstrap_servers, security_protocol, sasl_mechanism, username, connection_timeout_ms, default_normal_lag_max, default_high_lag_min, default_over_limit_seconds, created_at, updated_at", $suffix)
    };
}

impl ClusterRepository {
    pub fn new(pool: SqlitePool, cipher: SecretCipher) -> Self {
        Self { pool, cipher }
    }

    fn encrypt_secrets(&self, secrets: &ClusterSecrets) -> Result<Vec<u8>, RepositoryError> {
        let plaintext = serde_json::to_vec(secrets).map_err(|_| RepositoryError::InvalidInput)?;
        Ok(self.cipher.encrypt(&plaintext)?)
    }

    fn decrypt_secrets(
        &self,
        encrypted: Option<Vec<u8>>,
    ) -> Result<ClusterSecrets, RepositoryError> {
        match encrypted {
            Some(bytes) => serde_json::from_slice(&self.cipher.decrypt(&bytes)?)
                .map_err(|_| RepositoryError::InvalidStoredConfig),
            None => Ok(ClusterSecrets::default()),
        }
    }

    pub async fn create(&self, input: &ClusterInput) -> Result<ClusterDto, RepositoryError> {
        let encrypted = self.encrypt_secrets(&ClusterSecrets::from(input))?;
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let sql = public_columns!(
            "INSERT INTO clusters (id, name, bootstrap_servers, security_protocol, sasl_mechanism, username, encrypted_secrets, connection_timeout_ms, default_normal_lag_max, default_high_lag_min, default_over_limit_seconds, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ",
            ""
        );
        let row: ClusterRow = sqlx::query_as(sql)
            .bind(&id)
            .bind(&input.name)
            .bind(&input.bootstrap_servers)
            .bind(input.security_protocol.as_str())
            .bind(&input.sasl_mechanism)
            .bind(&input.username)
            .bind(encrypted)
            .bind(input.connection_timeout_ms)
            .bind(input.default_normal_lag_max)
            .bind(input.default_high_lag_min)
            .bind(input.default_over_limit_seconds)
            .bind(&now)
            .bind(&now)
            .fetch_one(&self.pool)
            .await?;
        row.into_dto()
    }

    pub async fn get(&self, id: &str) -> Result<Option<ClusterDto>, RepositoryError> {
        let sql = public_columns!("SELECT ", " FROM clusters WHERE id = ?");
        let row: Option<ClusterRow> = sqlx::query_as(sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(ClusterRow::into_dto).transpose()
    }

    pub async fn list(&self) -> Result<Vec<ClusterDto>, RepositoryError> {
        let sql = public_columns!("SELECT ", " FROM clusters ORDER BY name, id");
        let rows: Vec<ClusterRow> = sqlx::query_as(sql).fetch_all(&self.pool).await?;
        rows.into_iter().map(ClusterRow::into_dto).collect()
    }

    /// Updates settings; omitted or empty sensitive fields retain their previous values.
    pub async fn update(
        &self,
        id: &str,
        input: &ClusterInput,
    ) -> Result<Option<ClusterDto>, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let previous: Option<Option<Vec<u8>>> =
            sqlx::query_scalar("SELECT encrypted_secrets FROM clusters WHERE id = ?")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(previous) = previous else {
            return Ok(None);
        };
        let previous = self.decrypt_secrets(previous)?;
        let merged = ClusterSecrets {
            password: merge_secret(previous.password, &input.password),
            ca_pem: merge_secret(previous.ca_pem, &input.ca_pem),
            client_certificate_pem: merge_secret(
                previous.client_certificate_pem,
                &input.client_certificate_pem,
            ),
            client_key_pem: merge_secret(previous.client_key_pem, &input.client_key_pem),
        };
        let encrypted = self.encrypt_secrets(&merged)?;
        let sql = public_columns!(
            "UPDATE clusters SET name = ?, bootstrap_servers = ?, security_protocol = ?, sasl_mechanism = ?, username = ?, encrypted_secrets = ?, connection_timeout_ms = ?, default_normal_lag_max = ?, default_high_lag_min = ?, default_over_limit_seconds = ?, updated_at = ? WHERE id = ? RETURNING ",
            ""
        );
        let row: Option<ClusterRow> = sqlx::query_as(sql)
            .bind(&input.name)
            .bind(&input.bootstrap_servers)
            .bind(input.security_protocol.as_str())
            .bind(&input.sasl_mechanism)
            .bind(&input.username)
            .bind(encrypted)
            .bind(input.connection_timeout_ms)
            .bind(input.default_normal_lag_max)
            .bind(input.default_high_lag_min)
            .bind(input.default_over_limit_seconds)
            .bind(Utc::now().to_rfc3339())
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        let result = row.map(ClusterRow::into_dto).transpose()?;
        tx.commit().await?;
        Ok(result)
    }

    pub async fn delete(&self, id: &str) -> Result<bool, RepositoryError> {
        Ok(sqlx::query("DELETE FROM clusters WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0)
    }

    /// Internal connection configuration; never serialize this value into an API response.
    pub async fn decrypted_config(
        &self,
        id: &str,
    ) -> Result<Option<DecryptedClusterConfig>, RepositoryError> {
        // Read public settings and secrets from the same SQLite statement/snapshot.
        let sql = public_columns!("SELECT ", ", encrypted_secrets FROM clusters WHERE id = ?");
        let row = sqlx::query(sql).bind(id).fetch_optional(&self.pool).await?;
        let Some(row) = row else {
            return Ok(None);
        };
        use sqlx::{FromRow, Row};
        let public = ClusterRow::from_row(&row)?.into_dto()?;
        let encrypted: Option<Vec<u8>> = row.try_get("encrypted_secrets")?;
        let secrets = self.decrypt_secrets(encrypted)?;
        let timeout = u64::try_from(public.connection_timeout_ms)
            .map_err(|_| RepositoryError::InvalidStoredConfig)?;
        Ok(Some(DecryptedClusterConfig {
            bootstrap_servers: public.bootstrap_servers,
            security_protocol: public.security_protocol,
            sasl_mechanism: public.sasl_mechanism,
            username: public.username,
            password: secrets.password,
            ca_pem: secrets.ca_pem,
            client_certificate_pem: secrets.client_certificate_pem,
            client_key_pem: secrets.client_key_pem,
            connection_timeout: Duration::from_millis(timeout),
        }))
    }
}
