use super::RepositoryError;
use crate::{
    domain::cluster::{
        ClusterDto, ClusterInput, DecryptedClusterConfig, SecretClears, SecurityProtocol,
    },
    kafka::config::build_client_properties,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::time::Duration;

#[derive(Clone)]
pub struct ClusterRepository {
    pool: SqlitePool,
}

#[derive(Serialize, Deserialize, Default)]
struct ClusterSecrets {
    password: Option<String>,
    ca_pem: Option<String>,
    client_certificate_pem: Option<String>,
    client_key_pem: Option<String>,
    client_key_passphrase: Option<String>,
}

impl ClusterSecrets {
    fn merge(self, input: &ClusterInput, clears: SecretClears) -> Result<Self, RepositoryError> {
        Ok(Self {
            password: merge_secret(self.password, &input.password, clears.clear_password)?,
            ca_pem: merge_secret(self.ca_pem, &input.ca_pem, clears.clear_ca_pem)?,
            client_certificate_pem: merge_secret(
                self.client_certificate_pem,
                &input.client_certificate_pem,
                clears.clear_client_certificate_pem,
            )?,
            client_key_pem: merge_secret(
                self.client_key_pem,
                &input.client_key_pem,
                clears.clear_client_key_pem,
            )?,
            client_key_passphrase: merge_secret(
                self.client_key_passphrase,
                &input.client_key_passphrase,
                clears.clear_client_key_passphrase,
            )?,
        })
    }

    fn config(&self, input: &ClusterInput) -> Result<DecryptedClusterConfig, RepositoryError> {
        let timeout = u64::try_from(input.connection_timeout_ms)
            .map_err(|_| RepositoryError::InvalidInput)?;
        let config = DecryptedClusterConfig {
            bootstrap_servers: input.bootstrap_servers.clone(),
            security_protocol: input.security_protocol,
            sasl_mechanism: input.sasl_mechanism.clone(),
            username: input.username.clone(),
            password: self.password.clone(),
            ca_pem: self.ca_pem.clone(),
            client_certificate_pem: self.client_certificate_pem.clone(),
            client_key_pem: self.client_key_pem.clone(),
            client_key_passphrase: self.client_key_passphrase.clone(),
            connection_timeout: Duration::from_millis(timeout),
        };
        build_client_properties(&config).map_err(|_| RepositoryError::InvalidInput)?;
        Ok(config)
    }
}

fn merge_secret(
    previous: Option<String>,
    replacement: &Option<String>,
    clear: bool,
) -> Result<Option<String>, RepositoryError> {
    let replacement = replacement
        .as_ref()
        .filter(|value| !value.is_empty())
        .cloned();
    if clear && replacement.is_some() {
        return Err(RepositoryError::InvalidInput);
    }
    Ok(if clear {
        None
    } else {
        replacement.or(previous)
    })
}

#[derive(sqlx::FromRow)]
struct ClusterRow {
    id: String,
    name: String,
    bootstrap_servers: String,
    security_protocol: String,
    sasl_mechanism: Option<String>,
    username: Option<String>,
    secrets_json: Option<Vec<u8>>,
    connection_timeout_ms: i64,
    default_normal_lag_max: i64,
    default_high_lag_min: i64,
    default_over_limit_seconds: i64,
    created_at: String,
    updated_at: String,
}

impl ClusterRow {
    fn into_dto(self, secrets: &ClusterSecrets) -> Result<ClusterDto, RepositoryError> {
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
            password_configured: secrets.password.is_some(),
            ca_pem_configured: secrets.ca_pem.is_some(),
            client_certificate_pem_configured: secrets.client_certificate_pem.is_some(),
            client_key_pem_configured: secrets.client_key_pem.is_some(),
            client_key_passphrase_configured: secrets.client_key_passphrase.is_some(),
            connection_timeout_ms: self.connection_timeout_ms,
            default_normal_lag_max: self.default_normal_lag_max,
            default_high_lag_min: self.default_high_lag_min,
            default_over_limit_seconds: self.default_over_limit_seconds,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

// Secret values remain private; public reads expose only configured flags.
macro_rules! public_columns {
    ($prefix:literal, $suffix:literal) => {
        concat!($prefix, "id, name, bootstrap_servers, security_protocol, sasl_mechanism, username, secrets_json, connection_timeout_ms, default_normal_lag_max, default_high_lag_min, default_over_limit_seconds, created_at, updated_at", $suffix)
    };
}

impl ClusterRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn encode_secrets(&self, secrets: &ClusterSecrets) -> Result<Vec<u8>, RepositoryError> {
        serde_json::to_vec(secrets).map_err(|_| RepositoryError::InvalidInput)
    }

    fn decode_secrets(&self, stored: Option<Vec<u8>>) -> Result<ClusterSecrets, RepositoryError> {
        match stored {
            Some(bytes) => {
                serde_json::from_slice(&bytes).map_err(|_| RepositoryError::InvalidStoredConfig)
            }
            None => Ok(ClusterSecrets::default()),
        }
    }

    pub async fn create(&self, input: &ClusterInput) -> Result<ClusterDto, RepositoryError> {
        self.create_with_secret_clears(input, SecretClears::default())
            .await
    }

    pub async fn create_with_secret_clears(
        &self,
        input: &ClusterInput,
        clears: SecretClears,
    ) -> Result<ClusterDto, RepositoryError> {
        let secrets = ClusterSecrets::default().merge(input, clears)?;
        secrets.config(input)?;
        let stored = self.encode_secrets(&secrets)?;
        let id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let sql = public_columns!(
            "INSERT INTO clusters (id, name, bootstrap_servers, security_protocol, sasl_mechanism, username, secrets_json, connection_timeout_ms, default_normal_lag_max, default_high_lag_min, default_over_limit_seconds, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) RETURNING ",
            ""
        );
        let row: ClusterRow = sqlx::query_as(sql)
            .bind(&id)
            .bind(&input.name)
            .bind(&input.bootstrap_servers)
            .bind(input.security_protocol.as_str())
            .bind(&input.sasl_mechanism)
            .bind(&input.username)
            .bind(stored)
            .bind(input.connection_timeout_ms)
            .bind(input.default_normal_lag_max)
            .bind(input.default_high_lag_min)
            .bind(input.default_over_limit_seconds)
            .bind(&now)
            .bind(&now)
            .fetch_one(&self.pool)
            .await?;
        row.into_dto(&secrets)
    }

    pub async fn get(&self, id: &str) -> Result<Option<ClusterDto>, RepositoryError> {
        let sql = public_columns!("SELECT ", " FROM clusters WHERE id = ?");
        let row: Option<ClusterRow> = sqlx::query_as(sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        row.map(|mut row| {
            let secrets = self.decode_secrets(row.secrets_json.take())?;
            row.into_dto(&secrets)
        })
        .transpose()
    }

    pub async fn list(&self) -> Result<Vec<ClusterDto>, RepositoryError> {
        let sql = public_columns!("SELECT ", " FROM clusters ORDER BY name, id");
        let rows: Vec<ClusterRow> = sqlx::query_as(sql).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|mut row| {
                let secrets = self.decode_secrets(row.secrets_json.take())?;
                row.into_dto(&secrets)
            })
            .collect()
    }

    /// Updates settings; omitted or empty sensitive fields retain their previous values.
    pub async fn update(
        &self,
        id: &str,
        input: &ClusterInput,
    ) -> Result<Option<ClusterDto>, RepositoryError> {
        self.update_with_secret_clears(id, input, SecretClears::default())
            .await
    }

    /// Merge, validate, and persist all secret changes in one transaction.
    pub async fn update_with_secret_clears(
        &self,
        id: &str,
        input: &ClusterInput,
        clears: SecretClears,
    ) -> Result<Option<ClusterDto>, RepositoryError> {
        let mut tx = self.pool.begin().await?;
        let previous: Option<Option<Vec<u8>>> =
            sqlx::query_scalar("SELECT secrets_json FROM clusters WHERE id = ?")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(previous) = previous else {
            return Ok(None);
        };
        let merged = self.decode_secrets(previous)?.merge(input, clears)?;
        merged.config(input)?;
        let stored = self.encode_secrets(&merged)?;
        let sql = public_columns!(
            "UPDATE clusters SET name = ?, bootstrap_servers = ?, security_protocol = ?, sasl_mechanism = ?, username = ?, secrets_json = ?, connection_timeout_ms = ?, default_normal_lag_max = ?, default_high_lag_min = ?, default_over_limit_seconds = ?, updated_at = ? WHERE id = ? RETURNING ",
            ""
        );
        let row: Option<ClusterRow> = sqlx::query_as(sql)
            .bind(&input.name)
            .bind(&input.bootstrap_servers)
            .bind(input.security_protocol.as_str())
            .bind(&input.sasl_mechanism)
            .bind(&input.username)
            .bind(stored)
            .bind(input.connection_timeout_ms)
            .bind(input.default_normal_lag_max)
            .bind(input.default_high_lag_min)
            .bind(input.default_over_limit_seconds)
            .bind(Utc::now().to_rfc3339())
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?;
        let result = row.map(|row| row.into_dto(&merged)).transpose()?;
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

    /// Resolve a new or edited draft without persisting it or allocating Kafka resources.
    pub async fn draft_config(
        &self,
        id: Option<&str>,
        input: &ClusterInput,
        clears: SecretClears,
    ) -> Result<Option<DecryptedClusterConfig>, RepositoryError> {
        let secrets = if let Some(id) = id {
            let stored: Option<Option<Vec<u8>>> =
                sqlx::query_scalar("SELECT secrets_json FROM clusters WHERE id = ?")
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await?;
            let Some(stored) = stored else {
                return Ok(None);
            };
            self.decode_secrets(stored)?
        } else {
            ClusterSecrets::default()
        };
        Ok(Some(secrets.merge(input, clears)?.config(input)?))
    }

    /// Internal connection configuration; never serialize this value into an API response.
    pub async fn decrypted_config(
        &self,
        id: &str,
    ) -> Result<Option<DecryptedClusterConfig>, RepositoryError> {
        // Read public settings and secrets from the same SQLite statement/snapshot.
        let sql = public_columns!("SELECT ", " FROM clusters WHERE id = ?");
        let row: Option<ClusterRow> = sqlx::query_as(sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(mut row) = row else {
            return Ok(None);
        };
        let secrets = self.decode_secrets(row.secrets_json.take())?;
        let public = row.into_dto(&secrets)?;
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
            client_key_passphrase: secrets.client_key_passphrase,
            connection_timeout: Duration::from_millis(timeout),
        }))
    }
}
