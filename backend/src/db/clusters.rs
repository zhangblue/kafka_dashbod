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
    // 在新建、编辑或测试草稿时合并敏感字段，留空保留旧值，显式清除与新值冲突则拒绝。
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

    // 在持久化或测试连接前构造并校验合并后的完整配置，避免凭据组合错误进入原生客户端。
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
        // 先校验合并后的 SASL/TLS 组合，再允许保存或创建原生客户端，防止只校验新字段遗漏旧秘密。
        build_client_properties(&config).map_err(|_| RepositoryError::InvalidInput)?;
        Ok(config)
    }
}

// 在合并单个敏感字段时应用替换、保留或清除语义，拒绝同时清除并设置。
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
    // 在查询或保存集群后构造公开 DTO，仅暴露敏感字段配置标志并拒绝未知协议。
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
    // 在组装 API 或后台依赖时从共享连接池创建集群仓储，不建立 Kafka 连接。
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    // 在写入集群配置前将敏感字段编码为明文 JSON；返回值只能用于受保护的数据库存储。
    fn encode_secrets(&self, secrets: &ClusterSecrets) -> Result<Vec<u8>, RepositoryError> {
        serde_json::to_vec(secrets).map_err(|_| RepositoryError::InvalidInput)
    }

    // 在读取数据库配置时解析明文敏感字段，缺省为空，损坏数据返回安全存储错误。
    fn decode_secrets(&self, stored: Option<Vec<u8>>) -> Result<ClusterSecrets, RepositoryError> {
        match stored {
            Some(bytes) => {
                serde_json::from_slice(&bytes).map_err(|_| RepositoryError::InvalidStoredConfig)
            }
            None => Ok(ClusterSecrets::default()),
        }
    }

    // 在不需要显式清除字段的新增调用中委托完整创建流程，复用一致校验。
    pub async fn create(&self, input: &ClusterInput) -> Result<ClusterDto, RepositoryError> {
        self.create_with_secret_clears(input, SecretClears::default())
            .await
    }

    // 在新增集群 API 保存时合并并校验完整配置，再写入数据库且仅返回公开标志。
    pub async fn create_with_secret_clears(
        &self,
        input: &ClusterInput,
        clears: SecretClears,
    ) -> Result<ClusterDto, RepositoryError> {
        // 新建也采用统一合并规则，先拒绝“设置并清除”的冲突，再校验完整配置后编码落库。
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

    // 在集群详情 API 或存在性检查时读取单个公开配置，不向调用方返回敏感原值。
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

    // 在集群列表 API 调用时按名称和标识读取公开配置，敏感字段仅转换为配置标志。
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
    // 在普通编辑调用中委托带清除选项的更新流程，空敏感字段继续保留旧值。
    pub async fn update(
        &self,
        id: &str,
        input: &ClusterInput,
    ) -> Result<Option<ClusterDto>, RepositoryError> {
        self.update_with_secret_clears(id, input, SecretClears::default())
            .await
    }

    /// Merge, validate, and persist all secret changes in one transaction.
    // 在编辑 API 保存时于单一事务内读取旧秘密、合并校验再更新，任一步失败均不提交。
    pub async fn update_with_secret_clears(
        &self,
        id: &str,
        input: &ClusterInput,
        clears: SecretClears,
    ) -> Result<Option<ClusterDto>, RepositoryError> {
        // 将读取旧秘密、合并和写入放入同一事务；提前返回或校验失败时不提交，避免半更新。
        let mut tx = self.pool.begin().await?;
        let previous: Option<Option<Vec<u8>>> =
            sqlx::query_scalar("SELECT secrets_json FROM clusters WHERE id = ?")
                .bind(id)
                .fetch_optional(&mut *tx)
                .await?;
        let Some(previous) = previous else {
            return Ok(None);
        };
        // 必须先按清除/留空语义合并旧秘密，再校验切换协议后的完整配置，不能直接覆盖为请求字段。
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
        // 公开 DTO 转换也成功后才提交，确保无效存储协议等错误不会留下已保存的部分修改。
        tx.commit().await?;
        Ok(result)
    }

    // 在集群删除流程已处理关联资源后删除配置，返回是否实际删除记录。
    pub async fn delete(&self, id: &str) -> Result<bool, RepositoryError> {
        Ok(sqlx::query("DELETE FROM clusters WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?
            .rows_affected()
            > 0)
    }

    /// Resolve a new or edited draft without persisting it or allocating Kafka resources.
    // 在新增或编辑连接测试时解析草稿与旧秘密而不保存，也不分配 Kafka 资源。
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
        // 测试草稿遵循保存时相同的合并和完整校验顺序，保证测试与实际保存使用一致的凭据。
        Ok(Some(secrets.merge(input, clears)?.config(input)?))
    }

    /// Internal connection configuration; never serialize this value into an API response.
    // 在会话、后台监控或已保存配置测试需要连接时读取内部明文配置，禁止用于 API 序列化。
    pub async fn decrypted_config(
        &self,
        id: &str,
    ) -> Result<Option<DecryptedClusterConfig>, RepositoryError> {
        // 用同一条查询读取公开字段和秘密，避免并发编辑导致连接参数来自不同版本。
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
