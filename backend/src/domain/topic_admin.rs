use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};

pub const TOPIC_CONFIG_KEYS: [&str; 8] = [
    "retention.ms",
    "retention.bytes",
    "cleanup.policy",
    "segment.ms",
    "segment.bytes",
    "min.insync.replicas",
    "max.message.bytes",
    "compression.type",
];

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateTopicInput {
    pub name: String,
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    pub replication_factor: Option<i32>,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

impl CreateTopicInput {
    // 在创建 Topic 进入原生管理调用前校验名称、数量上限和白名单配置，阻止非法输入下沉。
    pub fn validate(&self) -> Result<(), BTreeMap<String, String>> {
        let mut errors = BTreeMap::new();
        // 先在领域层拒绝非法名称，再检查原生数量上限及配置，避免无效请求触及 Kafka。
        validate_topic_name(&self.name, "name", &mut errors);

        if self.partitions <= 0 {
            errors.insert(
                "partitions".into(),
                "Partitions must be greater than zero".into(),
            );
        } else if self.partitions > 100_000 {
            errors.insert(
                "partitions".into(),
                "Partitions must not exceed 100000".into(),
            );
        }
        if self.replication_factor.is_some_and(|factor| factor <= 0) {
            errors.insert(
                "replicationFactor".into(),
                "Replication factor must be greater than zero".into(),
            );
        } else if self
            .replication_factor
            .is_some_and(|factor| factor > 10_000)
        {
            errors.insert(
                "replicationFactor".into(),
                "Replication factor must not exceed 10000".into(),
            );
        }

        validate_configs(&self.configs, "configs", &mut errors);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

// 在反序列化创建请求未指定分区数时提供单分区默认值。
const fn default_partitions() -> i32 {
    1
}

// 在创建或删除前校验 Topic 名称的长度和字符边界，并累积安全字段错误。
fn validate_topic_name(name: &str, field: &str, errors: &mut BTreeMap<String, String>) {
    let valid_chars = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if !(1..=249).contains(&name.len()) || !valid_chars || matches!(name, "." | "..") {
        errors.insert(field.into(), "Invalid topic name".into());
    }
}

// 在创建和增量修改配置校验阶段限制允许的键和值，不回显请求中的配置值。
fn validate_configs(
    configs: &BTreeMap<String, String>,
    field: &str,
    errors: &mut BTreeMap<String, String>,
) {
    for (key, value) in configs {
        if !TOPIC_CONFIG_KEYS.contains(&key.as_str()) {
            errors.insert(field.into(), "Unsupported topic configuration".into());
            continue;
        }
        if !valid_config_value(key, value) {
            errors.insert(
                format!("{field}.{key}"),
                "Invalid topic configuration value".into(),
            );
        }
    }
}

// 在白名单配置校验时按各键的数值或枚举语义判断值是否合法。
fn valid_config_value(key: &str, value: &str) -> bool {
    match key {
        "retention.ms" | "retention.bytes" => {
            value == "-1" || parse_decimal_i64(value).is_some_and(|number| number >= 0)
        }
        "segment.ms" | "segment.bytes" | "min.insync.replicas" | "max.message.bytes" => {
            parse_decimal_i64(value).is_some_and(|number| number > 0)
        }
        "cleanup.policy" => matches!(value, "delete" | "compact" | "delete,compact"),
        "compression.type" => matches!(
            value,
            "producer" | "gzip" | "snappy" | "lz4" | "zstd" | "uncompressed"
        ),
        _ => false,
    }
}

// 在配置值校验时只解析无符号十进制文本，拒绝空值、符号和超出 i64 的整数。
fn parse_decimal_i64(value: &str) -> Option<i64> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigValue {
    pub key: String,
    pub value: Option<String>,
    pub source: TopicConfigSource,
    pub read_only: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TopicConfigSource {
    TopicOverride,
    BrokerDefault,
    Unknown,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateTopicConfigsInput {
    pub changes: BTreeMap<String, Option<String>>,
    pub expected_values: BTreeMap<String, Option<String>>,
}

impl UpdateTopicConfigsInput {
    // 在增量修改进入 Kafka 前校验非空变更、白名单和预期旧值，保证后续可检查写前冲突。
    pub fn validate(&self) -> Result<(), BTreeMap<String, String>> {
        let mut errors = BTreeMap::new();
        if self.changes.is_empty() {
            errors.insert(
                "changes".into(),
                "At least one topic configuration change is required".into(),
            );
        }

        validate_configs(
            &self
                .changes
                .iter()
                .filter_map(|(key, value)| value.as_ref().map(|value| (key.clone(), value.clone())))
                .collect(),
            "changes",
            &mut errors,
        );

        for key in self.changes.keys() {
            if !TOPIC_CONFIG_KEYS.contains(&key.as_str()) {
                errors.insert("changes".into(), "Unsupported topic configuration".into());
            } else if !self.expected_values.contains_key(key) {
                errors.insert(
                    format!("expectedValues.{key}"),
                    "Expected current value is required".into(),
                );
            }
        }

        if self
            .expected_values
            .keys()
            .any(|key| !TOPIC_CONFIG_KEYS.contains(&key.as_str()))
        {
            errors.insert(
                "expectedValues".into(),
                "Unsupported topic configuration".into(),
            );
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TopicConfigUpdateOutcome {
    Confirmed(Vec<TopicConfigValue>),
    AppliedButUnconfirmed,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TopicAdminError {
    Validation(BTreeMap<String, String>),
    InternalTopicProtected,
    AlreadyExists,
    NotFound,
    Authorization,
    Timeout,
    InvalidConfig,
    ConfigChanged,
    Unavailable,
}

impl TopicAdminError {
    // 在管理错误映射为 API 响应时返回稳定错误码，避免依赖原生错误文本。
    pub fn code(&self) -> &'static str {
        match self {
            Self::Validation(_) => "VALIDATION_ERROR",
            Self::InternalTopicProtected => "INTERNAL_TOPIC_PROTECTED",
            Self::AlreadyExists => "TOPIC_ALREADY_EXISTS",
            Self::NotFound => "TOPIC_NOT_FOUND",
            Self::Authorization => "TOPIC_AUTHORIZATION_FAILED",
            Self::Timeout => "TOPIC_OPERATION_TIMEOUT",
            Self::InvalidConfig => "INVALID_TOPIC_CONFIG",
            Self::ConfigChanged => "TOPIC_CONFIG_CHANGED",
            Self::Unavailable => "KAFKA_UNAVAILABLE",
        }
    }

    // 在生成管理校验错误响应时获取字段说明，非校验错误返回空集合。
    pub fn validation_errors(&self) -> &BTreeMap<String, String> {
        match self {
            Self::Validation(errors) => errors,
            _ => &EMPTY_VALIDATION_ERRORS,
        }
    }
}

static EMPTY_VALIDATION_ERRORS: BTreeMap<String, String> = BTreeMap::new();

impl fmt::Display for TopicAdminError {
    // 在展示或记录管理错误时输出固定安全消息，防止泄漏 Kafka 原始错误。
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Validation(_) => "Invalid topic administration request",
            Self::InternalTopicProtected => "Internal topics cannot be deleted",
            Self::AlreadyExists => "Topic already exists",
            Self::NotFound => "Topic was not found",
            Self::Authorization => "Kafka authorization failed",
            Self::Timeout => "Topic operation timed out",
            Self::InvalidConfig => "Kafka rejected the topic configuration",
            Self::ConfigChanged => "Topic configuration changed",
            Self::Unavailable => "Kafka topic administration is unavailable",
        };
        f.write_str(message)
    }
}

impl std::error::Error for TopicAdminError {}

// 在删除 Topic 进入原生调用前依次校验名称、内部 Topic 保护和完整确认名，限制不可恢复删除。
pub fn validate_delete(topic: &str, confirmation_name: &str) -> Result<(), TopicAdminError> {
    let mut errors = BTreeMap::new();
    // 名称合法后才检查内部保护与完整确认，确保删除入口沿同一安全顺序拒绝非法目标。
    validate_topic_name(topic, "name", &mut errors);
    if !errors.is_empty() {
        return Err(TopicAdminError::Validation(errors));
    }
    if topic.starts_with("__") {
        return Err(TopicAdminError::InternalTopicProtected);
    }
    if topic != confirmation_name {
        errors.insert(
            "confirmationName".into(),
            "Confirmation name must exactly match the topic name".into(),
        );
        return Err(TopicAdminError::Validation(errors));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        CreateTopicInput, TOPIC_CONFIG_KEYS, TopicAdminError, TopicConfigSource,
        TopicConfigUpdateOutcome, TopicConfigValue, UpdateTopicConfigsInput, validate_delete,
    };

    // 在 Topic 管理领域测试中构造最小合法创建请求，便于独立修改待测字段。
    fn create_input(name: &str) -> CreateTopicInput {
        CreateTopicInput {
            name: name.into(),
            partitions: 1,
            replication_factor: None,
            configs: BTreeMap::new(),
        }
    }

    #[test]
    // 在领域回归中验证合法名称及字符和长度边界，阻止无效名称进入 Kafka。
    fn accepts_valid_topic_names_and_rejects_invalid_names() {
        for name in ["orders.v1", "_private-topic", "UPPER9"] {
            assert!(create_input(name).validate().is_ok(), "{name}");
        }

        let cases = [
            ("", "Invalid topic name"),
            (".", "Invalid topic name"),
            ("..", "Invalid topic name"),
            ("orders\0v1", "Invalid topic name"),
            ("orders/backup", "Invalid topic name"),
            (&"a".repeat(250), "Invalid topic name"),
        ];
        for (name, expected) in cases {
            assert_eq!(create_input(name).validate().unwrap_err()["name"], expected);
        }
        assert!(create_input(&"a".repeat(249)).validate().is_ok());
    }

    #[test]
    // 在领域回归中验证分区数和副本数必须为正，防止非法原生创建参数。
    fn rejects_non_positive_partition_and_replication_counts() {
        for partitions in [0, -1] {
            let mut input = create_input("orders");
            input.partitions = partitions;
            assert_eq!(
                input.validate().unwrap_err()["partitions"],
                "Partitions must be greater than zero"
            );
        }
        for replication_factor in [Some(0), Some(-1)] {
            let mut input = create_input("orders");
            input.replication_factor = replication_factor;
            assert_eq!(
                input.validate().unwrap_err()["replicationFactor"],
                "Replication factor must be greater than zero"
            );
        }
    }

    // Catches rejecting the supported native upper boundaries before contacting Kafka.
    #[test]
    // 在领域回归中验证本地支持的原生数量上界可被接受。
    fn accepts_creation_counts_at_native_upper_boundaries() {
        let mut input = create_input("orders");
        input.partitions = 100_000;
        input.replication_factor = Some(10_000);
        assert!(input.validate().is_ok());
    }

    // Catches passing locally unsupported counts into AdminOpCreation instead of validation.
    #[test]
    // 在领域回归中验证超出本地原生上限的分区数在连接 Kafka 前被拒绝。
    fn rejects_partition_counts_above_native_limit() {
        for partitions in [100_001, i32::MAX] {
            let mut input = create_input("orders");
            input.partitions = partitions;
            assert_eq!(
                input.validate().unwrap_err(),
                BTreeMap::from([(
                    "partitions".into(),
                    "Partitions must not exceed 100000".into()
                )])
            );
        }
    }

    #[test]
    // 在领域回归中验证超出本地原生上限的副本数在连接 Kafka 前被拒绝。
    fn rejects_replication_counts_above_native_limit() {
        for factor in [10_001, i32::MAX] {
            let mut input = create_input("orders");
            input.replication_factor = Some(factor);
            assert_eq!(
                input.validate().unwrap_err(),
                BTreeMap::from([(
                    "replicationFactor".into(),
                    "Replication factor must not exceed 10000".into()
                )])
            );
        }
    }

    // Catches defaults or unknown-field acceptance changing the HTTP creation contract.
    #[test]
    // 在领域回归中验证创建请求默认值和拒绝未知字段的公开协议。
    fn deserializes_creation_defaults_and_rejects_unknown_fields() {
        let input: CreateTopicInput =
            serde_json::from_value(serde_json::json!({"name":"orders"})).unwrap();
        assert_eq!(input.name, "orders");
        assert_eq!(input.partitions, 1);
        assert_eq!(input.replication_factor, None);
        assert!(input.configs.is_empty());
        assert!(input.validate().is_ok());
        assert!(
            serde_json::from_value::<CreateTopicInput>(serde_json::json!({
                "name":"orders", "unexpected":true
            }))
            .is_err()
        );
    }

    #[test]
    // 在领域回归中验证八个白名单参数均可使用，其他配置不能透传。
    fn accepts_each_topic_config_key_and_rejects_unknown_configs() {
        let valid_values = [
            ("retention.ms", "-1"),
            ("retention.bytes", "0"),
            ("cleanup.policy", "delete,compact"),
            ("segment.ms", "1"),
            ("segment.bytes", "1"),
            ("min.insync.replicas", "1"),
            ("max.message.bytes", "1"),
            ("compression.type", "zstd"),
        ];
        assert_eq!(
            TOPIC_CONFIG_KEYS,
            [
                "retention.ms",
                "retention.bytes",
                "cleanup.policy",
                "segment.ms",
                "segment.bytes",
                "min.insync.replicas",
                "max.message.bytes",
                "compression.type",
            ]
        );
        for (key, value) in valid_values {
            let mut input = create_input("orders");
            input.configs.insert(key.into(), value.into());
            assert!(input.validate().is_ok(), "{key}={value}");
        }

        let mut input = create_input("orders");
        input
            .configs
            .insert("unclean.leader.election.enable".into(), "true".into());
        assert_eq!(
            input.validate().unwrap_err()["configs"],
            "Unsupported topic configuration"
        );
    }

    #[test]
    // 在领域回归中验证各配置值域及错误脱敏，防止非法值或秘密回显。
    fn validates_topic_config_value_domains_without_echoing_values() {
        let invalid_values = [
            ("retention.ms", "-2"),
            ("retention.bytes", "-2"),
            ("retention.ms", "+1"),
            ("segment.ms", "0"),
            ("segment.bytes", "-1"),
            ("min.insync.replicas", "1.5"),
            ("max.message.bytes", "9223372036854775808"),
            ("cleanup.policy", "compact,delete"),
            ("compression.type", "brotli"),
        ];
        for (key, value) in invalid_values {
            let mut input = create_input("orders");
            input.configs.insert(key.into(), value.into());
            let errors = input.validate().unwrap_err();
            assert_eq!(
                errors[&format!("configs.{key}")],
                "Invalid topic configuration value"
            );
            assert!(!errors.values().any(|message| message.contains(value)));
        }
        for (key, value) in [
            ("cleanup.policy", "delete"),
            ("cleanup.policy", "compact"),
            ("compression.type", "producer"),
            ("compression.type", "gzip"),
            ("compression.type", "snappy"),
            ("compression.type", "lz4"),
            ("compression.type", "uncompressed"),
            ("retention.ms", "0"),
        ] {
            let mut input = create_input("orders");
            input.configs.insert(key.into(), value.into());
            assert!(input.validate().is_ok(), "{key}={value}");
        }
    }

    #[test]
    // 在领域回归中验证修改非空、预期值齐全且键均受白名单约束。
    fn validates_non_empty_update_changes_and_expected_values() {
        let empty = UpdateTopicConfigsInput {
            changes: BTreeMap::new(),
            expected_values: BTreeMap::new(),
        };
        assert_eq!(
            empty.validate().unwrap_err()["changes"],
            "At least one topic configuration change is required"
        );

        let missing_expected = UpdateTopicConfigsInput {
            changes: BTreeMap::from([("retention.ms".into(), Some("60000".into()))]),
            expected_values: BTreeMap::new(),
        };
        assert_eq!(
            missing_expected.validate().unwrap_err()["expectedValues.retention.ms"],
            "Expected current value is required"
        );

        let unsupported_change = UpdateTopicConfigsInput {
            changes: BTreeMap::from([("ssl.keystore.password".into(), Some("secret".into()))]),
            expected_values: BTreeMap::from([("ssl.keystore.password".into(), None)]),
        };
        assert_eq!(
            unsupported_change.validate().unwrap_err()["changes"],
            "Unsupported topic configuration"
        );

        let unsupported_expected = UpdateTopicConfigsInput {
            changes: BTreeMap::from([("retention.ms".into(), None)]),
            expected_values: BTreeMap::from([
                ("retention.ms".into(), None),
                ("ssl.keystore.password".into(), None),
            ]),
        };
        assert_eq!(
            unsupported_expected.validate().unwrap_err()["expectedValues"],
            "Unsupported topic configuration"
        );

        let valid = UpdateTopicConfigsInput {
            changes: BTreeMap::from([("retention.ms".into(), None)]),
            expected_values: BTreeMap::from([("retention.ms".into(), Some("legacy".into()))]),
        };
        assert!(valid.validate().is_ok());
    }

    #[test]
    // 在领域回归中验证内部 Topic 保护和大小写完全一致的删除确认。
    fn protects_internal_topics_and_requires_exact_delete_confirmation() {
        let error = validate_delete("__consumer_offsets", "__consumer_offsets").unwrap_err();
        assert_eq!(error.code(), "INTERNAL_TOPIC_PROTECTED");
        assert_eq!(
            validate_delete("orders", "payments")
                .unwrap_err()
                .validation_errors()["confirmationName"],
            "Confirmation name must exactly match the topic name"
        );
        assert!(validate_delete("orders", "orders").is_ok());
    }

    #[test]
    // 在领域回归中验证配置来源、只读标志和已应用未确认结果的稳定序列化。
    fn serializes_stable_topic_config_source_values() {
        assert_eq!(
            serde_json::to_string(&TopicConfigSource::TopicOverride).unwrap(),
            "\"topicOverride\""
        );
        assert_eq!(
            serde_json::to_string(&TopicConfigSource::BrokerDefault).unwrap(),
            "\"brokerDefault\""
        );
        assert_eq!(
            serde_json::to_string(&TopicConfigSource::Unknown).unwrap(),
            "\"unknown\""
        );
        let value = TopicConfigValue {
            key: "retention.ms".into(),
            value: Some("60000".into()),
            source: TopicConfigSource::TopicOverride,
            read_only: false,
        };
        assert_eq!(
            serde_json::to_value(value).unwrap(),
            serde_json::json!({
                "key": "retention.ms", "value": "60000", "source": "topicOverride", "readOnly": false
            })
        );
        assert_eq!(
            serde_json::to_value(TopicConfigUpdateOutcome::AppliedButUnconfirmed).unwrap(),
            serde_json::json!("appliedButUnconfirmed")
        );
    }

    #[test]
    // 在领域回归中验证管理错误码及安全消息，防止底层敏感内容外泄。
    fn exposes_stable_safe_topic_admin_error_codes() {
        let cases = [
            (TopicAdminError::AlreadyExists, "TOPIC_ALREADY_EXISTS"),
            (TopicAdminError::NotFound, "TOPIC_NOT_FOUND"),
            (TopicAdminError::Authorization, "TOPIC_AUTHORIZATION_FAILED"),
            (TopicAdminError::Timeout, "TOPIC_OPERATION_TIMEOUT"),
            (TopicAdminError::InvalidConfig, "INVALID_TOPIC_CONFIG"),
            (TopicAdminError::ConfigChanged, "TOPIC_CONFIG_CHANGED"),
            (TopicAdminError::Unavailable, "KAFKA_UNAVAILABLE"),
        ];
        for (error, code) in cases {
            assert_eq!(error.code(), code);
            assert!(!error.to_string().contains("secret"));
        }
    }
}
