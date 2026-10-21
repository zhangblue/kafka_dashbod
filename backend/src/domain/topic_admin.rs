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
    pub fn validate(&self) -> Result<(), BTreeMap<String, String>> {
        let mut errors = BTreeMap::new();
        validate_topic_name(&self.name, "name", &mut errors);

        if self.partitions <= 0 {
            errors.insert(
                "partitions".into(),
                "Partitions must be greater than zero".into(),
            );
        }
        if self.replication_factor.is_some_and(|factor| factor <= 0) {
            errors.insert(
                "replicationFactor".into(),
                "Replication factor must be greater than zero".into(),
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

const fn default_partitions() -> i32 {
    1
}

fn validate_topic_name(name: &str, field: &str, errors: &mut BTreeMap<String, String>) {
    let valid_chars = name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'));
    if !(1..=249).contains(&name.len()) || !valid_chars || matches!(name, "." | "..") {
        errors.insert(field.into(), "Invalid topic name".into());
    }
}

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

    pub fn validation_errors(&self) -> &BTreeMap<String, String> {
        match self {
            Self::Validation(errors) => errors,
            _ => &EMPTY_VALIDATION_ERRORS,
        }
    }
}

static EMPTY_VALIDATION_ERRORS: BTreeMap<String, String> = BTreeMap::new();

impl fmt::Display for TopicAdminError {
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

pub fn validate_delete(topic: &str, confirmation_name: &str) -> Result<(), TopicAdminError> {
    let mut errors = BTreeMap::new();
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

    fn create_input(name: &str) -> CreateTopicInput {
        CreateTopicInput {
            name: name.into(),
            partitions: 1,
            replication_factor: None,
            configs: BTreeMap::new(),
        }
    }

    #[test]
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

    #[test]
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
        let outcome = TopicConfigUpdateOutcome::AppliedButUnconfirmed;
        assert!(matches!(
            outcome,
            TopicConfigUpdateOutcome::AppliedButUnconfirmed
        ));
    }

    #[test]
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
