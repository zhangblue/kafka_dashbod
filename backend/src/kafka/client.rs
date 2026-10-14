use std::time::{Duration, Instant};

use rdkafka::{
    admin::AdminClient,
    bindings,
    client::ClientContext,
    config::RDKafkaLogLevel,
    error::{KafkaError, RDKafkaErrorCode},
};
use serde::Serialize;

use super::config::build_client_properties;
use crate::domain::cluster::{DecryptedClusterConfig, SecurityProtocol};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionError {
    #[error("Unsupported or incomplete Kafka security settings")]
    InvalidConfig,
    #[error("Kafka authentication or authorization failed")]
    Auth,
    #[error("Kafka TLS verification failed")]
    Tls,
    #[error("Kafka broker hostname could not be resolved")]
    Dns,
    #[error("Kafka connection timed out")]
    Timeout,
}

impl ConnectionError {
    pub fn code(self) -> &'static str {
        match self {
            Self::InvalidConfig => "VALIDATION_ERROR",
            Self::Auth => "KAFKA_AUTH_FAILED",
            Self::Tls => "KAFKA_TLS_FAILED",
            Self::Dns => "KAFKA_DNS_FAILED",
            Self::Timeout => "KAFKA_TIMEOUT",
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionTestResult {
    pub broker_count: usize,
    pub topic_count: usize,
    pub elapsed_ms: u64,
}

// Never forward untrusted native log/error text (it can contain security settings).
struct SafeContext;
impl ClientContext for SafeContext {
    fn log(&self, _: RDKafkaLogLevel, _: &str, _: &str) {}
    fn error(&self, _: KafkaError, _: &str) {}
    fn stats_raw(&self, _: &[u8]) {}
}

fn map_code(code: RDKafkaErrorCode) -> ConnectionError {
    match code {
        RDKafkaErrorCode::Authentication
        | RDKafkaErrorCode::SaslAuthenticationFailed
        | RDKafkaErrorCode::UnsupportedSASLMechanism
        | RDKafkaErrorCode::IllegalSASLState
        | RDKafkaErrorCode::TopicAuthorizationFailed
        | RDKafkaErrorCode::GroupAuthorizationFailed
        | RDKafkaErrorCode::ClusterAuthorizationFailed => ConnectionError::Auth,
        RDKafkaErrorCode::SSL => ConnectionError::Tls,
        RDKafkaErrorCode::Resolve => ConnectionError::Dns,
        _ => ConnectionError::Timeout,
    }
}

fn map_error(error: &KafkaError) -> ConnectionError {
    error
        .rdkafka_error_code()
        .map(map_code)
        .unwrap_or(ConnectionError::Timeout)
}

/// AdminClient's polling thread services its admin response queue only. Drain the main
/// queue's typed error events so authentication/TLS/DNS causes survive a metadata timeout.
/// No error text or native log contents cross this boundary.
fn connection_failure(client: &AdminClient<SafeContext>, error: &KafkaError) -> ConnectionError {
    let mut result = map_error(error);
    // SAFETY: the queue reference is acquired from a live client, used only here, and
    // released before the client is dropped. Each owned event is destroyed exactly once.
    unsafe {
        let queue = bindings::rd_kafka_queue_get_main(client.inner().native_ptr());
        if queue.is_null() {
            return result;
        }
        loop {
            let event = bindings::rd_kafka_queue_poll(queue, 0);
            if event.is_null() {
                break;
            }
            if bindings::rd_kafka_event_type(event) == bindings::RD_KAFKA_EVENT_ERROR {
                let category = map_code(bindings::rd_kafka_event_error(event).into());
                if result == ConnectionError::Timeout || category == ConnectionError::Auth {
                    result = category;
                }
            }
            bindings::rd_kafka_event_destroy(event);
        }
        bindings::rd_kafka_queue_destroy(queue);
    }
    result
}

/// A temporary client scoped to this request; all native work and drop finish before return.
/// This function never creates or retains a monitoring session.
pub async fn test_connection(
    config: DecryptedClusterConfig,
) -> Result<ConnectionTestResult, ConnectionError> {
    let mut properties =
        build_client_properties(&config).map_err(|_| ConnectionError::InvalidConfig)?;
    properties
        .inner
        .set("enabled_events", bindings::RD_KAFKA_EVENT_ERROR.to_string());
    let timeout = config.connection_timeout;
    let tls = matches!(
        config.security_protocol,
        SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
    );
    tokio::task::spawn_blocking(move || {
        let started = Instant::now();
        let client: AdminClient<SafeContext> = properties
            .inner
            .create_with_context(SafeContext)
            .map_err(|error| {
                if tls && error.rdkafka_error_code().is_none() {
                    ConnectionError::Tls
                } else {
                    map_error(&error)
                }
            })?;
        let result = probe(&client, timeout, started);
        drop(client);
        result
    })
    .await
    .map_err(|_| ConnectionError::Timeout)?
}

fn probe(
    client: &AdminClient<SafeContext>,
    timeout: Duration,
    started: Instant,
) -> Result<ConnectionTestResult, ConnectionError> {
    // list_groups in librdkafka is exposed as fetch_group_list by rust-rdkafka.
    let groups = client
        .inner()
        .fetch_group_list(None, timeout)
        .map_err(|error| connection_failure(client, &error))?;
    drop(groups);
    let remaining = timeout
        .checked_sub(started.elapsed())
        .filter(|v| !v.is_zero())
        .ok_or(ConnectionError::Timeout)?;
    let metadata = client
        .inner()
        .fetch_metadata(None, remaining)
        .map_err(|error| connection_failure(client, &error))?;
    if metadata.brokers().is_empty() {
        return Err(ConnectionError::Timeout);
    }
    for topic in metadata.topics() {
        if let Some(code) = topic.error() {
            return Err(map_code(code.into()));
        }
    }
    Ok(ConnectionTestResult {
        broker_count: metadata.brokers().len(),
        topic_count: metadata.topics().len(),
        elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::error::{KafkaError, RDKafkaErrorCode};

    // Returning a timeout for an authentication/TLS/DNS failure hides actionable causes.
    #[test]
    fn maps_native_error_codes_to_safe_categories() {
        for (native, expected) in [
            (RDKafkaErrorCode::Authentication, "KAFKA_AUTH_FAILED"),
            (
                RDKafkaErrorCode::SaslAuthenticationFailed,
                "KAFKA_AUTH_FAILED",
            ),
            (
                RDKafkaErrorCode::GroupAuthorizationFailed,
                "KAFKA_AUTH_FAILED",
            ),
            (RDKafkaErrorCode::SSL, "KAFKA_TLS_FAILED"),
            (RDKafkaErrorCode::Resolve, "KAFKA_DNS_FAILED"),
            (RDKafkaErrorCode::OperationTimedOut, "KAFKA_TIMEOUT"),
        ] {
            let error = map_error(&KafkaError::MetadataFetch(native));
            assert_eq!(error.code(), expected);
        }
    }

    #[test]
    fn native_client_creation_errors_never_escape() {
        let error = map_error(&KafkaError::ClientCreation(
            "test-password private-key".into(),
        ));
        assert!(!format!("{error:?} {error}").contains("test-password"));
        assert!(!format!("{error:?} {error}").contains("private-key"));
    }
}
