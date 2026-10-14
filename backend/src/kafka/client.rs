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

type GroupListDestructor = unsafe fn(*const bindings::rd_kafka_group_list);

/// Owns one native group list, independently of the API's top-level return code.
/// The destructor is paired with the allocator when ownership is transferred.
struct OwnedGroupList {
    ptr: *const bindings::rd_kafka_group_list,
    destroy: GroupListDestructor,
}

impl Drop for OwnedGroupList {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            // SAFETY: validate_group_list_result's caller transfers unique ownership
            // of this allocation and supplies its matching destructor. No references
            // escape the validator, and Drop is the only place that frees the list.
            unsafe { (self.destroy)(self.ptr) };
        }
    }
}

unsafe fn destroy_native_group_list(ptr: *const bindings::rd_kafka_group_list) {
    // SAFETY: the production caller supplies only non-null owned list pointers
    // returned by rd_kafka_list_groups, including its __PARTIAL return path.
    unsafe { bindings::rd_kafka_group_list_destroy(ptr) };
}

/// Consume a native result without discarding ownership on error or __PARTIAL.
///
/// # Safety
/// A non-null `ptr` must uniquely own a valid group-list allocation, whose groups
/// array is readable for group_cnt entries and remains alive until `destroy`.
/// `destroy` must be the matching deallocator and must not retain any references.
/// Null is permitted and never passed to the destructor.
unsafe fn validate_group_list_result(
    status: bindings::rd_kafka_resp_err_t,
    ptr: *const bindings::rd_kafka_group_list,
    destroy: GroupListDestructor,
) -> Result<(), KafkaError> {
    // Establish ownership before examining status: __PARTIAL also allocates a list.
    let owned = OwnedGroupList { ptr, destroy };
    use bindings::rd_kafka_resp_err_t::{RD_KAFKA_RESP_ERR__PARTIAL, RD_KAFKA_RESP_ERR_NO_ERROR};
    if status != RD_KAFKA_RESP_ERR_NO_ERROR && status != RD_KAFKA_RESP_ERR__PARTIAL {
        return Err(KafkaError::GroupListFetch(status.into()));
    }
    // SAFETY: the caller guarantees a valid list allocation; as_ref handles null.
    let list =
        unsafe { owned.ptr.as_ref() }.ok_or(KafkaError::GroupListFetch(RDKafkaErrorCode::Fail))?;
    let count = usize::try_from(list.group_cnt)
        .map_err(|_| KafkaError::GroupListFetch(RDKafkaErrorCode::Fail))?;
    if count > 0 {
        if list.groups.is_null()
            || count > isize::MAX as usize / std::mem::size_of::<bindings::rd_kafka_group_info>()
        {
            return Err(KafkaError::GroupListFetch(RDKafkaErrorCode::Fail));
        }
        // SAFETY: the owning result's contract guarantees a valid, aligned array
        // of count entries within one allocation. Its storage remains alive through
        // owned, size/null checks hold, and this borrowed slice never escapes.
        let groups = unsafe { std::slice::from_raw_parts(list.groups, count) };
        for group in groups {
            if group.err != RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(KafkaError::GroupListFetch(group.err.into()));
            }
        }
    }
    if status != RD_KAFKA_RESP_ERR_NO_ERROR {
        return Err(KafkaError::GroupListFetch(status.into()));
    }
    Ok(())
}

fn list_groups(client: &AdminClient<SafeContext>, timeout: Duration) -> Result<(), KafkaError> {
    let timeout_ms = i32::try_from(timeout.as_millis())
        .map_err(|_| KafkaError::GroupListFetch(RDKafkaErrorCode::InvalidArgument))?;
    let mut ptr = std::ptr::null();
    // SAFETY: the client is borrowed and alive throughout this blocking call. The
    // optional group argument is null (all groups), the out-pointer is initialized
    // to null, and timeout fits the C API. Any returned allocation is owned below.
    let status = unsafe {
        bindings::rd_kafka_list_groups(
            client.inner().native_ptr(),
            std::ptr::null(),
            &mut ptr,
            timeout_ms,
        )
    };
    // SAFETY: librdkafka returns a uniquely owned valid list on success/__PARTIAL,
    // leaving our initialized null pointer untouched on other errors. Its matching
    // native destructor is supplied, and no references escape this invocation.
    unsafe { validate_group_list_result(status, ptr, destroy_native_group_list) }
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
    list_groups(client, timeout).map_err(|error| connection_failure(client, &error))?;
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    // repr(C) keeps `list` at offset zero so the injected destructor can recover
    // the allocation while the production parser sees the real native structure.
    #[repr(C)]
    struct GroupListFixture {
        list: bindings::rd_kafka_group_list,
        groups: Box<[bindings::rd_kafka_group_info]>,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for GroupListFixture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn group_list_fixture(
        errors: &[bindings::rd_kafka_resp_err_t],
        drops: Arc<AtomicUsize>,
    ) -> *const bindings::rd_kafka_group_list {
        let mut groups: Box<[_]> = errors
            .iter()
            .map(|&err| bindings::rd_kafka_group_info {
                broker: bindings::rd_kafka_metadata_broker {
                    id: 1,
                    host: std::ptr::null_mut(),
                    port: 9092,
                },
                group: std::ptr::null_mut(),
                err,
                state: std::ptr::null_mut(),
                protocol_type: std::ptr::null_mut(),
                protocol: std::ptr::null_mut(),
                members: std::ptr::null_mut(),
                member_cnt: 0,
            })
            .collect();
        let list = bindings::rd_kafka_group_list {
            groups: groups.as_mut_ptr(),
            group_cnt: groups.len() as i32,
        };
        Box::into_raw(Box::new(GroupListFixture {
            list,
            groups,
            drops,
        }))
        .cast()
    }

    unsafe fn destroy_fixture_group_list(ptr: *const bindings::rd_kafka_group_list) {
        // SAFETY: only pointers produced by group_list_fixture reach this destructor;
        // the repr(C) first field has the same address as the unique Box allocation.
        drop(unsafe { Box::from_raw(ptr.cast_mut().cast::<GroupListFixture>()) });
    }

    // A returned non-null list is owned even when librdkafka reports __PARTIAL.
    #[test]
    fn native_group_list_releases_success_partial_and_error_results() {
        use bindings::rd_kafka_resp_err_t::*;
        for (status, expected_success) in [
            (RD_KAFKA_RESP_ERR_NO_ERROR, true),
            (RD_KAFKA_RESP_ERR__PARTIAL, false),
            (RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED, false),
        ] {
            let drops = Arc::new(AtomicUsize::new(0));
            let ptr = group_list_fixture(&[RD_KAFKA_RESP_ERR_NO_ERROR], drops.clone());
            // SAFETY: fixture storage is valid, uniquely owned, and paired with its
            // matching destructor; the parser consumes that ownership exactly once.
            let result =
                unsafe { validate_group_list_result(status, ptr, destroy_fixture_group_list) };
            assert_eq!(result.is_ok(), expected_success);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
        }
    }

    // A top-level successful response may still contain denied groups.
    #[test]
    fn native_group_list_checks_group_item_authorization_and_releases_list() {
        use bindings::rd_kafka_resp_err_t::*;
        let drops = Arc::new(AtomicUsize::new(0));
        let ptr = group_list_fixture(
            &[
                RD_KAFKA_RESP_ERR_NO_ERROR,
                RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED,
            ],
            drops.clone(),
        );
        // SAFETY: ownership and destructor both originate from group_list_fixture.
        let result = unsafe {
            validate_group_list_result(RD_KAFKA_RESP_ERR_NO_ERROR, ptr, destroy_fixture_group_list)
        };
        let error = result.expect_err("a denied group must fail the connection test");
        assert_eq!(map_error(&error).code(), "KAFKA_AUTH_FAILED");
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

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
