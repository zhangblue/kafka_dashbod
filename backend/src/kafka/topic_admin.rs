use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};

use rdkafka::{
    admin::{
        AdminClient, AdminOptions, ConfigEntry, ConfigSource, NewTopic, TopicReplication,
        TopicResult,
    },
    error::RDKafkaErrorCode,
};

use crate::domain::topic_admin::{
    CreateTopicInput, TOPIC_CONFIG_KEYS, TopicAdminError, TopicConfigSource,
    TopicConfigUpdateOutcome, TopicConfigValue, UpdateTopicConfigsInput, validate_delete,
};
use crate::sessions::manager::SessionContext;

/// Uses a session's existing client. The session owner must await the complete
/// operation, including blocking native work, before releasing its client.
pub struct TopicAdminClient {
    admin: Arc<AdminClient<SessionContext>>,
    timeout: Duration,
}

impl TopicAdminClient {
    pub fn new(admin: Arc<AdminClient<SessionContext>>, timeout: Duration) -> Self {
        Self { admin, timeout }
    }

    pub async fn create(&self, input: &CreateTopicInput) -> Result<(), TopicAdminError> {
        input.validate().map_err(TopicAdminError::Validation)?;
        let topic = new_topic(input);
        let options = self.options();
        let results = self
            .admin
            .create_topics([&topic], &options)
            .await
            .map_err(map_error)?;
        map_topic_result(&input.name, results)
    }

    pub async fn delete(&self, topic: &str) -> Result<(), TopicAdminError> {
        validate_delete(topic, topic)?;
        let options = self.options();
        let results = self
            .admin
            .delete_topics(&[topic], &options)
            .await
            .map_err(map_error)?;
        map_topic_result(topic, results)
    }

    pub async fn describe(&self, topic: &str) -> Result<Vec<TopicConfigValue>, TopicAdminError> {
        let admin = self.admin.clone();
        let topic = topic.to_owned();
        let timeout = self.timeout;
        tokio::task::spawn_blocking(move || native::describe_topic(admin, topic, timeout))
            .await
            .map_err(|_| TopicAdminError::Unavailable)?
    }

    pub async fn update(
        &self,
        topic: &str,
        input: &UpdateTopicConfigsInput,
    ) -> Result<TopicConfigUpdateOutcome, TopicAdminError> {
        input.validate().map_err(TopicAdminError::Validation)?;
        update_with(
            input,
            || self.describe(topic),
            || async {
                let admin = self.admin.clone();
                let topic = topic.to_owned();
                let changes = input
                    .changes
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                let timeout = self.timeout;
                tokio::task::spawn_blocking(move || {
                    native::incremental_alter_topic(admin, topic, changes, timeout)
                })
                .await
                .map_err(|_| TopicAdminError::Unavailable)?
            },
        )
        .await
    }

    fn options(&self) -> AdminOptions {
        AdminOptions::new()
            .request_timeout(Some(self.timeout))
            .operation_timeout(Some(self.timeout))
    }
}

fn new_topic(input: &CreateTopicInput) -> NewTopic<'_> {
    let mut topic = NewTopic::new(
        &input.name,
        input.partitions,
        TopicReplication::Fixed(input.replication_factor.unwrap_or(-1)),
    );
    for (key, value) in &input.configs {
        topic = topic.set(key, value);
    }
    topic
}

async fn update_with<D, DF, A, AF>(
    input: &UpdateTopicConfigsInput,
    mut describe: D,
    alter: A,
) -> Result<TopicConfigUpdateOutcome, TopicAdminError>
where
    D: FnMut() -> DF,
    DF: Future<Output = Result<Vec<TopicConfigValue>, TopicAdminError>>,
    A: FnOnce() -> AF,
    AF: Future<Output = Result<(), TopicAdminError>>,
{
    let current = describe().await?;
    let values: BTreeMap<_, _> = current
        .iter()
        .map(|entry| (entry.key.as_str(), &entry.value))
        .collect();
    if input
        .expected_values
        .iter()
        .any(|(key, expected)| values.get(key.as_str()).copied() != Some(expected))
    {
        return Err(TopicAdminError::ConfigChanged);
    }
    alter().await?;
    Ok(match describe().await {
        Ok(values) => TopicConfigUpdateOutcome::Confirmed(values),
        Err(_) => TopicConfigUpdateOutcome::AppliedButUnconfirmed,
    })
}

fn map_error(error: rdkafka::error::KafkaError) -> TopicAdminError {
    error
        .rdkafka_error_code()
        .map(map_code)
        .unwrap_or(TopicAdminError::Unavailable)
}

fn check_resource_result(
    expected: &str,
    actual: &str,
    code: RDKafkaErrorCode,
) -> Result<(), TopicAdminError> {
    if expected != actual {
        return Err(TopicAdminError::Unavailable);
    }
    if code == RDKafkaErrorCode::NoError {
        Ok(())
    } else {
        Err(map_code(code))
    }
}

fn map_entries(entries: Vec<ConfigEntry>) -> Vec<TopicConfigValue> {
    entries
        .into_iter()
        .filter(|entry| TOPIC_CONFIG_KEYS.contains(&entry.name.as_str()) && !entry.is_sensitive)
        .map(|entry| TopicConfigValue {
            key: entry.name,
            value: entry.value,
            source: match entry.source {
                ConfigSource::DynamicTopic => TopicConfigSource::TopicOverride,
                ConfigSource::Unknown => TopicConfigSource::Unknown,
                _ => TopicConfigSource::BrokerDefault,
            },
            read_only: entry.is_read_only,
        })
        .collect()
}

fn map_code(code: RDKafkaErrorCode) -> TopicAdminError {
    match code {
        RDKafkaErrorCode::TopicAlreadyExists => TopicAdminError::AlreadyExists,
        RDKafkaErrorCode::UnknownTopicOrPartition => TopicAdminError::NotFound,
        RDKafkaErrorCode::TopicAuthorizationFailed
        | RDKafkaErrorCode::ClusterAuthorizationFailed
        | RDKafkaErrorCode::GroupAuthorizationFailed
        | RDKafkaErrorCode::Authentication
        | RDKafkaErrorCode::SaslAuthenticationFailed => TopicAdminError::Authorization,
        RDKafkaErrorCode::OperationTimedOut
        | RDKafkaErrorCode::RequestTimedOut
        | RDKafkaErrorCode::TimedOutQueue => TopicAdminError::Timeout,
        RDKafkaErrorCode::InvalidConfig
        | RDKafkaErrorCode::InvalidPartitions
        | RDKafkaErrorCode::InvalidReplicationFactor
        | RDKafkaErrorCode::InvalidArgument => TopicAdminError::InvalidConfig,
        _ => TopicAdminError::Unavailable,
    }
}

fn map_topic_result(topic: &str, results: Vec<TopicResult>) -> Result<(), TopicAdminError> {
    if results.len() != 1 {
        return Err(TopicAdminError::Unavailable);
    }
    match results.into_iter().next().expect("one result") {
        Ok(name) if name == topic => Ok(()),
        Err((name, code)) if name == topic => Err(map_code(code)),
        _ => Err(TopicAdminError::Unavailable),
    }
}

mod native {
    use super::{check_resource_result, map_code, map_entries};
    use crate::{
        domain::topic_admin::{TOPIC_CONFIG_KEYS, TopicAdminError, TopicConfigValue},
        sessions::manager::SessionContext,
    };
    use rdkafka::admin::{AdminClient, ConfigEntry, ConfigSource};
    use rdkafka_sys as ffi;
    use std::{
        ffi::{CStr, CString},
        ptr::{self, NonNull},
        sync::Arc,
        time::Duration,
    };

    /// Unique native ownership; the matching deallocator runs on every exit path.
    struct Owned<T> {
        ptr: NonNull<T>,
        destroy: unsafe extern "C" fn(*mut T),
    }

    impl<T> Owned<T> {
        /// # Safety
        /// A non-null pointer transfers unique ownership of an initialized native
        /// allocation. `destroy` must be its matching deallocator. All borrowed
        /// native handles must outlive this owner, and no references may outlive it.
        unsafe fn from_raw(
            ptr: *mut T,
            destroy: unsafe extern "C" fn(*mut T),
        ) -> Result<Self, TopicAdminError> {
            Ok(Self {
                ptr: NonNull::new(ptr).ok_or(TopicAdminError::Unavailable)?,
                destroy,
            })
        }

        fn ptr(&self) -> *mut T {
            self.ptr.as_ptr()
        }
    }

    impl<T> Drop for Owned<T> {
        fn drop(&mut self) {
            // SAFETY: from_raw transfers one uniquely owned non-null allocation
            // with its matching destructor. Borrowed views are scoped to the owner,
            // which is dropped before the still-live AdminClient, exactly once.
            unsafe {
                (self.destroy)(self.ptr());
            }
        }
    }

    type Queue = Owned<ffi::rd_kafka_queue_t>;
    type Resource = Owned<ffi::rd_kafka_ConfigResource_t>;
    type Options = Owned<ffi::rd_kafka_AdminOptions_t>;
    type Event = Owned<ffi::rd_kafka_event_t>;
    type NativeError = Owned<ffi::rd_kafka_error_t>;

    enum Operation {
        Describe,
        Incremental,
    }

    enum IncrementalChange<'a> {
        Set(&'a str, &'a str),
        Delete(&'a str),
    }

    impl IncrementalChange<'_> {
        fn prepare(
            &self,
        ) -> Result<(CString, ffi::rd_kafka_AlterConfigOpType_t, Option<CString>), TopicAdminError>
        {
            use ffi::rd_kafka_AlterConfigOpType_t::*;
            let (key, operation, value) = match self {
                Self::Set(key, value) => (*key, RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET, Some(*value)),
                Self::Delete(key) => (*key, RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE, None),
            };
            Ok((cstring(key)?, operation, value.map(cstring).transpose()?))
        }
    }

    fn cstring(value: &str) -> Result<CString, TopicAdminError> {
        CString::new(value).map_err(|_| TopicAdminError::InvalidConfig)
    }

    fn topic_resource(topic: &str) -> Result<Resource, TopicAdminError> {
        let name = cstring(topic)?;
        // SAFETY: name is a live NUL-terminated CString, copied by the constructor.
        // The returned allocation transfers unique ownership directly into Resource,
        // which releases it using ConfigResource_destroy on every return path.
        unsafe {
            Resource::from_raw(
                ffi::rd_kafka_ConfigResource_new(
                    ffi::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC,
                    name.as_ptr(),
                ),
                ffi::rd_kafka_ConfigResource_destroy,
            )
        }
    }

    fn add_change(
        resource: &Resource,
        change: IncrementalChange<'_>,
    ) -> Result<(), TopicAdminError> {
        let (key, operation, value) = change.prepare()?;
        // SAFETY: resource exclusively owns a live ConfigResource. The C API copies
        // key/value during this call; both CStrings outlive it. DELETE alone uses a
        // null value. A returned error is caller-owned and immediately RAII-wrapped.
        unsafe {
            let error = ffi::rd_kafka_ConfigResource_add_incremental_config(
                resource.ptr(),
                key.as_ptr(),
                operation,
                value.as_ref().map_or(ptr::null(), |value| value.as_ptr()),
            );
            if !error.is_null() {
                let error = NativeError::from_raw(error, ffi::rd_kafka_error_destroy)?;
                return Err(map_code(ffi::rd_kafka_error_code(error.ptr()).into()));
            }
        }
        Ok(())
    }

    /// Only called from spawn_blocking; client ownership survives every native
    /// allocation, request and response. No native pointer crosses the boundary.
    pub(super) fn incremental_alter_topic(
        admin: Arc<AdminClient<SessionContext>>,
        topic: String,
        changes: Vec<(String, Option<String>)>,
        timeout: Duration,
    ) -> Result<(), TopicAdminError> {
        let resource = topic_resource(&topic)?;
        for (key, value) in &changes {
            add_change(
                &resource,
                match value {
                    Some(value) => IncrementalChange::Set(key, value),
                    None => IncrementalChange::Delete(key),
                },
            )?;
        }
        let event = execute(&admin, &resource, timeout, Operation::Incremental)?;
        read_result(&event, &topic, Operation::Incremental).map(|_| ())
    }

    // rust-rdkafka 0.39's DescribeConfigsFuture ignores ConfigResource_error.
    // Decode the native result here so per-topic authorization/not-found errors
    // cannot turn into an apparently successful empty configuration response.
    pub(super) fn describe_topic(
        admin: Arc<AdminClient<SessionContext>>,
        topic: String,
        timeout: Duration,
    ) -> Result<Vec<TopicConfigValue>, TopicAdminError> {
        let resource = topic_resource(&topic)?;
        let event = execute(&admin, &resource, timeout, Operation::Describe)?;
        read_result(&event, &topic, Operation::Describe).map(map_entries)
    }

    fn execute(
        admin: &AdminClient<SessionContext>,
        resource: &Resource,
        timeout: Duration,
        operation: Operation,
    ) -> Result<Event, TopicAdminError> {
        let timeout_ms =
            i32::try_from(timeout.as_millis()).map_err(|_| TopicAdminError::InvalidConfig)?;
        let api = match operation {
            Operation::Describe => ffi::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_DESCRIBECONFIGS,
            Operation::Incremental => {
                ffi::rd_kafka_admin_op_t::RD_KAFKA_ADMIN_OP_INCREMENTALALTERCONFIGS
            }
        };
        // SAFETY: admin and resource are borrowed live owners for the whole call.
        // Queue/options constructors transfer unique ownership to matching RAII
        // destructors immediately. The request copies resource/options; their input
        // array is valid for the call. Poll transfers ownership of its event. The
        // caller retains admin until after that event is decoded and destroyed.
        unsafe {
            let client = admin.inner().native_ptr();
            let queue =
                Queue::from_raw(ffi::rd_kafka_queue_new(client), ffi::rd_kafka_queue_destroy)?;
            let options = Options::from_raw(
                ffi::rd_kafka_AdminOptions_new(client, api),
                ffi::rd_kafka_AdminOptions_destroy,
            )?;
            let mut error_buffer = [0; 512];
            let code = ffi::rd_kafka_AdminOptions_set_request_timeout(
                options.ptr(),
                timeout_ms,
                error_buffer.as_mut_ptr(),
                error_buffer.len(),
            );
            if code != ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(map_code(code.into()));
            }
            let mut resources = [resource.ptr()];
            match operation {
                Operation::Describe => ffi::rd_kafka_DescribeConfigs(
                    client,
                    resources.as_mut_ptr(),
                    1,
                    options.ptr(),
                    queue.ptr(),
                ),
                Operation::Incremental => ffi::rd_kafka_IncrementalAlterConfigs(
                    client,
                    resources.as_mut_ptr(),
                    1,
                    options.ptr(),
                    queue.ptr(),
                ),
            }
            let event = ffi::rd_kafka_queue_poll(queue.ptr(), timeout_ms);
            if event.is_null() {
                return Err(TopicAdminError::Timeout);
            }
            Event::from_raw(event, ffi::rd_kafka_event_destroy)
        }
    }

    fn read_result(
        event: &Event,
        topic: &str,
        operation: Operation,
    ) -> Result<Vec<ConfigEntry>, TopicAdminError> {
        // SAFETY: the borrowed event owns all result/resource/entry arrays and C
        // strings below; librdkafka guarantees their initialized, aligned storage
        // until event destruction. Counts, null pointers, type and resource identity
        // are checked before dereferencing. Strings are copied to owned Rust values;
        // no borrowed view or native pointer escapes, and only Event frees results.
        unsafe {
            let code = ffi::rd_kafka_event_error(event.ptr());
            if code != ffi::rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                return Err(map_code(code.into()));
            }
            let mut count = 0;
            let resources = match operation {
                Operation::Describe => {
                    let result = ffi::rd_kafka_event_DescribeConfigs_result(event.ptr());
                    if result.is_null() {
                        return Err(TopicAdminError::Unavailable);
                    }
                    ffi::rd_kafka_DescribeConfigs_result_resources(result, &mut count)
                }
                Operation::Incremental => {
                    let result = ffi::rd_kafka_event_IncrementalAlterConfigs_result(event.ptr());
                    if result.is_null() {
                        return Err(TopicAdminError::Unavailable);
                    }
                    ffi::rd_kafka_IncrementalAlterConfigs_result_resources(result, &mut count)
                }
            };
            if count != 1 || resources.is_null() {
                return Err(TopicAdminError::Unavailable);
            }
            let resource = *resources;
            if resource.is_null()
                || ffi::rd_kafka_ConfigResource_type(resource)
                    != ffi::rd_kafka_ResourceType_t::RD_KAFKA_RESOURCE_TOPIC
            {
                return Err(TopicAdminError::Unavailable);
            }
            let name = ffi::rd_kafka_ConfigResource_name(resource);
            if name.is_null() {
                return Err(TopicAdminError::Unavailable);
            }
            let name = CStr::from_ptr(name)
                .to_str()
                .map_err(|_| TopicAdminError::Unavailable)?;
            check_resource_result(
                topic,
                name,
                ffi::rd_kafka_ConfigResource_error(resource).into(),
            )?;
            if matches!(operation, Operation::Incremental) {
                return Ok(Vec::new());
            }
            let entries = ffi::rd_kafka_ConfigResource_configs(resource, &mut count);
            if count == 0 {
                return Ok(Vec::new());
            }
            if entries.is_null()
                || count
                    > isize::MAX as usize
                        / std::mem::size_of::<*const ffi::rd_kafka_ConfigEntry_t>()
            {
                return Err(TopicAdminError::Unavailable);
            }
            let mut result = Vec::new();
            for &entry in std::slice::from_raw_parts(entries, count) {
                if entry.is_null() {
                    return Err(TopicAdminError::Unavailable);
                }
                let name = ffi::rd_kafka_ConfigEntry_name(entry);
                if name.is_null() {
                    return Err(TopicAdminError::Unavailable);
                }
                let name = CStr::from_ptr(name)
                    .to_str()
                    .map_err(|_| TopicAdminError::Unavailable)?;
                if !TOPIC_CONFIG_KEYS.contains(&name)
                    || ffi::rd_kafka_ConfigEntry_is_sensitive(entry) != 0
                {
                    continue;
                }
                let value = ffi::rd_kafka_ConfigEntry_value(entry);
                use ffi::rd_kafka_ConfigSource_t::*;
                let source = match ffi::rd_kafka_ConfigEntry_source(entry) {
                    RD_KAFKA_CONFIG_SOURCE_DYNAMIC_TOPIC_CONFIG => ConfigSource::DynamicTopic,
                    RD_KAFKA_CONFIG_SOURCE_DYNAMIC_BROKER_CONFIG => ConfigSource::DynamicBroker,
                    RD_KAFKA_CONFIG_SOURCE_DYNAMIC_DEFAULT_BROKER_CONFIG => {
                        ConfigSource::DynamicDefaultBroker
                    }
                    RD_KAFKA_CONFIG_SOURCE_STATIC_BROKER_CONFIG => ConfigSource::StaticBroker,
                    RD_KAFKA_CONFIG_SOURCE_DEFAULT_CONFIG => ConfigSource::Default,
                    _ => ConfigSource::Unknown,
                };
                result.push(ConfigEntry {
                    name: name.to_owned(),
                    value: if value.is_null() {
                        None
                    } else {
                        Some(
                            CStr::from_ptr(value)
                                .to_str()
                                .map_err(|_| TopicAdminError::Unavailable)?
                                .to_owned(),
                        )
                    },
                    source,
                    is_read_only: ffi::rd_kafka_ConfigEntry_is_read_only(entry) != 0,
                    is_default: ffi::rd_kafka_ConfigEntry_is_default(entry) != 0,
                    is_sensitive: false,
                });
            }
            Ok(result)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{IncrementalChange, Owned, describe_topic, incremental_alter_topic};
        use crate::{domain::topic_admin::TopicAdminError, sessions::manager::SessionContext};
        use rdkafka::{ClientConfig, admin::AdminClient};
        use rdkafka_sys as ffi;
        use std::{
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering},
            },
            time::Duration,
        };

        // Catches swapping SET/DELETE or passing a non-null value for restore-default.
        #[test]
        fn prepares_set_and_delete_with_distinct_native_operations() {
            let (key, operation, value) = IncrementalChange::Set("retention.ms", "60000")
                .prepare()
                .unwrap();
            assert_eq!(key.to_bytes(), b"retention.ms");
            assert_eq!(
                operation,
                ffi::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET
            );
            assert_eq!(value.unwrap().to_bytes(), b"60000");
            let (_, operation, value) =
                IncrementalChange::Delete("retention.ms").prepare().unwrap();
            assert_eq!(
                operation,
                ffi::rd_kafka_AlterConfigOpType_t::RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE
            );
            assert!(value.is_none());
            assert!(matches!(
                IncrementalChange::Set("retention.ms", "bad\0value").prepare(),
                Err(TopicAdminError::InvalidConfig)
            ));
        }

        // Catches leaking owned native objects on any subsequent fallible step.
        #[test]
        fn releases_owned_resources_exactly_once_on_early_return() {
            static DROPS: AtomicUsize = AtomicUsize::new(0);
            unsafe extern "C" fn destroy(ptr: *mut u8) {
                // SAFETY: the test transfers one Box allocation to each Owned;
                // Drop invokes this matching destructor once, with no live borrows.
                unsafe {
                    drop(Box::from_raw(ptr));
                }
                DROPS.fetch_add(1, Ordering::SeqCst);
            }
            let result: Result<(), TopicAdminError> = (|| {
                let mut owners = Vec::new();
                for _ in 0..4 {
                    // SAFETY: each non-null pointer is uniquely allocated by Box
                    // and the supplied destructor reconstructs exactly that Box.
                    owners.push(unsafe { Owned::from_raw(Box::into_raw(Box::new(1u8)), destroy) }?);
                }
                Err(TopicAdminError::Unavailable)
            })();
            assert_eq!(result, Err(TopicAdminError::Unavailable));
            assert_eq!(DROPS.load(Ordering::SeqCst), 4);
        }

        // Uses a real native client without any broker: proves bounded polling and
        // safe native cleanup after both query and mutation timeout/error paths.
        #[test]
        fn native_operations_are_bounded_without_a_broker() {
            let admin: Arc<AdminClient<SessionContext>> = Arc::new(
                ClientConfig::new()
                    .create_with_context(SessionContext)
                    .unwrap(),
            );
            let start = std::time::Instant::now();
            assert_eq!(
                describe_topic(admin.clone(), "orders".into(), Duration::from_millis(5)),
                Err(TopicAdminError::Timeout)
            );
            assert_eq!(
                incremental_alter_topic(
                    admin.clone(),
                    "orders".into(),
                    vec![("retention.ms".into(), None)],
                    Duration::from_millis(5)
                ),
                Err(TopicAdminError::Timeout)
            );
            assert!(start.elapsed() < Duration::from_secs(2));
            assert_eq!(
                describe_topic(admin.clone(), "bad\0topic".into(), Duration::from_millis(5)),
                Err(TopicAdminError::InvalidConfig)
            );
            assert_eq!(
                incremental_alter_topic(
                    admin,
                    "orders".into(),
                    vec![("bad\0key".into(), None)],
                    Duration::from_millis(5)
                ),
                Err(TopicAdminError::InvalidConfig)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        check_resource_result, map_code, map_entries, map_topic_result, new_topic, update_with,
    };
    use crate::domain::topic_admin::{
        CreateTopicInput, TopicConfigUpdateOutcome, UpdateTopicConfigsInput,
    };
    use crate::domain::topic_admin::{TopicAdminError, TopicConfigSource, TopicConfigValue};
    use rdkafka::{
        admin::{ConfigEntry, ConfigSource},
        error::RDKafkaErrorCode,
    };
    use std::{collections::BTreeMap, future::ready};

    fn entry(
        name: &str,
        value: Option<&str>,
        source: ConfigSource,
        read_only: bool,
        sensitive: bool,
    ) -> ConfigEntry {
        ConfigEntry {
            name: name.into(),
            value: value.map(str::to_owned),
            source,
            is_read_only: read_only,
            is_default: false,
            is_sensitive: sensitive,
        }
    }

    // Catches leaking non-allowlisted or broker-marked sensitive values.
    #[test]
    fn maps_described_entries_without_exposing_unknown_or_sensitive_values() {
        let entries = vec![
            entry(
                "retention.ms",
                Some("60000"),
                ConfigSource::DynamicTopic,
                false,
                false,
            ),
            entry(
                "ssl.keystore.password",
                Some("secret"),
                ConfigSource::Unknown,
                false,
                true,
            ),
            entry(
                "segment.ms",
                Some("secret"),
                ConfigSource::Default,
                false,
                true,
            ),
        ];
        assert_eq!(
            map_entries(entries),
            vec![TopicConfigValue {
                key: "retention.ms".into(),
                value: Some("60000".into()),
                source: TopicConfigSource::TopicOverride,
                read_only: false,
            }]
        );
    }

    // Catches source inference from is_default or losing unknown/read-only state.
    #[test]
    fn preserves_sources_read_only_and_unknown_values() {
        for (source, expected) in [
            (ConfigSource::DynamicTopic, TopicConfigSource::TopicOverride),
            (
                ConfigSource::DynamicBroker,
                TopicConfigSource::BrokerDefault,
            ),
            (
                ConfigSource::DynamicDefaultBroker,
                TopicConfigSource::BrokerDefault,
            ),
            (ConfigSource::StaticBroker, TopicConfigSource::BrokerDefault),
            (ConfigSource::Default, TopicConfigSource::BrokerDefault),
            (ConfigSource::Unknown, TopicConfigSource::Unknown),
        ] {
            assert_eq!(
                map_entries(vec![entry("retention.ms", None, source, true, false)]),
                vec![TopicConfigValue {
                    key: "retention.ms".into(),
                    value: None,
                    source: expected,
                    read_only: true,
                }]
            );
        }
    }

    // Catches exposing native strings or collapsing actionable native error categories.
    #[test]
    fn maps_native_authorization_error_to_stable_safe_error() {
        let error = map_code(RDKafkaErrorCode::TopicAuthorizationFailed);
        assert_eq!(error.code(), "TOPIC_AUTHORIZATION_FAILED");
        assert!(!error.to_string().contains("broker"));
        for (code, expected) in [
            (
                RDKafkaErrorCode::TopicAlreadyExists,
                TopicAdminError::AlreadyExists,
            ),
            (
                RDKafkaErrorCode::UnknownTopicOrPartition,
                TopicAdminError::NotFound,
            ),
            (
                RDKafkaErrorCode::OperationTimedOut,
                TopicAdminError::Timeout,
            ),
            (RDKafkaErrorCode::RequestTimedOut, TopicAdminError::Timeout),
            (
                RDKafkaErrorCode::InvalidConfig,
                TopicAdminError::InvalidConfig,
            ),
            (
                RDKafkaErrorCode::BrokerTransportFailure,
                TopicAdminError::Unavailable,
            ),
        ] {
            assert_eq!(map_code(code), expected);
        }
    }

    // Catches treating a successful request envelope as successful topic mutation.
    #[test]
    fn checks_single_topic_result_and_resource_identity() {
        assert_eq!(
            map_topic_result("orders", vec![Ok("orders".into())]),
            Ok(())
        );
        assert_eq!(
            map_topic_result(
                "orders",
                vec![Err((
                    "orders".into(),
                    RDKafkaErrorCode::TopicAuthorizationFailed
                ))]
            ),
            Err(TopicAdminError::Authorization)
        );
        for results in [
            vec![],
            vec![Ok("other".into())],
            vec![Ok("orders".into()), Ok("orders".into())],
        ] {
            assert_eq!(
                map_topic_result("orders", results),
                Err(TopicAdminError::Unavailable)
            );
        }
    }

    // Catches default replication being forced to one or dropping create overrides.
    #[test]
    fn builds_create_request_with_broker_default_replication() {
        let mut input = CreateTopicInput {
            name: "orders".into(),
            partitions: 3,
            replication_factor: None,
            configs: BTreeMap::from([("retention.ms".into(), "60000".into())]),
        };
        let topic = new_topic(&input);
        assert_eq!((topic.name, topic.num_partitions), ("orders", 3));
        assert!(matches!(
            topic.replication,
            rdkafka::admin::TopicReplication::Fixed(-1)
        ));
        assert_eq!(topic.config, vec![("retention.ms", "60000")]);
        input.replication_factor = Some(2);
        assert!(matches!(
            new_topic(&input).replication,
            rdkafka::admin::TopicReplication::Fixed(2)
        ));
    }

    // Catches the upstream DescribeConfigs bug: an envelope is OK but the resource failed.
    #[test]
    fn propagates_resource_errors_before_reading_config_entries() {
        assert_eq!(
            check_resource_result(
                "orders",
                "orders",
                RDKafkaErrorCode::TopicAuthorizationFailed
            ),
            Err(TopicAdminError::Authorization)
        );
        assert_eq!(
            check_resource_result(
                "orders",
                "orders",
                RDKafkaErrorCode::UnknownTopicOrPartition
            ),
            Err(TopicAdminError::NotFound)
        );
        assert_eq!(
            check_resource_result("orders", "other", RDKafkaErrorCode::NoError),
            Err(TopicAdminError::Unavailable)
        );
    }

    fn update_input() -> UpdateTopicConfigsInput {
        UpdateTopicConfigsInput {
            changes: BTreeMap::from([("retention.ms".into(), None)]),
            expected_values: BTreeMap::from([("retention.ms".into(), Some("60000".into()))]),
        }
    }

    fn current(value: Option<&str>) -> Vec<TopicConfigValue> {
        vec![TopicConfigValue {
            key: "retention.ms".into(),
            value: value.map(str::to_owned),
            source: TopicConfigSource::TopicOverride,
            read_only: false,
        }]
    }

    // Catches overwriting concurrently changed values or treating missing as null.
    #[tokio::test]
    async fn rejects_conflicts_and_missing_expected_keys_before_writing() {
        for values in [current(Some("70000")), vec![]] {
            let result = update_with(
                &update_input(),
                || ready(Ok(values.clone())),
                || {
                    panic!("conflict must not reach the write boundary");
                    #[allow(unreachable_code)]
                    ready(Ok(()))
                },
            )
            .await;
            assert_eq!(result, Err(TopicAdminError::ConfigChanged));
        }
    }

    // Catches incorrectly comparing untouched keys or returning stale pre-write data.
    #[tokio::test]
    async fn compares_only_expected_keys_and_returns_post_write_values() {
        let mut before = current(Some("60000"));
        before.push(TopicConfigValue {
            key: "segment.ms".into(),
            value: Some("1000".into()),
            source: TopicConfigSource::BrokerDefault,
            read_only: false,
        });
        let after = current(Some("604800000"));
        let mut reads = vec![Ok(before), Ok(after.clone())].into_iter();
        assert_eq!(
            update_with(
                &update_input(),
                || ready(reads.next().unwrap()),
                || ready(Ok(()))
            )
            .await,
            Ok(TopicConfigUpdateOutcome::Confirmed(after))
        );
    }

    // Catches reporting a successfully submitted mutation as failed when readback fails.
    #[tokio::test]
    async fn distinguishes_failed_write_from_failed_confirmation() {
        let mut reads = vec![Ok(current(Some("60000"))), Err(TopicAdminError::Timeout)].into_iter();
        assert_eq!(
            update_with(
                &update_input(),
                || ready(reads.next().unwrap()),
                || ready(Ok(()))
            )
            .await,
            Ok(TopicConfigUpdateOutcome::AppliedButUnconfirmed)
        );
        let mut reads = vec![Ok(current(Some("60000")))].into_iter();
        assert_eq!(
            update_with(
                &update_input(),
                || ready(reads.next().unwrap()),
                || ready(Err(TopicAdminError::Authorization))
            )
            .await,
            Err(TopicAdminError::Authorization)
        );
    }
}
