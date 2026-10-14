use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use rdkafka::{
    Offset, TopicPartitionList,
    admin::AdminClient,
    bindings,
    consumer::Consumer,
    error::{KafkaError, RDKafkaErrorCode},
};
use tokio::sync::Semaphore;

use super::group_consumers::GroupConsumerCache;
use crate::{
    domain::cluster::DecryptedClusterConfig,
    sessions::manager::{KafkaSessionClient, SessionContext},
};

use crate::domain::snapshot::{
    ClusterSnapshot, ConsumerGroupTopicSnapshot, GroupPartitionSnapshot, PartialFailure,
    PartitionWatermark, TopicSnapshot,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TopicPartition {
    pub topic: String,
    pub partition: i32,
}

pub struct TopicMetadata {
    pub topic: String,
    pub partitions: Vec<i32>,
    pub metadata_complete: bool,
}

pub struct GroupMetadata {
    pub group_id: String,
    pub assigned_topics: Vec<String>,
}

pub struct CommittedOffset {
    pub partition: TopicPartition,
    pub offset: Option<i64>,
    pub error: Option<ProbeError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeError {
    Timeout,
    Query,
    Authorization,
    InvalidConfig,
}

impl ProbeError {
    pub fn failure(self, scope: &str, resource: impl Into<String>) -> PartialFailure {
        let (code, message) = match self {
            Self::Timeout => ("KAFKA_TIMEOUT", "Kafka query timed out"),
            Self::Query => ("KAFKA_QUERY_FAILED", "Kafka query failed"),
            Self::Authorization => ("KAFKA_AUTH_FAILED", "Kafka authorization failed"),
            Self::InvalidConfig => ("VALIDATION_ERROR", "Invalid Kafka client configuration"),
        };
        PartialFailure {
            scope: scope.into(),
            resource: resource.into(),
            code: code.into(),
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait KafkaProbe: Send + Sync {
    async fn topics(&self) -> Result<Vec<TopicMetadata>, ProbeError>;
    async fn watermarks(&self, topic: &str, partition: i32) -> Result<(i64, i64), ProbeError>;
    async fn groups(&self) -> Result<Vec<GroupMetadata>, ProbeError>;
    async fn committed(
        &self,
        group: &str,
        partitions: &[TopicPartition],
    ) -> Result<Vec<CommittedOffset>, ProbeError>;
    fn take_failures(&self) -> Vec<PartialFailure> {
        Vec::new()
    }
}

#[derive(Clone, Copy)]
pub struct CollectionOptions {
    pub concurrency: usize,
    pub operation_timeout: Duration,
}

impl Default for CollectionOptions {
    fn default() -> Self {
        Self {
            concurrency: 8,
            operation_timeout: Duration::from_secs(5),
        }
    }
}

async fn deadline<T>(
    duration: Duration,
    future: impl Future<Output = Result<T, ProbeError>>,
) -> Result<T, ProbeError> {
    tokio::time::timeout(duration, future)
        .await
        .unwrap_or(Err(ProbeError::Timeout))
}

pub async fn collect(
    probe: &impl KafkaProbe,
    cluster_id: &str,
    options: CollectionOptions,
) -> ClusterSnapshot {
    let concurrency = options.concurrency.max(1);
    let timeout = options.operation_timeout;
    let mut failures = Vec::new();
    let (topics, groups) = tokio::join!(
        deadline(timeout, probe.topics()),
        deadline(timeout, probe.groups())
    );
    let mut topics = topics.unwrap_or_else(|error| {
        failures.push(error.failure("topics", cluster_id));
        Vec::new()
    });
    let mut groups = groups.unwrap_or_else(|error| {
        failures.push(error.failure("groups", cluster_id));
        Vec::new()
    });
    topics.sort_by(|a, b| a.topic.cmp(&b.topic));
    topics.dedup_by(|a, b| a.topic == b.topic);
    groups.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    groups.dedup_by(|a, b| a.group_id == b.group_id);
    let consumer_group_ids = groups.iter().map(|group| group.group_id.clone()).collect();
    let partitions: Vec<_> = topics
        .iter_mut()
        .flat_map(|topic| {
            topic.partitions.sort_unstable();
            topic.partitions.dedup();
            topic
                .partitions
                .iter()
                .map(|&partition| TopicPartition {
                    topic: topic.topic.clone(),
                    partition,
                })
                .collect::<Vec<_>>()
        })
        .collect();
    let semaphore = Semaphore::new(concurrency);
    let mut watermarks = stream::iter(partitions.clone().into_iter().map(|partition| {
        let semaphore = &semaphore;
        async move {
            let _permit = semaphore
                .acquire()
                .await
                .expect("collector semaphore stays open");
            (
                partition.clone(),
                deadline(
                    timeout,
                    probe.watermarks(&partition.topic, partition.partition),
                )
                .await,
            )
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<HashMap<_, _>>()
    .await;
    let mut topic_snapshots = Vec::new();
    for topic in topics {
        let mut metrics = Vec::new();
        for partition in topic.partitions {
            let key = TopicPartition {
                topic: topic.topic.clone(),
                partition,
            };
            match watermarks
                .remove(&key)
                .expect("every discovered partition was queried")
            {
                Ok((low, high)) => {
                    metrics.push(PartitionWatermark::new(partition, low, high));
                }
                Err(error) => {
                    failures
                        .push(error.failure("watermark", format!("{}:{partition}", topic.topic)));
                    metrics.push(PartitionWatermark::unavailable(partition));
                }
            }
        }
        let mut snapshot = TopicSnapshot::from_partitions(topic.topic, metrics);
        snapshot.incomplete |= !topic.metadata_complete;
        topic_snapshots.push(snapshot);
    }
    // Query every discovered partition, including those whose watermark failed.
    let results = stream::iter(groups.into_iter().map(|group| {
        let partitions = &partitions;
        async move {
            let result = deadline(timeout, probe.committed(&group.group_id, partitions)).await;
            (group, result)
        }
    }))
    .buffer_unordered(concurrency)
    .collect::<Vec<_>>()
    .await;
    let mut consumer_groups = Vec::new();
    for (group, result) in results {
        let offsets = match result {
            Ok(offsets) => offsets,
            Err(error) => {
                failures.push(error.failure("committed", &group.group_id));
                Vec::new()
            }
        };
        let mut committed = HashMap::with_capacity(offsets.len());
        for offset in offsets {
            if let Some(error) = offset.error {
                failures.push(error.failure(
                    "committed",
                    format!(
                        "{}/{}:{}",
                        group.group_id, offset.partition.topic, offset.partition.partition
                    ),
                ));
            }
            let value = if offset.error.is_none() {
                offset.offset.filter(|value| *value >= 0)
            } else {
                None
            };
            committed.insert(offset.partition, value);
        }
        for topic in &topic_snapshots {
            if !group.assigned_topics.contains(&topic.topic)
                && !committed
                    .iter()
                    .any(|(partition, offset)| partition.topic == topic.topic && offset.is_some())
            {
                continue;
            }
            let rows = topic
                .partitions
                .iter()
                .map(|p| {
                    let offset = committed
                        .get(&TopicPartition {
                            topic: topic.topic.clone(),
                            partition: p.partition,
                        })
                        .copied()
                        .flatten();
                    GroupPartitionSnapshot::new(p.partition, p.high, offset)
                })
                .collect();
            let mut snapshot = ConsumerGroupTopicSnapshot::from_partitions(
                group.group_id.clone(),
                topic.topic.clone(),
                rows,
            );
            snapshot.incomplete |= topic.incomplete;
            consumer_groups.push(snapshot);
        }
    }
    consumer_groups.sort_by(|a, b| (&a.group_id, &a.topic).cmp(&(&b.group_id, &b.topic)));
    failures.extend(probe.take_failures());
    failures
        .sort_by(|a, b| (&a.scope, &a.resource, &a.code).cmp(&(&b.scope, &b.resource, &b.code)));
    ClusterSnapshot {
        cluster_id: cluster_id.into(),
        collected_at: chrono::Utc::now().to_rfc3339(),
        topics: topic_snapshots,
        consumer_group_ids,
        consumer_groups,
        partial_failures: failures,
    }
}

/// Tracks native tasks separately from request futures: timeout cannot cancel C calls.
pub struct NativeProbe {
    admin: Arc<AdminClient<SessionContext>>,
    cache: GroupConsumerCache,
    config: DecryptedClusterConfig,
    options: CollectionOptions,
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    permits: Arc<Semaphore>,
    failures: Arc<Mutex<Vec<PartialFailure>>>,
}

fn native_error(error: KafkaError) -> ProbeError {
    match error.rdkafka_error_code() {
        Some(
            RDKafkaErrorCode::TopicAuthorizationFailed
            | RDKafkaErrorCode::GroupAuthorizationFailed
            | RDKafkaErrorCode::ClusterAuthorizationFailed
            | RDKafkaErrorCode::SaslAuthenticationFailed
            | RDKafkaErrorCode::Authentication,
        ) => ProbeError::Authorization,
        Some(RDKafkaErrorCode::OperationTimedOut | RDKafkaErrorCode::RequestTimedOut) => {
            ProbeError::Timeout
        }
        _ => ProbeError::Query,
    }
}

// Kafka ConsumerProtocolAssignment versions 0–3 use the same non-flexible schema.
// Newer versions preserve the prefix; lengths are checked before any allocation.
fn assignment_topics(mut bytes: &[u8]) -> Result<Vec<String>, ProbeError> {
    fn take<'a>(bytes: &mut &'a [u8], count: usize) -> Result<&'a [u8], ProbeError> {
        let (value, rest) = bytes.split_at_checked(count).ok_or(ProbeError::Query)?;
        *bytes = rest;
        Ok(value)
    }
    fn short(bytes: &mut &[u8]) -> Result<i16, ProbeError> {
        Ok(i16::from_be_bytes(
            take(bytes, 2)?.try_into().map_err(|_| ProbeError::Query)?,
        ))
    }
    fn integer(bytes: &mut &[u8]) -> Result<i32, ProbeError> {
        Ok(i32::from_be_bytes(
            take(bytes, 4)?.try_into().map_err(|_| ProbeError::Query)?,
        ))
    }
    if short(&mut bytes)? < 0 {
        return Err(ProbeError::Query);
    }
    let count = usize::try_from(integer(&mut bytes)?).map_err(|_| ProbeError::Query)?;
    if count > bytes.len() / 6 {
        return Err(ProbeError::Query);
    }
    let mut topics = Vec::new();
    for _ in 0..count {
        let length = usize::try_from(short(&mut bytes)?).map_err(|_| ProbeError::Query)?;
        let topic =
            std::str::from_utf8(take(&mut bytes, length)?).map_err(|_| ProbeError::Query)?;
        let partitions = usize::try_from(integer(&mut bytes)?).map_err(|_| ProbeError::Query)?;
        let array = take(
            &mut bytes,
            partitions.checked_mul(4).ok_or(ProbeError::Query)?,
        )?;
        if array
            .as_chunks::<4>()
            .0
            .iter()
            .any(|value| i32::from_be_bytes(*value) < 0)
        {
            return Err(ProbeError::Query);
        }
        if partitions > 0 && !topic.is_empty() {
            topics.push(topic.to_owned());
        }
    }
    match integer(&mut bytes)? {
        -1 => {}
        value if value >= 0 => {
            take(&mut bytes, value as usize)?;
        }
        _ => return Err(ProbeError::Query),
    }
    topics.sort();
    topics.dedup();
    Ok(topics)
}

impl NativeProbe {
    pub fn new(client: &KafkaSessionClient, options: CollectionOptions) -> Self {
        Self {
            admin: client.admin.clone(),
            cache: client.group_consumers.clone(),
            config: client.config.clone(),
            options,
            workers: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(options.concurrency.max(1))),
            failures: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        work: impl FnOnce() -> Result<T, ProbeError> + Send + 'static,
    ) -> Result<T, ProbeError> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ProbeError::Query)?;
        let (send, receive) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let _ = send.send(work());
        });
        self.workers
            .lock()
            .expect("worker registry poisoned")
            .push(worker);
        receive.await.unwrap_or(Err(ProbeError::Query))
    }

    /// Must finish while the caller still holds its session client/collection locks.
    pub async fn finish(&self) {
        let workers = std::mem::take(&mut *self.workers.lock().expect("worker registry poisoned"));
        for worker in workers {
            let _ = worker.await;
        }
    }
}

// rust-rdkafka 0.39 discards __PARTIAL and hides per-group errors. Own the native list
// immediately so partial results are retained and every native allocation is released.
fn discover_groups(
    admin: &AdminClient<SessionContext>,
    timeout: Duration,
    failures: &Mutex<Vec<PartialFailure>>,
) -> Result<Vec<GroupMetadata>, ProbeError> {
    struct List(*const bindings::rd_kafka_group_list);
    impl Drop for List {
        fn drop(&mut self) {
            if !self.0.is_null() {
                // SAFETY: unique list ownership transferred by rd_kafka_list_groups.
                unsafe { bindings::rd_kafka_group_list_destroy(self.0) };
            }
        }
    }
    let mut pointer = std::ptr::null();
    // SAFETY: live client, null group means all groups, initialized output pointer,
    // finite i32 timeout. Returned allocation is immediately managed by List.
    let status = unsafe {
        bindings::rd_kafka_list_groups(
            admin.inner().native_ptr(),
            std::ptr::null(),
            &mut pointer,
            timeout.as_millis().min(i32::MAX as u128) as i32,
        )
    };
    let list = List(pointer);
    use bindings::rd_kafka_resp_err_t::{RD_KAFKA_RESP_ERR__PARTIAL, RD_KAFKA_RESP_ERR_NO_ERROR};
    if status != RD_KAFKA_RESP_ERR_NO_ERROR && status != RD_KAFKA_RESP_ERR__PARTIAL {
        return Err(native_error(KafkaError::GroupListFetch(status.into())));
    }
    if status == RD_KAFKA_RESP_ERR__PARTIAL {
        failures
            .lock()
            .expect("failures poisoned")
            .push(ProbeError::Query.failure("groups", "cluster"));
    }
    // SAFETY: librdkafka's owned group list remains live for this scope.
    let native = unsafe { list.0.as_ref() }.ok_or(ProbeError::Query)?;
    let count = usize::try_from(native.group_cnt).map_err(|_| ProbeError::Query)?;
    if count == 0 {
        return Ok(Vec::new());
    }
    if native.groups.is_null()
        || count > isize::MAX as usize / std::mem::size_of::<bindings::rd_kafka_group_info>()
    {
        return Err(ProbeError::Query);
    }
    // SAFETY: group_cnt entries in the native-owned list; null/size checked above.
    let entries = unsafe { std::slice::from_raw_parts(native.groups, count) };
    let mut groups = Vec::new();
    for entry in entries {
        if entry.group.is_null() {
            failures
                .lock()
                .expect("failures poisoned")
                .push(ProbeError::Query.failure("groups", "unknown"));
            continue;
        }
        // SAFETY: librdkafka group names are NUL-terminated and list-owned.
        let name = unsafe { std::ffi::CStr::from_ptr(entry.group) }
            .to_str()
            .map_err(|_| ProbeError::Query)?
            .to_owned();
        if entry.err != RD_KAFKA_RESP_ERR_NO_ERROR {
            failures.lock().expect("failures poisoned").push(
                native_error(KafkaError::GroupListFetch(entry.err.into())).failure("groups", &name),
            );
        }
        let mut assigned_topics = Vec::new();
        // Only the consumer protocol has the topic/partition assignment wire schema.
        let consumer = !entry.protocol_type.is_null()
            // SAFETY: strings are NUL-terminated and owned by this live native list.
            && unsafe { std::ffi::CStr::from_ptr(entry.protocol_type) }.to_bytes() == b"consumer";
        if consumer && entry.member_cnt > 0 {
            let count = entry.member_cnt as usize;
            if entry.members.is_null()
                || count
                    > isize::MAX as usize
                        / std::mem::size_of::<bindings::rd_kafka_group_member_info>()
            {
                failures
                    .lock()
                    .expect("failures poisoned")
                    .push(ProbeError::Query.failure("groups", &name));
            } else {
                // SAFETY: native list owns member_cnt aligned member entries until List drops.
                for member in unsafe { std::slice::from_raw_parts(entry.members, count) } {
                    if member.member_assignment_size == 0 {
                        continue;
                    }
                    let size = usize::try_from(member.member_assignment_size)
                        .ok()
                        .filter(|size| *size <= isize::MAX as usize);
                    let topics = if let Some(size) =
                        size.filter(|_| !member.member_assignment.is_null())
                    {
                        // SAFETY: librdkafka owns size initialized assignment bytes in this list.
                        let bytes = unsafe {
                            std::slice::from_raw_parts(member.member_assignment.cast::<u8>(), size)
                        };
                        assignment_topics(bytes)
                    } else {
                        Err(ProbeError::Query)
                    };
                    match topics {
                        Ok(topics) => assigned_topics.extend(topics),
                        Err(error) => failures
                            .lock()
                            .expect("failures poisoned")
                            .push(error.failure("groups", &name)),
                    }
                }
            }
        }
        assigned_topics.sort();
        assigned_topics.dedup();
        groups.push(GroupMetadata {
            group_id: name,
            assigned_topics,
        });
    }
    Ok(groups)
}

#[async_trait]
impl KafkaProbe for NativeProbe {
    async fn topics(&self) -> Result<Vec<TopicMetadata>, ProbeError> {
        let admin = self.admin.clone();
        let timeout = self.options.operation_timeout;
        let failures = self.failures.clone();
        self.blocking(move || {
            let metadata = admin
                .inner()
                .fetch_metadata(None, timeout)
                .map_err(native_error)?;
            Ok(metadata
                .topics()
                .iter()
                .map(|topic| {
                    let mut metadata_complete = topic.error().is_none();
                    if let Some(error) = topic.error() {
                        failures.lock().expect("failures poisoned").push(
                            native_error(KafkaError::MetadataFetch(error.into()))
                                .failure("topics", topic.name()),
                        );
                    }
                    let partitions = topic
                        .partitions()
                        .iter()
                        .map(|p| {
                            if let Some(error) = p.error() {
                                metadata_complete = false;
                                failures.lock().expect("failures poisoned").push(
                                    native_error(KafkaError::MetadataFetch(error.into())).failure(
                                        "metadata",
                                        format!("{}:{}", topic.name(), p.id()),
                                    ),
                                );
                            }
                            p.id()
                        })
                        .collect();
                    TopicMetadata {
                        topic: topic.name().into(),
                        partitions,
                        metadata_complete,
                    }
                })
                .collect())
        })
        .await
    }
    async fn watermarks(&self, topic: &str, partition: i32) -> Result<(i64, i64), ProbeError> {
        let admin = self.admin.clone();
        let topic = topic.to_owned();
        let timeout = self.options.operation_timeout;
        self.blocking(move || {
            admin
                .inner()
                .fetch_watermarks(&topic, partition, timeout)
                .map_err(native_error)
        })
        .await
    }
    async fn groups(&self) -> Result<Vec<GroupMetadata>, ProbeError> {
        let admin = self.admin.clone();
        let failures = self.failures.clone();
        let timeout = self.options.operation_timeout;
        self.blocking(move || discover_groups(&admin, timeout, &failures))
            .await
    }
    async fn committed(
        &self,
        group: &str,
        partitions: &[TopicPartition],
    ) -> Result<Vec<CommittedOffset>, ProbeError> {
        if partitions.is_empty() {
            return Ok(Vec::new());
        }
        let cache = self.cache.clone();
        let config = self.config.clone();
        let group = group.to_owned();
        let partitions = partitions.to_vec();
        let timeout = self.options.operation_timeout;
        self.blocking(move || {
            let consumer = cache.get_or_create(&group, &config)?;
            let mut list = TopicPartitionList::new();
            for p in partitions {
                list.add_partition(&p.topic, p.partition);
            }
            let offsets = consumer
                .committed_offsets(list, timeout)
                .map_err(native_error)?;
            Ok(offsets
                .elements()
                .into_iter()
                .map(|p| CommittedOffset {
                    partition: TopicPartition {
                        topic: p.topic().into(),
                        partition: p.partition(),
                    },
                    offset: match p.offset() {
                        Offset::Offset(value) if value >= 0 => Some(value),
                        _ => None,
                    },
                    error: p.error().err().map(native_error),
                })
                .collect())
        })
        .await
    }
    fn take_failures(&self) -> Vec<PartialFailure> {
        std::mem::take(&mut *self.failures.lock().expect("failures poisoned"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // Consumer assignment includes topic/partition arrays, not subscriptions or user data.
    #[test]
    fn parses_assignment_topics_and_rejects_truncated_or_invalid_lengths() {
        let assignment = b"\x00\x03\x00\x00\x00\x02\x00\x06orders\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x02\x00\x05empty\x00\x00\x00\x00\xff\xff\xff\xff";
        assert_eq!(assignment_topics(assignment).unwrap(), ["orders"]);
        for end in 0..assignment.len() {
            assert!(
                assignment_topics(&assignment[..end]).is_err(),
                "accepted truncated assignment {end}"
            );
        }
        assert!(assignment_topics(b"\x00\x00\x7f\xff\xff\xff").is_err());
        assert!(assignment_topics(b"\x00\x00\xff\xff\xff\xff").is_err());
        assert!(assignment_topics(b"\xff\xff\x00\x00\x00\x00\xff\xff\xff\xff").is_err());
    }

    struct FakeProbe {
        active: AtomicUsize,
        peak: AtomicUsize,
        delay: Duration,
        committed_fails: bool,
        topics_fails: bool,
        all_watermarks_fail: bool,
        multiple_groups: bool,
        assigned_topics: Vec<String>,
        no_offsets: bool,
    }

    #[async_trait::async_trait]
    impl KafkaProbe for FakeProbe {
        async fn topics(&self) -> Result<Vec<TopicMetadata>, ProbeError> {
            if self.topics_fails {
                return Err(ProbeError::Query);
            }
            Ok(vec![
                TopicMetadata {
                    topic: "zeta".into(),
                    partitions: vec![0],
                    metadata_complete: true,
                },
                TopicMetadata {
                    topic: "orders".into(),
                    partitions: vec![2, 0, 1],
                    metadata_complete: true,
                },
            ])
        }
        async fn watermarks(&self, topic: &str, partition: i32) -> Result<(i64, i64), ProbeError> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            struct Release<'a>(&'a AtomicUsize);
            impl Drop for Release<'_> {
                fn drop(&mut self) {
                    self.0.fetch_sub(1, Ordering::SeqCst);
                }
            }
            let _release = Release(&self.active);
            tokio::time::sleep(self.delay).await;
            if self.all_watermarks_fail {
                return Err(ProbeError::Query);
            }
            match (topic, partition) {
                ("orders", 2) => Err(ProbeError::Query),
                ("orders", 0) => Ok((120, 98_241)),
                ("orders", 1) => Ok((5, 5)),
                _ => Ok((0, 10)),
            }
        }
        async fn groups(&self) -> Result<Vec<GroupMetadata>, ProbeError> {
            // No active members are required to discover an inactive group.
            let mut groups = vec![GroupMetadata {
                group_id: "inactive".into(),
                assigned_topics: self.assigned_topics.clone(),
            }];
            if self.multiple_groups {
                groups.push(GroupMetadata {
                    group_id: "live".into(),
                    assigned_topics: Vec::new(),
                });
            }
            Ok(groups)
        }
        async fn committed(
            &self,
            group: &str,
            partitions: &[TopicPartition],
        ) -> Result<Vec<CommittedOffset>, ProbeError> {
            assert!(
                group == "inactive" || (self.multiple_groups && group == "live"),
                "unexpected target group.id"
            );
            let expected = if self.topics_fails {
                vec![]
            } else {
                vec![("orders", 0), ("orders", 1), ("orders", 2), ("zeta", 0)]
            };
            assert_eq!(
                partitions
                    .iter()
                    .map(|p| (p.topic.as_str(), p.partition))
                    .collect::<Vec<_>>(),
                expected,
                "committed queries must include every discovered partition"
            );
            if self.committed_fails {
                return Err(ProbeError::Authorization);
            }
            Ok(partitions
                .iter()
                .map(|p| CommittedOffset {
                    partition: p.clone(),
                    offset: if self.no_offsets {
                        Some(-1)
                    } else if p.topic == "orders" && p.partition == 0 {
                        Some(if group == "inactive" { 90_031 } else { 98_240 })
                    } else if p.topic == "orders" && p.partition == 2 {
                        Some(77)
                    } else {
                        None
                    },
                    error: None,
                })
                .collect())
        }
    }

    fn probe(delay: Duration) -> FakeProbe {
        FakeProbe {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            delay,
            committed_fails: false,
            topics_fails: false,
            all_watermarks_fail: false,
            multiple_groups: false,
            assigned_topics: Vec::new(),
            no_offsets: false,
        }
    }

    // Unrelated topics cannot produce artificial uncommitted/abnormal group rows.
    #[tokio::test]
    async fn committed_topics_exclude_unrelated_topics_but_keep_every_associated_partition() {
        let snapshot = collect(
            &probe(Duration::ZERO),
            "cluster",
            CollectionOptions::default(),
        )
        .await;
        assert_eq!(snapshot.consumer_groups.len(), 1);
        assert_eq!(snapshot.consumer_groups[0].topic, "orders");
        assert_eq!(snapshot.consumer_groups[0].partitions.len(), 3);
        assert_eq!(
            snapshot.consumer_groups[0].partitions[1].committed_offset,
            None
        );
    }

    #[tokio::test]
    async fn unassociated_group_with_failed_offsets_keeps_only_its_identity() {
        let mut probe = probe(Duration::ZERO);
        probe.committed_fails = true;
        let snapshot = collect(&probe, "cluster", CollectionOptions::default()).await;
        assert_eq!(snapshot.consumer_group_ids, ["inactive"]);
        assert!(snapshot.consumer_groups.is_empty());
        assert!(
            snapshot
                .partial_failures
                .iter()
                .any(|failure| failure.scope == "committed")
        );
    }

    #[tokio::test]
    async fn active_assignments_keep_uncommitted_topic_and_negative_offsets_do_not_associate_topics()
     {
        let mut probe = probe(Duration::ZERO);
        probe.assigned_topics = vec!["orders".into()];
        probe.no_offsets = true;
        let snapshot = collect(&probe, "cluster", CollectionOptions::default()).await;
        assert_eq!(snapshot.consumer_groups.len(), 1);
        assert_eq!(snapshot.consumer_groups[0].topic, "orders");
        assert_eq!(snapshot.consumer_groups[0].partitions.len(), 3);
        assert!(
            snapshot.consumer_groups[0]
                .partitions
                .iter()
                .all(|partition| partition.committed_offset.is_none())
        );
        probe.assigned_topics.clear();
        let snapshot = collect(&probe, "cluster", CollectionOptions::default()).await;
        assert!(snapshot.consumer_groups.is_empty());
        assert_eq!(snapshot.consumer_group_ids, ["inactive"]);
    }

    // Dropping a failed partition, treating uncommitted as zero, or missing inactive groups breaks this.
    #[tokio::test]
    async fn preserves_healthy_topics_inactive_groups_unknown_offsets_and_partial_failures() {
        let snapshot = collect(
            &probe(Duration::ZERO),
            "cluster",
            CollectionOptions {
                concurrency: 2,
                operation_timeout: Duration::from_secs(1),
            },
        )
        .await;
        assert_eq!(snapshot.cluster_id, "cluster");
        assert!(chrono::DateTime::parse_from_rfc3339(&snapshot.collected_at).is_ok());
        assert_eq!(
            snapshot
                .topics
                .iter()
                .map(|t| t.topic.as_str())
                .collect::<Vec<_>>(),
            ["orders", "zeta"]
        );
        assert_eq!(
            snapshot.topics[0]
                .partitions
                .iter()
                .map(|p| p.partition)
                .collect::<Vec<_>>(),
            [0, 1, 2]
        );
        assert_eq!(snapshot.topics[0].latest_message_offset, Some(98_240));
        assert_eq!(snapshot.topics[0].retained_offset_span, 98_121);
        let group = &snapshot.consumer_groups[0];
        assert_eq!((&*group.group_id, &*group.topic), ("inactive", "orders"));
        assert_eq!(group.lag, 8_210);
        assert_eq!(group.unknown_partition_count, 2);
        assert_eq!(group.partitions[1].lag, None);
        assert_eq!(snapshot.partial_failures.len(), 1);
        assert_eq!(snapshot.partial_failures[0].scope, "watermark");
        assert_eq!(snapshot.partial_failures[0].resource, "orders:2");
        assert_eq!(snapshot.partial_failures[0].code, "KAFKA_QUERY_FAILED");
    }

    // Starting all partition requests at once breaks the measured upper bound.
    #[tokio::test]
    async fn bounds_parallel_watermark_requests() {
        let probe = probe(Duration::from_millis(5));
        collect(
            &probe,
            "cluster",
            CollectionOptions {
                concurrency: 2,
                operation_timeout: Duration::from_secs(1),
            },
        )
        .await;
        assert_eq!(probe.peak.load(Ordering::SeqCst), 2);
    }

    // An unbounded await or aborting the entire snapshot on a local timeout breaks this.
    #[tokio::test]
    async fn operation_deadlines_return_partial_snapshot() {
        let snapshot = collect(
            &probe(Duration::from_secs(60)),
            "cluster",
            CollectionOptions {
                concurrency: 2,
                operation_timeout: Duration::from_millis(5),
            },
        )
        .await;
        assert_eq!(snapshot.topics.len(), 2);
        assert_eq!(snapshot.partial_failures.len(), 4);
        assert!(
            snapshot
                .partial_failures
                .iter()
                .all(|f| f.code == "KAFKA_TIMEOUT")
        );
    }

    // A failed committed request must leave the discovered group visible with unknown lag.
    #[tokio::test]
    async fn committed_failure_preserves_discovered_group_and_healthy_topics() {
        let mut probe = probe(Duration::ZERO);
        probe.committed_fails = true;
        let snapshot = collect(&probe, "cluster", CollectionOptions::default()).await;
        assert_eq!(snapshot.topics.len(), 2);
        assert!(snapshot.consumer_groups.is_empty());
        assert_eq!(snapshot.consumer_group_ids, ["inactive"]);
        assert!(
            snapshot
                .partial_failures
                .iter()
                .any(|failure| failure.scope == "committed" && failure.code == "KAFKA_AUTH_FAILED")
        );
    }

    // Independent committed success must survive failure of this partition's watermark.
    #[tokio::test]
    async fn failed_watermark_keeps_partition_identity_and_successful_committed_offset() {
        let snapshot = serde_json::to_value(
            collect(
                &probe(Duration::ZERO),
                "cluster",
                CollectionOptions::default(),
            )
            .await,
        )
        .unwrap();
        let partition = &snapshot["topics"][0]["partitions"][2];
        assert_eq!(partition["partition"], 2);
        assert!(partition["low"].is_null() && partition["high"].is_null());
        assert!(partition["retainedOffsetSpan"].is_null());
        assert_eq!(snapshot["topics"][0]["incomplete"], true);
        let group = &snapshot["consumerGroups"][0];
        assert_eq!(group["partitions"][2]["committedOffset"], 77);
        assert!(group["partitions"][2]["high"].is_null());
        assert!(group["partitions"][2]["lag"].is_null());
        assert_eq!(group["unknownPartitionCount"], 2);
        assert_eq!(group["incomplete"], true);
    }

    // All watermark failures must still preserve known offsets and every partition.
    #[tokio::test]
    async fn all_watermarks_failed_retains_unknown_metrics_and_known_committed_offsets() {
        let mut probe = probe(Duration::ZERO);
        probe.all_watermarks_fail = true;
        let snapshot =
            serde_json::to_value(collect(&probe, "cluster", CollectionOptions::default()).await)
                .unwrap();
        assert_eq!(
            snapshot["topics"][0]["partitions"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert!(snapshot["topics"][0]["low"].is_null());
        assert_eq!(snapshot["topics"][0]["incomplete"], true);
        let group = &snapshot["consumerGroups"][0];
        assert_eq!(group["partitions"].as_array().unwrap().len(), 3);
        assert_eq!(group["partitions"][0]["committedOffset"], 90_031);
        assert!(group["partitions"][1]["committedOffset"].is_null());
        assert_eq!(group["partitions"][2]["committedOffset"], 77);
        assert_eq!(group["unknownPartitionCount"], 3);
        assert_eq!(group["incomplete"], true);
    }

    // Topic metadata failure must not erase successful group discovery.
    #[tokio::test]
    async fn metadata_failure_preserves_discovered_group_identities() {
        let mut probe = probe(Duration::ZERO);
        probe.topics_fails = true;
        probe.multiple_groups = true;
        let snapshot =
            serde_json::to_value(collect(&probe, "cluster", CollectionOptions::default()).await)
                .unwrap();
        assert_eq!(
            snapshot["consumerGroupIds"],
            serde_json::json!(["inactive", "live"])
        );
        assert_eq!(snapshot["topics"], serde_json::json!([]));
        assert_eq!(snapshot["consumerGroups"], serde_json::json!([]));
        assert_eq!(snapshot["partialFailures"][0]["scope"], "topics");
    }

    // A wrong group.id, swapped offsets, or replacing unknown with zero breaks this.
    #[tokio::test]
    async fn committed_offsets_are_aggregated_for_the_exact_target_group() {
        let mut probe = probe(Duration::ZERO);
        probe.multiple_groups = true;
        let snapshot = collect(&probe, "cluster", CollectionOptions::default()).await;
        let inactive = snapshot
            .consumer_groups
            .iter()
            .find(|row| row.group_id == "inactive" && row.topic == "orders")
            .unwrap();
        let live = snapshot
            .consumer_groups
            .iter()
            .find(|row| row.group_id == "live" && row.topic == "orders")
            .unwrap();
        assert_eq!(inactive.partitions[0].committed_offset, Some(90_031));
        assert_eq!(inactive.partitions[0].lag, Some(8_210));
        assert_eq!(live.partitions[0].committed_offset, Some(98_240));
        assert_eq!(live.partitions[0].lag, Some(1));
        assert_eq!(inactive.partitions[1].committed_offset, None);
        assert_eq!(live.partitions[1].committed_offset, None);
        assert_eq!(inactive.partitions[1].lag, None);
        assert_eq!(live.partitions[1].lag, None);
    }

    // Cancellation of an async timeout must not release the native concurrency slot.
    #[tokio::test]
    async fn native_timeouts_retain_permits_and_finish_waits_for_blocking_work() {
        use crate::sessions::manager::{KafkaSessionClientFactory, SessionClientFactory};
        let client = KafkaSessionClientFactory
            .create(DecryptedClusterConfig::plaintext("127.0.0.1:1"))
            .await
            .unwrap();
        let probe = Arc::new(NativeProbe::new(
            &client,
            CollectionOptions {
                concurrency: 1,
                operation_timeout: Duration::from_millis(5),
            },
        ));
        let (release, wait) = std::sync::mpsc::channel();
        let timed = tokio::time::timeout(
            Duration::from_millis(5),
            probe.blocking(move || {
                wait.recv().unwrap();
                Ok(())
            }),
        )
        .await;
        assert!(timed.is_err());
        let started = Arc::new(AtomicUsize::new(0));
        let count = started.clone();
        assert!(
            tokio::time::timeout(
                Duration::from_millis(5),
                probe.blocking(move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            )
            .await
            .is_err()
        );
        assert_eq!(started.load(Ordering::SeqCst), 0);
        let closing_probe = probe.clone();
        let finish = tokio::spawn(async move { closing_probe.finish().await });
        tokio::task::yield_now().await;
        assert!(!finish.is_finished());
        release.send(()).unwrap();
        finish.await.unwrap();
    }
}
