use std::{
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
}

pub struct GroupMetadata {
    pub group_id: String,
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
    .collect::<Vec<_>>()
    .await;
    watermarks.sort_by(|(a, _), (b, _)| (&a.topic, a.partition).cmp(&(&b.topic, b.partition)));
    let mut topic_snapshots = Vec::new();
    for topic in topics {
        let mut healthy = Vec::new();
        for (partition, result) in watermarks.iter().filter(|(p, _)| p.topic == topic.topic) {
            match result {
                Ok((low, high)) => {
                    healthy.push(PartitionWatermark::new(partition.partition, *low, *high))
                }
                Err(error) => failures.push(error.failure(
                    "watermark",
                    format!("{}:{}", partition.topic, partition.partition),
                )),
            }
        }
        topic_snapshots.push(TopicSnapshot::from_partitions(topic.topic, healthy));
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
        for offset in &offsets {
            if let Some(error) = offset.error {
                failures.push(error.failure(
                    "committed",
                    format!(
                        "{}/{}:{}",
                        group.group_id, offset.partition.topic, offset.partition.partition
                    ),
                ));
            }
        }
        for topic in &topic_snapshots {
            let rows = topic
                .partitions
                .iter()
                .map(|p| {
                    let offset = offsets
                        .iter()
                        .find(|offset| {
                            offset.partition.topic == topic.topic
                                && offset.partition.partition == p.partition
                                && offset.error.is_none()
                        })
                        .and_then(|offset| offset.offset)
                        .filter(|offset| *offset >= 0);
                    GroupPartitionSnapshot::new(p.partition, p.high, offset)
                })
                .collect();
            consumer_groups.push(ConsumerGroupTopicSnapshot::from_partitions(
                group.group_id.clone(),
                topic.topic.clone(),
                rows,
            ));
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
        groups.push(GroupMetadata { group_id: name });
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
                    if let Some(error) = topic.error() {
                        failures.lock().expect("failures poisoned").push(
                            native_error(KafkaError::MetadataFetch(error.into()))
                                .failure("topics", topic.name()),
                        );
                    }
                    let partitions = topic
                        .partitions()
                        .iter()
                        .filter_map(|p| {
                            if let Some(error) = p.error() {
                                failures.lock().expect("failures poisoned").push(
                                    native_error(KafkaError::MetadataFetch(error.into())).failure(
                                        "metadata",
                                        format!("{}:{}", topic.name(), p.id()),
                                    ),
                                );
                                None
                            } else {
                                Some(p.id())
                            }
                        })
                        .collect();
                    TopicMetadata {
                        topic: topic.name().into(),
                        partitions,
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

    struct FakeProbe {
        active: AtomicUsize,
        peak: AtomicUsize,
        delay: Duration,
        committed_fails: bool,
    }

    #[async_trait::async_trait]
    impl KafkaProbe for FakeProbe {
        async fn topics(&self) -> Result<Vec<TopicMetadata>, ProbeError> {
            Ok(vec![
                TopicMetadata {
                    topic: "zeta".into(),
                    partitions: vec![0],
                },
                TopicMetadata {
                    topic: "orders".into(),
                    partitions: vec![2, 0, 1],
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
            match (topic, partition) {
                ("orders", 2) => Err(ProbeError::Query),
                ("orders", 0) => Ok((120, 98_241)),
                ("orders", 1) => Ok((5, 5)),
                _ => Ok((0, 10)),
            }
        }
        async fn groups(&self) -> Result<Vec<GroupMetadata>, ProbeError> {
            // No active members are required to discover an inactive group.
            Ok(vec![GroupMetadata {
                group_id: "inactive".into(),
            }])
        }
        async fn committed(
            &self,
            _: &str,
            partitions: &[TopicPartition],
        ) -> Result<Vec<CommittedOffset>, ProbeError> {
            if self.committed_fails {
                return Err(ProbeError::Authorization);
            }
            Ok(partitions
                .iter()
                .map(|p| CommittedOffset {
                    partition: p.clone(),
                    offset: if p.topic == "orders" && p.partition == 0 {
                        Some(90_031)
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
        }
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
            [0, 1]
        );
        assert_eq!(snapshot.topics[0].latest_message_offset, Some(98_240));
        assert_eq!(snapshot.topics[0].retained_offset_span, 98_121);
        let group = &snapshot.consumer_groups[0];
        assert_eq!((&*group.group_id, &*group.topic), ("inactive", "orders"));
        assert_eq!(group.lag, 8_210);
        assert_eq!(group.unknown_partition_count, 1);
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
        assert_eq!(snapshot.consumer_groups.len(), 2);
        assert_eq!(snapshot.consumer_groups[0].group_id, "inactive");
        assert_eq!(snapshot.consumer_groups[0].unknown_partition_count, 2);
        assert!(
            snapshot
                .partial_failures
                .iter()
                .any(|failure| failure.scope == "committed" && failure.code == "KAFKA_AUTH_FAILED")
        );
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
