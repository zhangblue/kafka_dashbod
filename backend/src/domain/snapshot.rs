use serde::{Deserialize, Serialize};

use super::offset::{lag, latest_message_offset, retained_offset_span};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionWatermark {
    pub partition: i32,
    pub low: i64,
    pub high: i64,
    pub latest_message_offset: Option<i64>,
    pub retained_offset_span: u64,
}

impl PartitionWatermark {
    pub fn new(partition: i32, low: i64, high: i64) -> Self {
        Self {
            partition,
            low,
            high,
            latest_message_offset: latest_message_offset(low, high),
            retained_offset_span: retained_offset_span(low, high),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicSnapshot {
    pub topic: String,
    /// Minimum partition low watermark; offsets remain partition-local.
    pub low: Option<i64>,
    /// Maximum latest offset among nonempty partitions.
    pub latest_message_offset: Option<i64>,
    /// Sum of retained offset spans, not a physical message count.
    pub retained_offset_span: u64,
    pub partitions: Vec<PartitionWatermark>,
}

impl TopicSnapshot {
    pub fn from_partitions(topic: String, partitions: Vec<PartitionWatermark>) -> Self {
        Self {
            topic,
            low: partitions.iter().map(|partition| partition.low).min(),
            latest_message_offset: partitions
                .iter()
                .filter_map(|partition| partition.latest_message_offset)
                .max(),
            retained_offset_span: partitions
                .iter()
                .map(|partition| partition.retained_offset_span)
                .sum(),
            partitions,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupPartitionSnapshot {
    pub partition: i32,
    pub high: i64,
    pub committed_offset: Option<i64>,
    pub lag: Option<u64>,
}

impl GroupPartitionSnapshot {
    pub fn new(partition: i32, high: i64, committed_offset: Option<i64>) -> Self {
        Self {
            partition,
            high,
            committed_offset,
            lag: lag(high, committed_offset),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConsumerGroupTopicSnapshot {
    pub group_id: String,
    pub topic: String,
    /// Sum over committed partitions only; consult unknown_partition_count as well.
    pub lag: u64,
    pub unknown_partition_count: usize,
    pub partitions: Vec<GroupPartitionSnapshot>,
}

impl ConsumerGroupTopicSnapshot {
    pub fn from_partitions(
        group_id: String,
        topic: String,
        partitions: Vec<GroupPartitionSnapshot>,
    ) -> Self {
        Self {
            group_id,
            topic,
            lag: partitions
                .iter()
                .filter_map(|partition| partition.lag)
                .sum(),
            unknown_partition_count: partitions
                .iter()
                .filter(|partition| partition.committed_offset.is_none())
                .count(),
            partitions,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartialFailure {
    pub scope: String,
    pub resource: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterSnapshot {
    pub cluster_id: String,
    /// Collection time as an RFC 3339 timestamp supplied by the collector.
    pub collected_at: String,
    pub topics: Vec<TopicSnapshot>,
    pub consumer_groups: Vec<ConsumerGroupTopicSnapshot>,
    pub partial_failures: Vec<PartialFailure>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Catch summing partition offsets or mistaking the exclusive high for the latest offset.
    #[test]
    fn topic_uses_min_low_max_latest_and_sum_of_spans() {
        let snapshot = TopicSnapshot::from_partitions(
            "orders".into(),
            vec![
                PartitionWatermark::new(0, 120, 98_241),
                PartitionWatermark::new(1, 10, 200),
                PartitionWatermark::new(2, 100_000, 100_000),
            ],
        );
        assert_eq!(snapshot.low, Some(10));
        assert_eq!(snapshot.latest_message_offset, Some(98_240));
        assert_eq!(snapshot.retained_offset_span, 98_311);
        assert_eq!(snapshot.partitions[0].latest_message_offset, Some(98_240));
        assert_eq!(snapshot.partitions[0].retained_offset_span, 98_121);
    }

    // Catch fabricating a latest offset when no partition retains messages.
    #[test]
    fn empty_topics_have_no_latest_message_offset() {
        let absent = TopicSnapshot::from_partitions("absent".into(), vec![]);
        assert_eq!(absent.low, None);
        assert_eq!(absent.latest_message_offset, None);
        assert_eq!(absent.retained_offset_span, 0);

        let empty =
            TopicSnapshot::from_partitions("empty".into(), vec![PartitionWatermark::new(0, 5, 5)]);
        assert_eq!(empty.low, Some(5));
        assert_eq!(empty.latest_message_offset, None);
        assert_eq!(empty.retained_offset_span, 0);
    }

    // Catch counting uncommitted partitions as known zero lag or summing committed offsets.
    #[test]
    fn group_topic_sums_known_lag_and_counts_unknown_partitions() {
        let snapshot = ConsumerGroupTopicSnapshot::from_partitions(
            "billing".into(),
            "orders".into(),
            vec![
                GroupPartitionSnapshot::new(0, 98_241, Some(90_031)),
                GroupPartitionSnapshot::new(1, 200, Some(180)),
                GroupPartitionSnapshot::new(2, 5, None),
            ],
        );
        assert_eq!(snapshot.lag, 8_230);
        assert_eq!(snapshot.unknown_partition_count, 1);
        assert_eq!(snapshot.partitions[0].lag, Some(8_210));
        assert_eq!(snapshot.partitions[2].committed_offset, None);
        assert_eq!(snapshot.partitions[2].lag, None);
    }

    #[test]
    fn entirely_uncommitted_group_keeps_unknown_count() {
        let snapshot = ConsumerGroupTopicSnapshot::from_partitions(
            "billing".into(),
            "orders".into(),
            vec![GroupPartitionSnapshot::new(0, 5, None)],
        );
        assert_eq!(snapshot.lag, 0);
        assert_eq!(snapshot.unknown_partition_count, 1);
    }

    // Catch snake_case fields or omission of unknown values in the public JSON contract.
    #[test]
    fn cluster_json_uses_camel_case_and_explicit_unknown_values() {
        let snapshot = ClusterSnapshot {
            cluster_id: "local".into(),
            collected_at: "2026-10-15T00:00:00Z".into(),
            topics: vec![TopicSnapshot::from_partitions(
                "orders".into(),
                vec![PartitionWatermark::new(0, 5, 5)],
            )],
            consumer_groups: vec![ConsumerGroupTopicSnapshot::from_partitions(
                "billing".into(),
                "orders".into(),
                vec![GroupPartitionSnapshot::new(0, 5, None)],
            )],
            partial_failures: vec![PartialFailure {
                scope: "watermark".into(),
                resource: "orders:1".into(),
                code: "KAFKA_TIMEOUT".into(),
                message: "watermark timeout".into(),
            }],
        };
        let serialized = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(
            serialized,
            json!({
                "clusterId": "local",
                "collectedAt": "2026-10-15T00:00:00Z",
                "topics": [{
                    "topic": "orders", "low": 5, "latestMessageOffset": null,
                    "retainedOffsetSpan": 0,
                    "partitions": [{ "partition": 0, "low": 5, "high": 5,
                        "latestMessageOffset": null, "retainedOffsetSpan": 0 }]
                }],
                "consumerGroups": [{
                    "groupId": "billing", "topic": "orders", "lag": 0,
                    "unknownPartitionCount": 1,
                    "partitions": [{ "partition": 0, "high": 5,
                        "committedOffset": null, "lag": null }]
                }],
                "partialFailures": [{ "scope": "watermark", "resource": "orders:1", "code": "KAFKA_TIMEOUT", "message": "watermark timeout" }]
            })
        );
        assert_eq!(
            serde_json::from_value::<ClusterSnapshot>(serialized).unwrap(),
            snapshot
        );
    }
}
