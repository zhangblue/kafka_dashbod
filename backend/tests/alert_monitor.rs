use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use kafka_dashboard::{
    alerts::monitor::{self, AlertMonitorHandle},
    db::{self, clusters::ClusterRepository, feishu_alerts::FeishuAlertRepository},
    domain::{
        cluster::{ClusterInput, DecryptedClusterConfig},
        feishu_alert::FeishuAlertRuleInput,
        snapshot::{ClusterSnapshot, ConsumerGroupTopicSnapshot, GroupPartitionSnapshot},
    },
    notifications::feishu::{AlertSendError, AlertSender},
    sessions::manager::{SessionClientFactory, SessionError},
};
use tokio::sync::Semaphore;

const PERIOD: Duration = Duration::from_secs(5);
const TEST_ALERT_TOKEN: &str = "test-alert-token";
const FAILURE_PROBE_TOKEN: &str = "monitor-diagnostic-probe-token";

#[tokio::test]
async fn monitor_token_assertion_failure_probe() {
    if std::env::var_os("MONITOR_ASSERTION_FAILURE_PROBE").is_none() {
        return;
    }
    let sender = FakeSender::default();
    let _ = sender.send_text(FAILURE_PROBE_TOKEN, "probe").await;
}

// Run the real sender assertion in isolation, keeping all failed-test output private.
#[test]
fn monitor_sensitive_assertion_diagnostics_never_echo_tokens() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "monitor_token_assertion_failure_probe",
            "--nocapture",
        ])
        .env("MONITOR_ASSERTION_FAILURE_PROBE", "1")
        .output()
        .expect("diagnostic probe should execute");
    assert!(!output.status.success(), "diagnostic probe must fail");
    let captured = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let exposes_token = [TEST_ALERT_TOKEN, FAILURE_PROBE_TOKEN]
        .iter()
        .any(|token| captured.contains(token));
    assert!(
        !exposes_token,
        "monitor assertion diagnostics must omit credentials"
    );
}

#[derive(Default)]
struct DropGate {
    started: AtomicBool,
    released: Mutex<bool>,
    changed: Condvar,
}

impl DropGate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

#[derive(Clone, Default)]
struct FakeFactory {
    snapshots: Arc<Mutex<VecDeque<Result<ClusterSnapshot, SessionError>>>>,
    configs: Arc<Mutex<Vec<DecryptedClusterConfig>>>,
    samples: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
    create_gate: Option<Arc<Semaphore>>,
    sample_gate: Option<Arc<Semaphore>>,
    drop_gate: Option<Arc<DropGate>>,
}

struct FakeClient {
    factory: FakeFactory,
}

impl Drop for FakeClient {
    fn drop(&mut self) {
        if let Some(gate) = &self.factory.drop_gate {
            gate.started.store(true, Ordering::SeqCst);
            let mut released = gate.released.lock().unwrap();
            while !*released {
                released = gate.changed.wait(released).unwrap();
            }
        }
        self.factory.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

impl SessionClientFactory for FakeFactory {
    type Client = FakeClient;

    async fn create(&self, config: DecryptedClusterConfig) -> Result<FakeClient, SessionError> {
        self.configs.lock().unwrap().push(config);
        if let Some(gate) = &self.create_gate {
            gate.acquire().await.unwrap().forget();
        }
        Ok(FakeClient {
            factory: self.clone(),
        })
    }

    async fn snapshot(
        &self,
        _client: &mut FakeClient,
        _id: &str,
    ) -> Result<ClusterSnapshot, SessionError> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.samples.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.sample_gate {
            gate.acquire().await.unwrap().forget();
        }
        let mut snapshots = self.snapshots.lock().unwrap();
        let result = if snapshots.len() > 1 {
            snapshots.pop_front().unwrap()
        } else {
            snapshots
                .front()
                .cloned()
                .unwrap_or_else(|| Ok(snapshot(&[6_000])))
        };
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

#[derive(Default)]
struct FakeSender {
    messages: Mutex<Vec<String>>,
    failures: AtomicUsize,
    gate: Option<Arc<Semaphore>>,
}

#[async_trait]
impl AlertSender for FakeSender {
    async fn send_text(&self, token: &str, text: &str) -> Result<(), AlertSendError> {
        let expected_token = TEST_ALERT_TOKEN;
        let matches_expected = token == expected_token;
        assert!(
            matches_expected,
            "monitor must pass the configured credential"
        );
        self.messages.lock().unwrap().push(text.to_owned());
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        if self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            Err(AlertSendError::Network)
        } else {
            Ok(())
        }
    }
}

// Keep SQLite's external worker threads from causing paused Tokio time to auto-advance.
struct Awake(tokio::task::JoinHandle<()>);
impl Awake {
    fn new() -> Self {
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}
impl Drop for Awake {
    fn drop(&mut self) {
        self.0.abort();
    }
}

struct Fixture {
    pool: sqlx::SqlitePool,
    clusters: ClusterRepository,
    settings: FeishuAlertRepository,
    cluster_id: String,
    factory: FakeFactory,
    sender: Arc<FakeSender>,
    _awake: Awake,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Failed assertions must not strand a fake native destructor or gate.
        if let Some(gate) = &self.factory.drop_gate {
            gate.release();
        }
        for gate in [
            &self.factory.create_gate,
            &self.factory.sample_gate,
            &self.sender.gate,
        ]
        .into_iter()
        .flatten()
        {
            gate.add_permits(100);
        }
    }
}

impl Fixture {
    async fn new(snapshots: Vec<ClusterSnapshot>) -> Self {
        let awake = Awake::new();
        let pool = db::connect("sqlite::memory:").await.unwrap();
        let clusters = ClusterRepository::new(pool.clone());
        let cluster_id = clusters
            .create(&ClusterInput::plaintext(
                "告警测试集群",
                "fixture.invalid:9092",
            ))
            .await
            .unwrap()
            .id;
        Self {
            pool: pool.clone(),
            clusters,
            settings: FeishuAlertRepository::new(pool),
            cluster_id,
            factory: FakeFactory {
                snapshots: Arc::new(Mutex::new(snapshots.into_iter().map(Ok).collect())),
                ..Default::default()
            },
            sender: Arc::new(FakeSender::default()),
            _awake: awake,
        }
    }

    async fn enable(&self) {
        self.settings
            .replace(
                &self.cluster_id,
                Some(TEST_ALERT_TOKEN),
                false,
                &[rule(true)],
            )
            .await
            .unwrap();
    }

    fn start(&self) -> AlertMonitorHandle {
        monitor::start(
            self.clusters.clone(),
            self.settings.clone(),
            self.factory.clone(),
            self.sender.clone(),
            PERIOD,
        )
    }

    fn created(&self) -> usize {
        self.factory.configs.lock().unwrap().len()
    }
    fn samples(&self) -> usize {
        self.factory.samples.load(Ordering::SeqCst)
    }
    fn messages(&self) -> Vec<String> {
        self.sender.messages.lock().unwrap().clone()
    }

    async fn tick(&self) {
        tokio::time::advance(PERIOD).await;
        settle().await;
    }
}

fn rule(enabled: bool) -> FeishuAlertRuleInput {
    FeishuAlertRuleInput {
        id: None,
        group_id: "g".into(),
        topic: "t".into(),
        lag_threshold: 5_000,
        growth_seconds: 10,
        enabled,
    }
}

fn snapshot(lags: &[i64]) -> ClusterSnapshot {
    let partitions = lags
        .iter()
        .enumerate()
        .map(|(i, lag)| GroupPartitionSnapshot::new(i as i32, 10_000 + lag, Some(10_000)))
        .collect();
    ClusterSnapshot {
        cluster_id: "fixture".into(),
        collected_at: "2026-10-22T10:00:00+08:00".into(),
        topics: vec![],
        consumer_group_ids: vec!["g".into()],
        consumer_groups: vec![ConsumerGroupTopicSnapshot::from_partitions(
            "g".into(),
            "t".into(),
            partitions,
        )],
        partial_failures: vec![],
    }
}

async fn settle() {
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
}

async fn until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..100_000 {
        if predicate() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("monitor did not reach expected state without advancing time");
}

// Catches summing only a subset, trusting a partial row aggregate, or duplicate sends.
#[tokio::test(start_paused = true)]
async fn monitor_sends_once_for_complete_total_lag_growth() {
    let mut samples = vec![
        snapshot(&[2_500, 2_600]),
        snapshot(&[2_700, 2_800]),
        snapshot(&[2_900, 3_000]),
    ];
    for sample in &mut samples {
        sample.consumer_groups[0].lag = 1;
    }
    let fixture = Fixture::new(samples).await;
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    fixture.tick().await;
    assert!(fixture.messages().is_empty());
    fixture.tick().await;
    assert_eq!(fixture.messages().len(), 1);
    let text = &fixture.messages()[0];
    for field in [
        "集群：告警测试集群",
        "Topic：t",
        "Consumer Group：g",
        "阈值：5,000",
        "全部 Partition Lag 总和）：5,900",
        "连续增长时间：10 秒",
        "检测时间：",
    ] {
        assert!(text.contains(field), "missing {field}");
    }
    let contains_token = text.contains(TEST_ALERT_TOKEN);
    assert!(!contains_token, "alert text must omit credentials");
    let contains_server = text.contains("fixture.invalid");
    assert!(!contains_server, "alert text must omit connection details");
    for _ in 0..4 {
        fixture.tick().await;
    }
    assert_eq!(fixture.messages().len(), 1);
    assert_eq!(fixture.created(), 1);
    handle.shutdown().await;
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
}

// Each incomplete observation must break the growth window, never become partial/zero Lag.
#[tokio::test(start_paused = true)]
async fn incomplete_unknown_missing_and_wrong_rows_break_growth() {
    for case in 0..9 {
        let mut invalid = snapshot(&[5_300]);
        let row = &mut invalid.consumer_groups[0];
        match case {
            0 => row.incomplete = true,
            1 => row.unknown_partition_count = 1,
            2 => row.partitions[0].high = None,
            3 => row.partitions[0].committed_offset = None,
            4 => row.partitions[0].lag = None,
            5 => row.group_id = "other-g".into(),
            6 => row.topic = "other-t".into(),
            7 => row.partitions[0].lag = Some(u64::MAX),
            _ => invalid.consumer_groups.clear(),
        }
        let fixture = Fixture::new(vec![
            snapshot(&[5_100]),
            invalid,
            snapshot(&[5_500]),
            snapshot(&[5_600]),
            snapshot(&[5_700]),
        ])
        .await;
        fixture.enable().await;
        let handle = fixture.start();
        until(|| fixture.samples() == 1).await;
        for _ in 0..3 {
            fixture.tick().await;
        }
        assert!(fixture.messages().is_empty(), "case {case}");
        fixture.tick().await;
        assert_eq!(fixture.messages().len(), 1, "case {case}");
        handle.shutdown().await;
    }
}

// Eligibility must gate client allocation, including token-only and disabled rules.
#[tokio::test(start_paused = true)]
async fn no_token_or_no_enabled_rules_never_creates_a_client() {
    let fixture = Fixture::new(vec![]).await;
    let handle = fixture.start();
    fixture.tick().await;
    assert_eq!(fixture.created(), 0);
    fixture
        .settings
        .replace(&fixture.cluster_id, None, false, &[rule(true)])
        .await
        .unwrap();
    fixture.tick().await;
    assert_eq!(fixture.created(), 0);
    fixture
        .settings
        .replace(
            &fixture.cluster_id,
            Some(TEST_ALERT_TOKEN),
            false,
            &[rule(false)],
        )
        .await
        .unwrap();
    fixture.tick().await;
    assert_eq!(fixture.created(), 0);
    fixture
        .settings
        .replace(&fixture.cluster_id, None, false, &[])
        .await
        .unwrap();
    fixture.tick().await;
    assert_eq!(fixture.created(), 0);
    fixture.enable().await;
    fixture.tick().await;
    until(|| fixture.created() == 1).await;
    handle.shutdown().await;
}

// Retry timing is measured from completion, with at most three attempts in one round.
#[tokio::test(start_paused = true)]
async fn failed_delivery_retries_after_thirty_seconds_and_stops_after_three_attempts() {
    let fixture = Fixture::new(vec![
        snapshot(&[5_100]),
        snapshot(&[5_200]),
        snapshot(&[5_300]),
    ])
    .await;
    fixture.sender.failures.store(10, Ordering::SeqCst);
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    for _ in 0..2 {
        fixture.tick().await;
    }
    assert_eq!(fixture.messages().len(), 1);
    for expected in [2, 3] {
        for _ in 0..5 {
            fixture.tick().await;
        }
        assert_eq!(fixture.messages().len(), expected - 1);
        fixture.tick().await;
        assert_eq!(fixture.messages().len(), expected);
    }
    for _ in 0..10 {
        fixture.tick().await;
    }
    assert_eq!(fixture.messages().len(), 3);
    handle.shutdown().await;
}

// A collection error must break continuity just as an unknown row does.
#[tokio::test(start_paused = true)]
async fn collection_failure_resets_growth_and_recovers() {
    let fixture = Fixture::new(vec![
        snapshot(&[5_100]),
        snapshot(&[5_200]),
        snapshot(&[5_300]),
        snapshot(&[5_400]),
        snapshot(&[5_500]),
    ])
    .await;
    fixture.factory.snapshots.lock().unwrap()[1] = Err(SessionError::Expired);
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    for _ in 0..3 {
        fixture.tick().await;
    }
    assert!(fixture.messages().is_empty());
    fixture.tick().await;
    assert_eq!(fixture.messages().len(), 1);
    handle.shutdown().await;
}

// Updating either alert settings or Kafka connection settings must replace the worker.
#[tokio::test(start_paused = true)]
async fn both_settings_and_connection_changes_rebuild_workers() {
    let fixture = Fixture::new(vec![]).await;
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    fixture.enable().await;
    fixture.tick().await;
    until(|| fixture.created() == 2).await;
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
    fixture
        .clusters
        .update(
            &fixture.cluster_id,
            &ClusterInput::plaintext("更新的集群", "updated.invalid:9092"),
        )
        .await
        .unwrap();
    fixture.tick().await;
    until(|| fixture.created() == 3).await;
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 2);
    assert_eq!(
        fixture.factory.configs.lock().unwrap()[2].bootstrap_servers,
        "updated.invalid:9092"
    );
    handle.shutdown().await;
}

// Clearing eligibility or deleting a cluster must release the persistent client.
#[tokio::test(start_paused = true)]
async fn clearing_token_disabling_rules_and_deleting_cluster_stop_workers() {
    for action in 0..3 {
        let fixture = Fixture::new(vec![]).await;
        fixture.enable().await;
        let handle = fixture.start();
        until(|| fixture.samples() == 1).await;
        match action {
            0 => {
                fixture
                    .settings
                    .replace(&fixture.cluster_id, None, true, &[rule(true)])
                    .await
                    .unwrap();
            }
            1 => {
                fixture
                    .settings
                    .replace(&fixture.cluster_id, None, false, &[rule(false)])
                    .await
                    .unwrap();
            }
            _ => {
                fixture.clusters.delete(&fixture.cluster_id).await.unwrap();
            }
        }
        fixture.tick().await;
        until(|| fixture.factory.dropped.load(Ordering::SeqCst) == 1).await;
        let samples = fixture.samples();
        fixture.tick().await;
        assert_eq!(fixture.samples(), samples);
        assert_eq!(fixture.created(), 1);
        handle.shutdown().await;
    }
}

// An accepted native creation may not be cancelled or detached on shutdown.
#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_creation_then_drops_without_sampling() {
    let mut fixture = Fixture::new(vec![]).await;
    let gate = Arc::new(Semaphore::new(0));
    fixture.factory.create_gate = Some(gate.clone());
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.created() == 1).await;
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    assert!(!shutdown.is_finished());
    gate.add_permits(1);
    shutdown.await.unwrap();
    assert_eq!(fixture.samples(), 0);
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
}

// Long snapshots must never overlap or cause queued catch-up bursts; shutdown drains one.
#[tokio::test(start_paused = true)]
async fn slow_sampling_requires_three_valid_samples_even_after_duration() {
    let mut fixture = Fixture::new(vec![
        snapshot(&[5_100]),
        snapshot(&[5_200]),
        snapshot(&[5_300]),
    ])
    .await;
    let gate = Arc::new(Semaphore::new(1));
    fixture.factory.sample_gate = Some(gate.clone());
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    settle().await;
    fixture.tick().await;
    until(|| fixture.samples() == 2).await;
    tokio::time::advance(Duration::from_secs(20)).await;
    gate.add_permits(1);
    until(|| fixture.samples() == 3).await;
    assert!(
        fixture.messages().is_empty(),
        "two valid samples must not notify"
    );
    gate.add_permits(1);
    until(|| fixture.messages().len() == 1).await;
    handle.shutdown().await;
}

// Long snapshots must never overlap or cause queued catch-up bursts; shutdown drains one.
#[tokio::test(start_paused = true)]
async fn slow_sampling_is_serial_skips_missed_ticks_and_drains_on_shutdown() {
    let mut fixture = Fixture::new(vec![]).await;
    let gate = Arc::new(Semaphore::new(0));
    fixture.factory.sample_gate = Some(gate.clone());
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    tokio::time::advance(Duration::from_secs(26)).await;
    settle().await;
    assert_eq!(fixture.samples(), 1);
    gate.add_permits(100);
    until(|| fixture.samples() == 2).await;
    settle().await;
    assert_eq!(fixture.samples(), 2);
    assert_eq!(fixture.factory.max_active.load(Ordering::SeqCst), 1);
    gate.forget_permits(gate.available_permits());
    fixture.tick().await;
    until(|| fixture.samples() == 3).await;
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    assert!(!shutdown.is_finished());
    gate.add_permits(1);
    shutdown.await.unwrap();
    assert_eq!(fixture.samples(), 3);
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
}

// A pending send must finish before client destruction and shutdown completion.
#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_send_and_blocking_client_drop() {
    let mut fixture = Fixture::new(vec![
        snapshot(&[5_100]),
        snapshot(&[5_200]),
        snapshot(&[5_300]),
    ])
    .await;
    let send_gate = Arc::new(Semaphore::new(0));
    let drop_gate = Arc::new(DropGate::default());
    fixture.factory.drop_gate = Some(drop_gate.clone());
    fixture.sender = Arc::new(FakeSender {
        gate: Some(send_gate.clone()),
        ..Default::default()
    });
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    fixture.tick().await;
    fixture.tick().await;
    until(|| fixture.messages().len() == 1).await;
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    assert!(!shutdown.is_finished());
    assert!(!drop_gate.started.load(Ordering::SeqCst));
    send_gate.add_permits(1);
    until(|| drop_gate.started.load(Ordering::SeqCst)).await;
    assert!(!shutdown.is_finished());
    drop_gate.release();
    shutdown.await.unwrap();
    assert_eq!(fixture.samples(), 3);
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
}

// Replacement must not create a second client until the prior native destructor finishes.
#[tokio::test(start_paused = true)]
async fn configuration_replacement_waits_for_old_client_drop() {
    let mut fixture = Fixture::new(vec![]).await;
    let drop_gate = Arc::new(DropGate::default());
    fixture.factory.drop_gate = Some(drop_gate.clone());
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    fixture.enable().await;
    fixture.tick().await;
    until(|| drop_gate.started.load(Ordering::SeqCst)).await;
    assert_eq!(fixture.created(), 1);
    drop_gate.release();
    until(|| fixture.created() == 2).await;
    handle.shutdown().await;
}

// A second save while native destruction is blocked must revoke replacement
// eligibility before any new client is accepted, including between reconcile ticks.
#[tokio::test(start_paused = true)]
async fn replacement_rechecks_eligibility_after_blocked_drop() {
    for action in 0..3 {
        let mut fixture = Fixture::new(vec![]).await;
        let sentinel = fixture
            .clusters
            .create(&ClusterInput::plaintext(
                "sentinel",
                "sentinel.invalid:9092",
            ))
            .await
            .unwrap();
        let drop_gate = Arc::new(DropGate::default());
        fixture.factory.drop_gate = Some(drop_gate.clone());
        fixture.enable().await;
        let handle = fixture.start();
        until(|| fixture.samples() == 1).await;
        fixture.enable().await;
        fixture.tick().await;
        until(|| drop_gate.started.load(Ordering::SeqCst)).await;
        match action {
            0 => {
                fixture
                    .settings
                    .replace(&fixture.cluster_id, None, true, &[rule(true)])
                    .await
                    .unwrap();
            }
            1 => {
                fixture
                    .settings
                    .replace(&fixture.cluster_id, None, false, &[rule(false)])
                    .await
                    .unwrap();
            }
            _ => {
                fixture.clusters.delete(&fixture.cluster_id).await.unwrap();
            }
        }
        fixture
            .settings
            .replace(&sentinel.id, Some(TEST_ALERT_TOKEN), false, &[rule(true)])
            .await
            .unwrap();
        drop_gate.release();
        until(|| fixture.factory.dropped.load(Ordering::SeqCst) == 1).await;
        fixture.tick().await;
        // The sentinel is an observable reconciliation barrier: reaching it
        // means the coordinator has resumed after the blocked destruction.
        until(|| {
            fixture
                .factory
                .configs
                .lock()
                .unwrap()
                .iter()
                .any(|config| config.bootstrap_servers == "sentinel.invalid:9092")
        })
        .await;
        let revoked_client_creations = fixture
            .factory
            .configs
            .lock()
            .unwrap()
            .iter()
            .filter(|config| config.bootstrap_servers == "fixture.invalid:9092")
            .count();
        assert_eq!(
            revoked_client_creations, 1,
            "revoked worker was replaced for action {action}"
        );
        handle.shutdown().await;
    }
}

// A replacement must combine current alert rules, cluster name and connection
// settings, rather than old alert metadata with newly read connection settings.
#[tokio::test(start_paused = true)]
async fn replacement_uses_latest_alert_and_cluster_revisions_after_blocked_drop() {
    let mut fixture = Fixture::new(vec![
        snapshot(&[5_100]),
        snapshot(&[5_200]),
        snapshot(&[5_300]),
        snapshot(&[5_400]),
        snapshot(&[5_500]),
        snapshot(&[5_600]),
    ])
    .await;
    let drop_gate = Arc::new(DropGate::default());
    fixture.factory.drop_gate = Some(drop_gate.clone());
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    fixture.enable().await;
    fixture.tick().await;
    until(|| drop_gate.started.load(Ordering::SeqCst)).await;
    let mut latest_rule = rule(true);
    latest_rule.lag_threshold = 0;
    fixture
        .settings
        .replace(&fixture.cluster_id, None, false, &[latest_rule])
        .await
        .unwrap();
    fixture
        .clusters
        .update(
            &fixture.cluster_id,
            &ClusterInput::plaintext("最新集群名称", "latest.invalid:9092"),
        )
        .await
        .unwrap();
    let old_samples = fixture.samples();
    drop_gate.release();
    until(|| fixture.created() == 2).await;
    until(|| fixture.samples() > old_samples).await;
    assert_eq!(
        fixture.factory.configs.lock().unwrap()[1].bootstrap_servers,
        "latest.invalid:9092"
    );
    fixture.tick().await;
    fixture.tick().await;
    assert_eq!(
        fixture.created(),
        2,
        "replacement must not require another rebuild for changes saved during drop"
    );
    assert_eq!(fixture.messages().len(), 1);
    assert!(fixture.messages()[0].contains("集群：最新集群名称"));
    assert!(fixture.messages()[0].contains("阈值：0"));
    handle.shutdown().await;
}

// Lost access to persisted eligibility must stop an existing worker, not leave
// it sampling forever with the last known token and rules.
#[tokio::test(start_paused = true)]
async fn repository_read_failure_stops_existing_worker() {
    let fixture = Fixture::new(vec![]).await;
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    fixture.pool.close().await;
    fixture.tick().await;
    until(|| fixture.factory.dropped.load(Ordering::SeqCst) == 1).await;
    let samples = fixture.samples();
    fixture.tick().await;
    assert_eq!(fixture.samples(), samples);
    assert_eq!(fixture.created(), 1);
    handle.shutdown().await;
}

// A slow send for the first rule must not age the same snapshot for later rules.
#[tokio::test(start_paused = true)]
async fn all_rules_observe_the_same_sampling_time_despite_a_slow_send() {
    let mut snapshots = vec![snapshot(&[5_100]), snapshot(&[5_200]), snapshot(&[5_300])];
    for snapshot in &mut snapshots {
        let mut second = snapshot.consumer_groups[0].clone();
        second.group_id = "second-group".into();
        snapshot.consumer_groups.push(second);
    }
    let mut fixture = Fixture::new(snapshots).await;
    let sample_gate = Arc::new(Semaphore::new(3));
    fixture.factory.sample_gate = Some(sample_gate.clone());
    let send_gate = Arc::new(Semaphore::new(0));
    fixture.sender = Arc::new(FakeSender {
        gate: Some(send_gate.clone()),
        ..Default::default()
    });
    let mut second_rule = rule(true);
    second_rule.group_id = "second-group".into();
    second_rule.growth_seconds = 15;
    fixture
        .settings
        .replace(
            &fixture.cluster_id,
            Some(TEST_ALERT_TOKEN),
            false,
            &[rule(true), second_rule],
        )
        .await
        .unwrap();
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    fixture.tick().await;
    fixture.tick().await;
    until(|| fixture.messages().len() == 1).await;
    fixture.tick().await;
    send_gate.add_permits(2);
    until(|| fixture.samples() == 4).await;
    assert_eq!(
        fixture.messages().len(),
        1,
        "send delay must not make an old sample satisfy another rule's duration"
    );
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    sample_gate.add_permits(1);
    shutdown.await.unwrap();
}
