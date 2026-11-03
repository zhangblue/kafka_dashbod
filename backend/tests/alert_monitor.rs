use std::{
    collections::{HashMap, VecDeque},
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

// 仅在隔离探针子进程中触发发送器断言失败，供诊断脱敏测试检查。
#[tokio::test]
async fn monitor_token_assertion_failure_probe() {
    if std::env::var_os("MONITOR_ASSERTION_FAILURE_PROBE").is_none() {
        return;
    }
    let sender = FakeSender::default();
    let _ = sender.send_text(FAILURE_PROBE_TOKEN, "probe").await;
}

// Run the real sender assertion in isolation, keeping all failed-test output private.
// 验证真实失败断言的子进程输出不包含凭据，防止测试诊断泄密。
#[test]
fn monitor_sensitive_assertion_diagnostics_never_echo_tokens() {
    // 把预期失败隔离在子进程并捕获其全部输出，避免探针凭据混入主测试诊断。
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
    // 在测试观察完阻塞析构后释放条件变量，也供失败清理阶段解锁。
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
    live: Arc<AtomicUsize>,
    max_live: Arc<AtomicUsize>,
    create_gate: Option<Arc<Semaphore>>,
    sample_gate: Option<Arc<Semaphore>>,
    drop_gate: Option<Arc<DropGate>>,
}

struct FakeClient {
    factory: FakeFactory,
}

impl Drop for FakeClient {
    // 在客户端析构阶段按闸门模拟原生阻塞，结束后更新存活计数供停机断言。
    fn drop(&mut self) {
        if let Some(gate) = &self.factory.drop_gate {
            // 先发布已进入析构状态再等待闸门，让测试可断言停机尚未完成。
            gate.started.store(true, Ordering::SeqCst);
            let mut released = gate.released.lock().unwrap();
            while !*released {
                released = gate.changed.wait(released).unwrap();
            }
        }
        self.factory.dropped.fetch_add(1, Ordering::SeqCst);
        self.factory.live.fetch_sub(1, Ordering::SeqCst);
    }
}

impl SessionClientFactory for FakeFactory {
    type Client = FakeClient;

    // 在后台建连阶段记录配置和并发客户端数，并用可控闸门模拟慢创建。
    async fn create(&self, config: DecryptedClusterConfig) -> Result<FakeClient, SessionError> {
        let live = self.live.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_live.fetch_max(live, Ordering::SeqCst);
        self.configs.lock().unwrap().push(config);
        if let Some(gate) = &self.create_gate {
            gate.acquire().await.unwrap().forget();
        }
        Ok(FakeClient {
            factory: self.clone(),
        })
    }

    // 在采样阶段记录并发数并返回预设序列，可用闸门模拟慢采集或注入错误。
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

#[derive(Clone)]
struct MultiFactory(Arc<HashMap<String, FakeFactory>>);

impl SessionClientFactory for MultiFactory {
    type Client = FakeClient;

    // 在多集群建连阶段按连接地址选择独立 fake，观察集群之间是否相互阻塞。
    async fn create(&self, config: DecryptedClusterConfig) -> Result<FakeClient, SessionError> {
        self.0
            .get(&config.bootstrap_servers)
            .expect("test cluster must have a configured factory")
            .create(config)
            .await
    }

    // 在多集群采样阶段转交给客户端所属 fake，保留各集群独立的计数和阻塞。
    async fn snapshot(
        &self,
        client: &mut FakeClient,
        id: &str,
    ) -> Result<ClusterSnapshot, SessionError> {
        client.factory.clone().snapshot(client, id).await
    }
}

// Also release gates when a regression assertion panics.
struct ReleaseDropGates(Vec<Arc<DropGate>>);

impl Drop for ReleaseDropGates {
    // 在测试退出或断言展开阶段释放全部析构闸门，避免失败路径挂住运行时。
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.release();
        }
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
    // 在告警发送阶段检查凭据但只输出固定诊断，记录消息并注入延迟或网络失败。
    async fn send_text(&self, token: &str, text: &str) -> Result<(), AlertSendError> {
        let expected_token = TEST_ALERT_TOKEN;
        // 先把敏感比较转换为布尔值再断言，失败消息不得携带任一参与比较的 Token。
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
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
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
    // 在暂停时间测试准备阶段保持调度器可运行，避免数据库线程等待导致时间自动前进。
    fn new() -> Self {
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}
impl Drop for Awake {
    // 在夹具退出时取消保持唤醒的任务，避免后台循环遗留。
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
    // 在夹具失败清理阶段释放创建、采样、发送及析构闸门，保证任务能够退出。
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
    // 在监控测试准备阶段创建内存仓储、有效集群和可控采样与发送 fake。
    async fn new(snapshots: Vec<ClusterSnapshot>) -> Self {
        // 先保持调度器唤醒再访问 SQLite，避免暂停时钟因外部数据库线程而自动前进。
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

    // 在监控启动或重配置阶段保存启用规则，使测试集群满足后台监控资格。
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

    // 在配置准备完成后启动受控监控器，将关闭句柄交给测试等待清理。
    fn start(&self) -> AlertMonitorHandle {
        monitor::start(
            self.clusters.clone(),
            self.settings.clone(),
            self.factory.clone(),
            self.sender.clone(),
            PERIOD,
        )
    }

    // 在等待及断言阶段读取创建次数，判断是否发生多余的客户端替换。
    fn created(&self) -> usize {
        self.factory.configs.lock().unwrap().len()
    }
    // 在等待及断言阶段读取采样次数，观察串行采集和停止行为。
    fn samples(&self) -> usize {
        self.factory.samples.load(Ordering::SeqCst)
    }
    // 在发送断言阶段复制已记录消息，避免持锁跨越异步等待。
    fn messages(&self) -> Vec<String> {
        self.sender.messages.lock().unwrap().clone()
    }

    // 在暂停时间的测试中推进一个采样周期，再让异步协调工作获得执行机会。
    async fn tick(&self) {
        tokio::time::advance(PERIOD).await;
        settle().await;
    }
}

// 在告警测试准备阶段构造指定启用状态的有效增长规则。
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

// 在监控测试准备阶段按分区 Lag 构造完整有效快照。
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

// 在不推进虚拟时间的阶段反复让出执行权，让已就绪的后台任务完成。
async fn settle() {
    for _ in 0..1_000 {
        tokio::task::yield_now().await;
    }
}

// 在并发测试阶段有界等待可观察状态，避免依赖真实睡眠或推进采样时间。
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
// 验证完整分区 Lag 总和持续增长只发送一次，防止部分求和或重复通知。
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
// 验证不完整、未知、缺失及不匹配样本中断增长窗口，不能折算为零。
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
// 验证缺少 Token 或启用规则时不创建客户端，避免无效后台连接。
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
// 验证发送完成后等待三十秒才重试，且同轮最多尝试三次。
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
// 验证采集失败中断连续增长，恢复后须重新积累有效样本。
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
// 验证告警或连接配置变化都替换工作器，避免继续使用旧配置。
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
// 验证清除 Token、停用规则或删除集群会停止采样并释放客户端。
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
// 验证停机等待已接受的创建完成后直接释放，不能遗留创建或继续采样。
#[tokio::test(start_paused = true)]
async fn shutdown_waits_for_creation_then_drops_without_sampling() {
    let mut fixture = Fixture::new(vec![]).await;
    let gate = Arc::new(Semaphore::new(0));
    fixture.factory.create_gate = Some(gate.clone());
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.created() == 1).await;
    // 并发请求停机后先观察其等待状态，再释放受控闸门，验证在途工作确实被排空。
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    assert!(!shutdown.is_finished());
    gate.add_permits(1);
    shutdown.await.unwrap();
    assert_eq!(fixture.samples(), 0);
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
}

// Long snapshots must never overlap or cause queued catch-up bursts; shutdown drains one.
// 验证慢采集即使跨过持续时间也需要三个有效样本，防止样本不足误报。
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
// 验证慢采集串行且跳过错过的周期，停机必须等待当前采集释放。
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
    // 并发请求停机后先观察其等待状态，再释放受控闸门，验证在途工作确实被排空。
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    assert!(!shutdown.is_finished());
    gate.add_permits(1);
    shutdown.await.unwrap();
    assert_eq!(fixture.samples(), 3);
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
}

// A pending send must finish before client destruction and shutdown completion.
// 验证停机先等待发送再等待阻塞析构，防止提前宣告资源已释放。
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
    // 并发请求停机后先观察其等待状态，再释放受控闸门，验证在途工作确实被排空。
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    assert!(!shutdown.is_finished());
    assert!(!drop_gate.started.load(Ordering::SeqCst));
    // 先让在途发送结束，确认进入析构且停机仍阻塞，再单独放行客户端析构。
    send_gate.add_permits(1);
    until(|| drop_gate.started.load(Ordering::SeqCst)).await;
    assert!(!shutdown.is_finished());
    drop_gate.release();
    shutdown.await.unwrap();
    assert_eq!(fixture.samples(), 3);
    assert_eq!(fixture.factory.dropped.load(Ordering::SeqCst), 1);
}

// Replacement must not create a second client until the prior native destructor finishes.
// 验证旧原生客户端析构完成前不创建替代客户端，防止同集群连接重叠。
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

// Retiring one cluster must not freeze revocation, replacement or new workers elsewhere.
// 验证单集群析构阻塞不妨碍其他集群协调，停机仍等待全部资源。
#[tokio::test(start_paused = true)]
async fn blocked_cluster_drop_does_not_block_other_clusters_and_shutdown_drains_all() {
    for action in 0..3 {
        let mut fixture = Fixture::new(vec![]).await;
        let a_drop = Arc::new(DropGate::default());
        let c_drop = Arc::new(DropGate::default());
        // 在引入阻塞析构前安装失败清理 guard，保证中途断言展开也能解除所有闸门。
        let _release = ReleaseDropGates(vec![a_drop.clone(), c_drop.clone()]);
        fixture.factory.drop_gate = Some(a_drop.clone());
        let b_sample = Arc::new(Semaphore::new(2));
        let b_factory = FakeFactory {
            snapshots: Arc::new(Mutex::new(VecDeque::from([
                Ok(snapshot(&[5_100])),
                Ok(snapshot(&[5_200])),
                Ok(snapshot(&[5_300])),
            ]))),
            sample_gate: Some(b_sample.clone()),
            ..Default::default()
        };
        let c_factory = FakeFactory {
            drop_gate: Some(c_drop.clone()),
            ..Default::default()
        };
        let b = fixture
            .clusters
            .create(&ClusterInput::plaintext("B", "b.invalid:9092"))
            .await
            .unwrap();
        let c = fixture
            .clusters
            .create(&ClusterInput::plaintext("C", "c.invalid:9092"))
            .await
            .unwrap();
        fixture.enable().await;
        fixture
            .settings
            .replace(&b.id, Some(TEST_ALERT_TOKEN), false, &[rule(true)])
            .await
            .unwrap();
        let factory = MultiFactory(Arc::new(HashMap::from([
            ("fixture.invalid:9092".into(), fixture.factory.clone()),
            ("b.invalid:9092".into(), b_factory.clone()),
            ("c.invalid:9092".into(), c_factory.clone()),
        ])));
        let handle = monitor::start(
            fixture.clusters.clone(),
            fixture.settings.clone(),
            factory,
            fixture.sender.clone(),
            PERIOD,
        );
        until(|| fixture.samples() == 1 && b_factory.samples.load(Ordering::SeqCst) == 1).await;
        fixture.enable().await;
        fixture.tick().await;
        until(|| a_drop.started.load(Ordering::SeqCst)).await;
        until(|| b_factory.samples.load(Ordering::SeqCst) == 2).await;
        match action {
            0 => {
                fixture
                    .settings
                    .replace(&b.id, None, true, &[rule(true)])
                    .await
                    .unwrap();
            }
            1 => {
                fixture.clusters.delete(&b.id).await.unwrap();
            }
            _ => {
                let mut updated = rule(true);
                updated.lag_threshold = 9_000_000;
                fixture
                    .settings
                    .replace(&b.id, None, false, &[updated])
                    .await
                    .unwrap();
            }
        }
        fixture
            .settings
            .replace(&c.id, Some(TEST_ALERT_TOKEN), false, &[rule(true)])
            .await
            .unwrap();
        fixture.tick().await;
        // C is a reconciliation barrier while A's native destructor is still blocked.
        until(|| c_factory.samples.load(Ordering::SeqCst) == 1).await;
        b_sample.add_permits(1);
        until(|| b_factory.dropped.load(Ordering::SeqCst) == 1).await;
        assert!(
            fixture.messages().is_empty(),
            "revoked configuration must not send"
        );
        assert_eq!(
            fixture.created(),
            1,
            "A must not overlap its retiring client"
        );
        if action == 2 {
            until(|| b_factory.configs.lock().unwrap().len() == 2).await;
        } else {
            assert_eq!(b_factory.configs.lock().unwrap().len(), 1);
        }
        // 并发请求停机后先观察其等待状态，再释放受控闸门，验证在途工作确实被排空。
        let shutdown = tokio::spawn(async move { handle.shutdown().await });
        b_sample.add_permits(100);
        until(|| c_drop.started.load(Ordering::SeqCst)).await;
        assert!(
            !shutdown.is_finished(),
            "shutdown must await active client destruction"
        );
        // 先只释放活跃集群的析构，验证停机还等待更早退休的客户端。
        c_drop.release();
        until(|| c_factory.dropped.load(Ordering::SeqCst) == 1).await;
        assert!(
            !shutdown.is_finished(),
            "shutdown must await the retiring client"
        );
        // 最后释放退休客户端并等待停机，检查所有集群的存活计数归零且无重叠。
        a_drop.release();
        shutdown.await.unwrap();
        for factory in [&fixture.factory, &b_factory, &c_factory] {
            assert_eq!(factory.live.load(Ordering::SeqCst), 0);
            assert_eq!(factory.max_live.load(Ordering::SeqCst), 1);
        }
    }
}

// A second save while native destruction is blocked must revoke replacement
// eligibility before any new client is accepted, including between reconcile ticks.
// 验证阻塞析构期间撤销资格后不会补建客户端，包括协调周期之间的变更。
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
// 验证阻塞析构后的替代工作器同时使用最新规则、名称与连接配置。
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
// 验证无法读取持久化资格时停止已有工作器，避免沿用旧凭据永久采样。
#[tokio::test(start_paused = true)]
async fn repository_read_failure_stops_existing_worker() {
    let fixture = Fixture::new(vec![]).await;
    fixture.enable().await;
    let handle = fixture.start();
    until(|| fixture.samples() == 1).await;
    // 关闭临时连接池注入资格读取失败，再推进协调周期，检查旧工作器不会无限沿用缓存。
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
// 验证首条规则发送变慢不会让同一快照在后续规则中被错误计时。
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
    // 结束用例时发起停机并让出调度，再放行采样闸门，使在途采样完成后可等待停机收尾。
    let shutdown = tokio::spawn(async move { handle.shutdown().await });
    settle().await;
    sample_gate.add_permits(1);
    shutdown.await.unwrap();
}
