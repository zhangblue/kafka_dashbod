use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{
    sync::{Mutex, watch},
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    db::{clusters::ClusterRepository, feishu_alerts::FeishuAlertRepository},
    domain::{
        cluster::DecryptedClusterConfig,
        feishu_alert::{AlertDecision, AlertState, EnabledAlertCluster, FeishuAlertRule},
        snapshot::ClusterSnapshot,
    },
    notifications::feishu::{AlertSender, GrowthAlertMessage, format_growth_alert},
    sessions::manager::SessionClientFactory,
};

pub const MONITOR_INTERVAL: Duration = Duration::from_secs(5);

struct MonitorTask {
    stop: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

/// Clones share one shutdown operation. Cancelling a caller waiting for shutdown
/// does not cancel the monitor or discard its join handle.
#[derive(Clone)]
pub struct AlertMonitorHandle(Arc<MonitorTask>);

impl AlertMonitorHandle {
    pub async fn shutdown(&self) {
        self.0.stop.send_replace(true);
        let mut task = self.0.task.lock().await;
        if let Some(task) = task.as_mut() {
            let _ = task.await;
        }
        task.take();
    }
}

pub fn start<F: SessionClientFactory>(
    clusters: ClusterRepository,
    settings: FeishuAlertRepository,
    factory: F,
    sender: Arc<dyn AlertSender>,
    reconcile_interval: Duration,
) -> AlertMonitorHandle {
    let (stop, stopped) = watch::channel(false);
    let task = tokio::spawn(coordinate(
        clusters,
        settings,
        factory,
        sender,
        reconcile_interval,
        stopped,
    ));
    AlertMonitorHandle(Arc::new(MonitorTask {
        stop,
        task: Mutex::new(Some(task)),
    }))
}

struct Worker {
    // Neither revision contains credentials. Do not log configuration values.
    settings_revision: String,
    cluster_revision: String,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

fn stopped(stop: &watch::Receiver<bool>) -> bool {
    *stop.borrow() || stop.has_changed().is_err()
}

async fn coordinate<F: SessionClientFactory>(
    clusters: ClusterRepository,
    settings: FeishuAlertRepository,
    factory: F,
    sender: Arc<dyn AlertSender>,
    reconcile_interval: Duration,
    mut stop: watch::Receiver<bool>,
) {
    let mut workers: HashMap<String, Worker> = HashMap::new();
    let mut timer = tokio::time::interval(reconcile_interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = stop.changed() => break,
            _ = timer.tick() => {},
        }
        if stopped(&stop) {
            break;
        }
        let mut desired = HashMap::new();
        match settings.enabled_clusters().await {
            Ok(enabled) => {
                for setting in enabled {
                    if stopped(&stop) {
                        break;
                    }
                    match clusters.get(&setting.cluster_id).await {
                        Ok(Some(cluster)) => {
                            desired.insert(setting.cluster_id.clone(), (setting, cluster));
                        }
                        Ok(None) => {}
                        Err(_) => {
                            tracing::warn!(cluster_id = %setting.cluster_id, code = "ALERT_CONFIG_READ_FAILED")
                        }
                    }
                }
            }
            // Fail closed when eligibility cannot be established; never keep a
            // stale credential/configuration running indefinitely after a DB failure.
            Err(_) => tracing::warn!(code = "ALERT_CONFIG_READ_FAILED"),
        }

        let obsolete: Vec<_> = workers
            .iter()
            .filter_map(|(id, worker)| {
                let unchanged = desired.get(id).is_some_and(|(setting, cluster)| {
                    setting.revision == worker.settings_revision
                        && cluster.updated_at == worker.cluster_revision
                });
                (!unchanged || worker.task.is_finished() || stopped(&stop)).then(|| id.clone())
            })
            .collect();
        let mut retiring = Vec::new();
        for id in obsolete {
            if let Some(worker) = workers.remove(&id) {
                worker.stop.send_replace(true);
                retiring.push(worker.task);
            }
        }
        // Signal every retiring worker before waiting on any native operation.
        for task in retiring {
            let _ = task.await;
        }
        for (id, (setting, cluster)) in desired {
            if stopped(&stop) {
                break;
            }
            if workers.contains_key(&id) {
                continue;
            }
            let config = match clusters.decrypted_config(&id).await {
                Ok(Some(config)) => config,
                Ok(None) => continue,
                Err(_) => {
                    tracing::warn!(cluster_id = %id, code = "ALERT_CONFIG_READ_FAILED");
                    continue;
                }
            };
            if stopped(&stop) {
                break;
            }
            let (worker_stop, worker_stopped) = watch::channel(false);
            let worker = Worker {
                settings_revision: setting.revision.clone(),
                cluster_revision: cluster.updated_at,
                stop: worker_stop,
                task: tokio::spawn(run_worker(
                    setting,
                    cluster.name,
                    config,
                    factory.clone(),
                    sender.clone(),
                    worker_stopped,
                    stop.clone(),
                )),
            };
            workers.insert(id, worker);
        }
    }
    for worker in workers.values() {
        worker.stop.send_replace(true);
    }
    for (_, worker) in workers {
        let _ = worker.task.await;
    }
}

struct RuleState {
    rule: FeishuAlertRule,
    state: AlertState,
    attempt: u8,
}

async fn run_worker<F: SessionClientFactory>(
    setting: EnabledAlertCluster,
    cluster_name: String,
    config: DecryptedClusterConfig,
    factory: F,
    sender: Arc<dyn AlertSender>,
    mut stop: watch::Receiver<bool>,
    mut global_stop: watch::Receiver<bool>,
) {
    if stopped(&stop) || stopped(&global_stop) {
        return;
    }
    // Creation/snapshot/send are deliberately outside select: accepted native
    // work cannot be cancelled, and a delivery result must reach AlertState.
    let mut client = match factory.create(config).await {
        Ok(client) => client,
        Err(_) => {
            tracing::warn!(cluster_id = %setting.cluster_id, code = "ALERT_CLIENT_CREATE_FAILED");
            return;
        }
    };
    let mut rules: Vec<_> = setting
        .rules
        .into_iter()
        .map(|rule| RuleState {
            rule,
            state: AlertState::default(),
            attempt: 0,
        })
        .collect();
    let mut timer = tokio::time::interval(MONITOR_INTERVAL);
    timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        if stopped(&stop) || stopped(&global_stop) {
            break;
        }
        tokio::select! {
            biased;
            _ = global_stop.changed() => break,
            _ = stop.changed() => break,
            _ = timer.tick() => {},
        }
        if stopped(&stop) || stopped(&global_stop) {
            break;
        }
        let snapshot = factory.snapshot(&mut client, &setting.cluster_id).await;
        // Every rule evaluates this sample at its collection completion time;
        // a slow notification for another rule must not extend its growth window.
        let observed_at = Instant::now().into_std();
        if snapshot.is_err() {
            tracing::warn!(cluster_id = %setting.cluster_id, code = "ALERT_COLLECTION_FAILED");
        }
        for runtime in &mut rules {
            // Do not accept a new send after stopping; an already accepted send
            // below is always awaited and recorded before the next observation.
            if stopped(&stop) || stopped(&global_stop) {
                break;
            }
            let total_lag = snapshot
                .as_ref()
                .ok()
                .and_then(|snapshot| complete_total_lag(snapshot, &runtime.rule));
            if total_lag.is_some_and(|lag| lag <= runtime.rule.lag_threshold) {
                runtime.attempt = 0;
            }
            let (Ok(growth_seconds), Ok(threshold)) = (
                u64::try_from(runtime.rule.growth_seconds),
                u64::try_from(runtime.rule.lag_threshold),
            ) else {
                continue;
            };
            let duration = Duration::from_secs(growth_seconds);
            if runtime
                .state
                .observe(observed_at, total_lag, runtime.rule.lag_threshold, duration)
                != AlertDecision::Send
            {
                continue;
            }
            let text = format_growth_alert(&GrowthAlertMessage {
                cluster_name: &cluster_name,
                topic: &runtime.rule.topic,
                group_id: &runtime.rule.group_id,
                threshold,
                // complete_total_lag only returns nonnegative checked sums.
                total_lag: u64::try_from(total_lag.expect("send requires complete Lag"))
                    .expect("complete Lag is nonnegative"),
                growth_seconds: duration.as_secs(),
                detected_at: chrono::Local::now().fixed_offset(),
            });
            runtime.attempt += 1;
            let result = sender.send_text(&setting.access_token, &text).await;
            runtime
                .state
                .record_attempt(Instant::now().into_std(), result.is_ok());
            // Only a stable classification is logged; never include native/HTTP
            // errors, response bodies, message text, cluster names, or credentials.
            let code = if result.is_ok() {
                "ALERT_SENT"
            } else {
                "ALERT_SEND_FAILED"
            };
            tracing::info!(cluster_id = %setting.cluster_id, rule_id = %runtime.rule.id, attempt = runtime.attempt, code);
        }
    }
    // Native destructors may join poll threads. The coordinator awaits this
    // blocking drop before replacing the worker or reporting shutdown complete.
    let _ = tokio::task::spawn_blocking(move || drop(client)).await;
}

fn complete_total_lag(snapshot: &ClusterSnapshot, rule: &FeishuAlertRule) -> Option<i64> {
    let row = snapshot
        .consumer_groups
        .iter()
        .find(|row| row.group_id == rule.group_id && row.topic == rule.topic)?;
    if row.incomplete || row.unknown_partition_count != 0 || row.partitions.is_empty() {
        return None;
    }
    let total = row.partitions.iter().try_fold(0_u64, |total, partition| {
        partition.high?;
        partition.committed_offset?;
        total.checked_add(partition.lag?)
    })?;
    i64::try_from(total).ok()
}
