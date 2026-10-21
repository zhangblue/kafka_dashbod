# 飞书 Lag 持续增长告警实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 为每个 Kafka 集群配置一个飞书机器人 Token，并按 Consumer Group + Topic 在后台持续监控全部 Partition Lag 总和，超过独立阈值并持续增长达到设定时间后发送一次飞书通知。

**架构：** SQLite 分离保存集群级 Token 与组合级告警规则；Axum 提供读取、整体保存和测试通知接口。独立 `AlertMonitor` 使用与页面会话相同的只读 Kafka 客户端工厂，但不占用短租约；每个启用集群拥有一个常驻 worker，纯状态机负责增长、恢复和重试判定，飞书发送器只允许固定官方 HTTPS 地址。

**技术栈：** Rust 2024、Tokio、Axum 0.8、SQLx/SQLite、rust-rdkafka 0.39、reqwest 0.12.9（`json` + `rustls-tls`）、React、TypeScript、TanStack Query、Vitest、Testing Library、Playwright。

---

## 文件结构

### 后端新增

- `backend/migrations/0002_feishu_alerts.sql`：创建集群 Token 设置表与 Group + Topic 告警规则表。
- `backend/src/domain/feishu_alert.rs`：告警设置/规则类型、校验常量、纯增长与重试状态机。
- `backend/src/db/feishu_alerts.rs`：Token 与规则事务仓储、脱敏 DTO、后台启用配置读取。
- `backend/src/notifications/mod.rs`：通知模块导出。
- `backend/src/notifications/feishu.rs`：固定官方地址的飞书文本发送器、消息格式化与安全错误。
- `backend/src/api/feishu_alerts.rs`：GET、PUT 和测试通知接口。
- `backend/src/alerts/mod.rs`：后台告警模块导出。
- `backend/src/alerts/monitor.rs`：配置协调器、每集群 worker、采样过滤、生命周期和关闭等待。
- `backend/tests/feishu_alerts_api.rs`：仓储/API、Token 不回显、校验与测试发送集成测试。
- `backend/tests/alert_monitor.rs`：假 Kafka 与假飞书发送器驱动的后台监控集成测试。

### 后端修改

- `backend/Cargo.toml`、`Cargo.lock`：加入 reqwest 锁定依赖。
- `backend/src/domain/mod.rs`、`backend/src/db/mod.rs`、`backend/src/lib.rs`：导出新模块。
- `backend/src/api/mod.rs`：注入告警仓储与发送器，注册接口。
- `backend/src/main.rs`：启动 `AlertMonitor`，SIGINT/SIGTERM 时等待 monitor 与页面会话释放。
- `backend/tests/repositories.rs`：迁移、级联删除、事务替换和 Token 明文/公开脱敏约束。
- `backend/tests/server_process.rs`：无告警配置的正常启动与 SIGTERM 退出继续覆盖后台监控生命周期。

### 前端新增

- `frontend/src/features/feishu-alerts/FeishuAlertForm.tsx`：已保存掩码、新 Token 眼睛图标、规则表格与验证。
- `frontend/src/features/feishu-alerts/FeishuAlertForm.test.tsx`：交互、可访问性、阈值/时间和重复组合测试。

### 前端修改

- `frontend/src/api/types.ts`：飞书设置、规则、保存及测试请求类型。
- `frontend/src/api/client.ts`：飞书设置 GET/PUT/test 客户端。
- `frontend/src/features/clusters/ClustersPage.tsx`：集群卡片摘要、独立弹窗、配置会话和保存/测试流程。
- `frontend/src/features/clusters/ClustersPage.test.tsx`：完整页面集成与 Token 不回显测试。
- `frontend/src/index.css`：与已确认 Demo 一致的弹窗、统一输入框、紧凑数字列、眼睛图标和窄屏滚动。
- `frontend/e2e/kafka-dashboard.spec.ts`：真实保存规则、页面重开回显和测试通知 UI 验收。

### 文档与发行

- `README.md`：功能、判定口径、飞书配置、安全和后台连接说明。
- `packaging/README.txt`：离线部署后的中文配置与使用步骤、安全边界和故障排查。

## 参考资料

- 飞书官方自定义机器人文档规定 Webhook 为 `https://open.feishu.cn/open-apis/bot/v2/hook/<token>`，文本消息体为 `{"msg_type":"text","content":{"text":"..."}}`，成功业务码为 `code: 0`：[飞书自定义机器人使用指南](https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot)。
- reqwest 官方文档使用 `Client::builder().timeout(...)` 配置全局超时、`.json(...)` 发送 JSON，并可用 `error_for_status()` 拒绝非 2xx 响应；本实现还必须禁用重定向，避免固定官方地址被跳转绕过。

---

### 任务 1：持久化集群 Token 与组合告警规则

**文件：**
- 创建：`backend/migrations/0002_feishu_alerts.sql`
- 创建：`backend/src/domain/feishu_alert.rs`
- 创建：`backend/src/db/feishu_alerts.rs`
- 修改：`backend/src/domain/mod.rs`
- 修改：`backend/src/db/mod.rs`
- 修改：`backend/tests/repositories.rs`

- [ ] **步骤 1：编写失败的仓储测试**

在 `backend/tests/repositories.rs` 增加：保存 Token 和两条规则后公开读取只得到掩码；空 Token 更新保留旧值；清除 Token 保留规则；规则整体替换原子完成；删除集群级联删除两张表。

```rust
#[tokio::test]
async fn feishu_settings_replace_rules_without_exposing_token() {
    let (pool, clusters, _) = repositories().await;
    let alerts = FeishuAlertRepository::new(pool.clone());
    let cluster = clusters.create(&ClusterInput::plaintext("prod", "broker:9092")).await.unwrap();
    let saved = alerts.replace(&cluster.id, Some("token-secret"), false, &[
        FeishuAlertRuleInput { id: None, group_id: "orders".into(), topic: "events".into(), lag_threshold: 5000, growth_seconds: 60, enabled: true },
    ]).await.unwrap();
    assert!(saved.access_token_configured);
    assert_eq!(saved.masked_access_token.as_deref(), Some("••••••••cret"));
    assert!(!serde_json::to_string(&saved).unwrap().contains("token-secret"));
    assert_eq!(alerts.token(&cluster.id).await.unwrap().as_deref(), Some("token-secret"));
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p backend --test repositories feishu_settings -- --nocapture`

预期：FAIL，提示 `FeishuAlertRepository`、`FeishuAlertRuleInput` 或迁移表不存在。

- [ ] **步骤 3：创建迁移和领域类型**

`0002_feishu_alerts.sql` 使用以下约束：

```sql
CREATE TABLE feishu_alert_settings (
  cluster_id TEXT PRIMARY KEY REFERENCES clusters(id) ON DELETE CASCADE,
  access_token TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE feishu_alert_rules (
  id TEXT PRIMARY KEY,
  cluster_id TEXT NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
  group_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  lag_threshold INTEGER NOT NULL CHECK (lag_threshold >= 0),
  growth_seconds INTEGER NOT NULL CHECK (growth_seconds >= 10),
  enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(cluster_id, group_id, topic)
);
```

在领域文件定义 `MIN_GROWTH_SECONDS: i64 = 10`、`FeishuAlertRuleInput`、公开 `FeishuAlertRule`、公开 `FeishuAlertSettings` 和仅后台使用的 `EnabledAlertCluster`。所有含完整 Token 的类型手写 `Debug` 为 `[REDACTED]`，不得派生会泄漏字段的 `Debug`。

- [ ] **步骤 4：实现事务仓储**

`FeishuAlertRepository::replace` 必须：

1. 验证集群存在；
2. 在同一事务中合并新 Token/保留/清除语义；
3. 替换该集群全部规则；
4. 保留传入且属于该集群的现有规则 ID，新规则生成 UUID；
5. 返回只含配置标志、最多末 4 位掩码和规则的 DTO；
6. 提供 `token(cluster_id)` 给测试通知使用；
7. 提供 `enabled_clusters()` 给后台协调器使用，只有 Token 非空且存在启用规则才返回。

掩码函数示例：

```rust
fn mask_token(token: &str) -> String {
    let suffix: String = token.chars().rev().take(4).collect::<Vec<_>>().into_iter().rev().collect();
    format!("••••••••{suffix}")
}
```

- [ ] **步骤 5：运行仓储测试**

运行：`cargo test -p backend --test repositories -- --nocapture`

预期：PASS；现有 Kafka 凭据和 Lag 规则仓储测试保持通过。

- [ ] **步骤 6：提交**

```bash
git add backend/migrations/0002_feishu_alerts.sql backend/src/domain/feishu_alert.rs backend/src/domain/mod.rs backend/src/db/feishu_alerts.rs backend/src/db/mod.rs backend/tests/repositories.rs
git commit -m "feat: 持久化飞书告警设置"
```

---

### 任务 2：实现安全的飞书文本发送器

**文件：**
- 创建：`backend/src/notifications/mod.rs`
- 创建：`backend/src/notifications/feishu.rs`
- 修改：`backend/src/lib.rs`
- 修改：`backend/Cargo.toml`
- 修改：`Cargo.lock`

- [ ] **步骤 1：编写失败的发送器单元测试**

在 `backend/src/notifications/feishu.rs` 的测试模块覆盖：固定官方主机、Token 作为单一路径段编码、禁用跳转、5 秒超时、文本 JSON、HTTP 非成功、`code != 0`、超时错误分类，以及 `Debug`/错误文本不包含 Token 或响应正文。

```rust
#[tokio::test]
async fn text_payload_and_success_code_follow_feishu_contract() {
    let transport = RecordingTransport::responding(200, br#"{"code":0,"msg":"success"}"#);
    let sender = ReqwestFeishuSender::with_transport(transport.clone());
    sender.send("token/value", "Kafka test").await.unwrap();
    let request = transport.only_request();
    assert_eq!(request.host(), Some("open.feishu.cn"));
    assert!(request.path().ends_with("/token%2Fvalue"));
    assert_eq!(request.json, json!({"msg_type":"text","content":{"text":"Kafka test"}}));
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p backend notifications::feishu -- --nocapture`

预期：FAIL，提示通知模块或发送器不存在。

- [ ] **步骤 3：添加 reqwest 依赖并实现发送器**

在 `backend/Cargo.toml` 添加：

```toml
reqwest = { version = "0.12.9", default-features = false, features = ["json", "rustls-tls"] }
```

生产客户端固定配置：

```rust
let client = reqwest::Client::builder()
    .timeout(Duration::from_secs(5))
    .https_only(true)
    .redirect(reqwest::redirect::Policy::none())
    .build()?;
```

用 `reqwest::Url::parse("https://open.feishu.cn/open-apis/bot/v2/hook/")` 后通过 `path_segments_mut().push(token)` 添加 Token。POST JSON 后先拒绝非 2xx，再解析小型业务响应，只接受 `code == 0`；不得把 URL、Token、请求体或响应正文放入错误与日志。

定义对象安全接口供 API 和 monitor 注入：

```rust
#[async_trait]
pub trait AlertSender: Send + Sync {
    async fn send_text(&self, access_token: &str, text: &str) -> Result<(), AlertSendError>;
}
```

- [ ] **步骤 4：实现消息格式化并测试必填字段**

新增 `format_growth_alert` 和 `format_test_alert`。真实消息必须包含集群名、Topic、Consumer Group、阈值、全部 Partition Lag 总和、连续时间和带时区检测时间。

```rust
let text = format_growth_alert(&GrowthAlertMessage {
    cluster_name: "订单生产集群",
    topic: "orders",
    group_id: "order-service",
    threshold: 5_000,
    total_lag: 12_580,
    growth_seconds: 60,
    detected_at: fixed_time,
});
assert!(text.contains("当前积压量（全部 Partition Lag 总和）：12,580"));
```

- [ ] **步骤 5：运行测试与依赖检查**

运行：

```bash
cargo test -p backend notifications::feishu -- --nocapture
cargo tree -p backend -i openssl-sys
```

预期：发送器测试 PASS；reqwest 使用 rustls，不额外引入非预期的系统 OpenSSL 动态依赖。

- [ ] **步骤 6：提交**

```bash
git add backend/Cargo.toml Cargo.lock backend/src/lib.rs backend/src/notifications
git commit -m "feat: 添加安全飞书通知客户端"
```

---

### 任务 3：提供飞书告警配置与测试 API

**文件：**
- 创建：`backend/src/api/feishu_alerts.rs`
- 创建：`backend/tests/feishu_alerts_api.rs`
- 修改：`backend/src/api/mod.rs`
- 修改：`backend/tests/support/mod.rs`

- [ ] **步骤 1：编写失败的 API 测试**

使用记录消息但不联网的 `FakeAlertSender`，覆盖 GET、PUT、测试通知、缺失集群、Token 留空保留、清除、重复组合、空 Group/Topic、负阈值、9 秒拒绝、10 秒接受、超大请求和所有响应不泄漏 Token。

```rust
#[tokio::test]
async fn put_returns_masked_settings_and_test_uses_saved_token() {
    let fixture = test_app_with_sender(FakeAlertSender::default()).await;
    let id = create_cluster(&fixture.app, "prod").await;
    let response = request(&fixture.app, "PUT", &format!("/api/clusters/{id}/feishu-alert"), Some(json!({
        "accessToken":"fixture-token-secret",
        "clearAccessToken":false,
        "rules":[{"groupId":"g","topic":"t","lagThreshold":5000,"growthSeconds":10,"enabled":true}]
    }))).await;
    assert_eq!(response.status(), 200);
    let body = response_text(response).await;
    assert!(!body.contains("fixture-token-secret"));
    assert!(body.contains("••••••••cret"));
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p backend --test feishu_alerts_api -- --nocapture`

预期：FAIL，接口返回 404 或发送器注入方法不存在。

- [ ] **步骤 3：扩展 AppState 与路由**

给 `AppState` 增加 `FeishuAlertRepository` 和 `Arc<dyn AlertSender>`；`AppState::new` 使用生产发送器，测试提供 `with_alert_sender`。注册：

```rust
.route(
  "/api/clusters/{id}/feishu-alert",
  get(feishu_alerts::get).put(feishu_alerts::put),
)
.route(
  "/api/clusters/{id}/feishu-alert/test",
  post(feishu_alerts::test),
)
```

- [ ] **步骤 4：实现严格字段校验与安全测试通知**

PUT 在调用仓储前构建字段错误：

- `accessToken` 新值与 `clearAccessToken` 不能同时出现；
- Token 去除首尾空白后非空、长度不超过 512、无控制字符；
- `groupId`、`topic` 非空，同一组合唯一；
- `lagThreshold >= 0`；
- `growthSeconds >= 10`。

测试通知优先使用请求中的新 Token，否则读取已保存 Token；消息仅含“飞书通知测试”和集群名称。发送错误映射为固定 `FEISHU_SEND_FAILED`，响应和日志不包含远端内容。

- [ ] **步骤 5：运行 API 与日志安全回归**

运行：

```bash
cargo test -p backend --test feishu_alerts_api -- --nocapture
cargo test -p backend --test error_logging -- --nocapture
```

预期：全部 PASS；秘密扫描断言不发现测试 Token。

- [ ] **步骤 6：提交**

```bash
git add backend/src/api/mod.rs backend/src/api/feishu_alerts.rs backend/tests/feishu_alerts_api.rs backend/tests/support/mod.rs
git commit -m "feat: 提供飞书告警配置接口"
```

---

### 任务 4：实现总 Lag 持续增长与重试状态机

**文件：**
- 修改：`backend/src/domain/feishu_alert.rs`

- [ ] **步骤 1：编写表驱动失败测试**

覆盖：等于阈值不开始、超过阈值后增长、持平不算实际增长、下降重新计时、回到阈值恢复、无效总 Lag 重置、10 秒边界、一次成功通知、失败后 30 秒重试、最多 3 次、成功后停止。

```rust
#[test]
fn total_lag_must_exceed_threshold_and_grow_for_ten_seconds() {
    let start = Instant::now();
    let mut state = AlertState::default();
    assert_eq!(state.observe(start, Some(5_000), 5_000, Duration::from_secs(10)), AlertDecision::None);
    assert_eq!(state.observe(start + Duration::from_secs(5), Some(5_100), 5_000, Duration::from_secs(10)), AlertDecision::None);
    assert_eq!(state.observe(start + Duration::from_secs(10), Some(5_200), 5_000, Duration::from_secs(10)), AlertDecision::Send);
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p backend domain::feishu_alert -- --nocapture`

预期：FAIL，`AlertState`/`AlertDecision` 不存在。

- [ ] **步骤 3：实现纯状态机**

接口固定为：

```rust
pub enum AlertDecision { None, Send }
impl AlertState {
    pub fn observe(&mut self, now: Instant, total_lag: Option<i64>, threshold: i64, duration: Duration) -> AlertDecision;
    pub fn record_attempt(&mut self, now: Instant, succeeded: bool);
}
```

`None` 总 Lag代表任一 Partition 未提交、未知、失败或陈旧，立即清除连续候选；成功发送后保持已通知直到 `total_lag <= threshold`；失败后设置下一次为 `now + 30s`，第三次失败后抑制本轮后续发送。

- [ ] **步骤 4：运行领域测试**

运行：`cargo test -p backend domain::feishu_alert -- --nocapture`

预期：所有边界测试 PASS，测试使用暂停或显式 `Instant`，不实际 sleep。

- [ ] **步骤 5：提交**

```bash
git add backend/src/domain/feishu_alert.rs
git commit -m "feat: 实现 Lag 增长告警状态机"
```

---

### 任务 5：运行并安全关闭后台告警监控器

**文件：**
- 创建：`backend/src/alerts/mod.rs`
- 创建：`backend/src/alerts/monitor.rs`
- 创建：`backend/tests/alert_monitor.rs`
- 修改：`backend/src/lib.rs`
- 修改：`backend/src/main.rs`
- 修改：`backend/tests/server_process.rs`

- [ ] **步骤 1：编写失败的 monitor 集成测试**

实现假的 `SessionClientFactory`：按调用顺序返回快照；假的 `AlertSender` 记录文本。测试启用配置启动 worker、全部 Partition 总 Lag 求和、部分未知不发送、阈值/时间判定、每轮一次、失败重试、配置变更重建、清除 Token/删除集群停止、同集群不重叠和 shutdown 等待 client drop。

```rust
#[tokio::test(start_paused = true)]
async fn monitor_sends_once_for_complete_total_lag_growth() {
    let fixture = MonitorFixture::new(vec![snapshot(&[2_500, 2_600]), snapshot(&[2_700, 2_800]), snapshot(&[2_900, 3_000])]).await;
    fixture.enable_rule("g", "t", 5_000, 10).await;
    let handle = fixture.start();
    tokio::time::advance(Duration::from_secs(11)).await;
    assert_eq!(fixture.sender.messages().len(), 1);
    assert!(fixture.sender.messages()[0].contains("全部 Partition Lag 总和）：5,900"));
    handle.shutdown().await;
}
```

- [ ] **步骤 2：运行测试验证失败**

运行：`cargo test -p backend --test alert_monitor -- --nocapture`

预期：FAIL，后台 monitor 类型不存在。

- [ ] **步骤 3：实现协调器和每集群 worker**

定义：

```rust
pub struct AlertMonitorHandle { /* shutdown sender + join handle */ }
pub fn start<F: SessionClientFactory>(
    clusters: ClusterRepository,
    settings: FeishuAlertRepository,
    factory: F,
    sender: Arc<dyn AlertSender>,
    reconcile_interval: Duration,
) -> AlertMonitorHandle;
```

协调器每 5 秒读取 `enabled_clusters()`，按配置 revision 对比 worker：新增则启动，变化则先停止并 await 旧 worker，再启动新 worker，消失则停止。worker 创建一个只读 Kafka client，使用 interval 的 `MissedTickBehavior::Skip`，每次 snapshot 完成后才允许下一次采样。

- [ ] **步骤 4：实现完整总 Lag 过滤与发送**

对每条启用规则精确寻找 `snapshot.consumer_groups` 行，只有以下条件全部满足才向状态机传 `Some(row.lag)`：

```rust
let valid = !row.incomplete
    && row.unknown_partition_count == 0
    && row.partitions.iter().all(|p| p.high.is_some() && p.committed_offset.is_some() && p.lag.is_some());
```

否则传 `None`。发送后调用 `record_attempt`；日志只记录 cluster ID、规则 ID、尝试次数和安全错误码。worker 退出时通过 `spawn_blocking` 丢弃原生 client，协调器和主进程必须 await。

- [ ] **步骤 5：接入 main 的启动与关闭顺序**

`run()` 在路由启动前创建 monitor。关闭流程固定为：收到信号 → `monitor.shutdown().await` → `sessions.close_all().await` → Axum 退出；服务器错误路径同样关闭两者。无 Token 时 monitor 不创建 Kafka client。

- [ ] **步骤 6：运行 monitor 和进程测试**

运行：

```bash
cargo test -p backend --test alert_monitor -- --nocapture
cargo test -p backend --test server_process -- --nocapture
```

预期：PASS；Tokio 暂停时间测试无真实等待；SIGTERM 测试正常退出。

- [ ] **步骤 7：提交**

```bash
git add backend/src/alerts backend/src/lib.rs backend/src/main.rs backend/tests/alert_monitor.rs backend/tests/server_process.rs
git commit -m "feat: 后台监控 Lag 增长并发送告警"
```

---

### 任务 6：实现飞书告警表单和前端 API

**文件：**
- 创建：`frontend/src/features/feishu-alerts/FeishuAlertForm.tsx`
- 创建：`frontend/src/features/feishu-alerts/FeishuAlertForm.test.tsx`
- 修改：`frontend/src/api/types.ts`
- 修改：`frontend/src/api/client.ts`
- 修改：`frontend/src/index.css`

- [ ] **步骤 1：编写失败的组件测试**

测试已保存掩码、完整 Token 不出现、睁眼/闭眼图标无障碍名称、本次输入显示切换、清除禁用 Token 输入、下拉选项、紧凑输入、最小 10 秒、负阈值、重复组合、新增删除、启用状态和保存值。

```tsx
it('shows only new token and validates pair threshold and duration', async () => {
  render(<Harness />)
  expect(screen.getByText('••••••••a1b2')).toBeInTheDocument()
  expect(screen.queryByDisplayValue('stored-secret')).not.toBeInTheDocument()
  await user.type(screen.getByLabelText('更新 Token'), 'new-token')
  await user.click(screen.getByRole('button', { name: '显示 Token' }))
  expect(screen.getByLabelText('更新 Token')).toHaveAttribute('type', 'text')
  await user.clear(screen.getByLabelText('持续时间（秒）'))
  await user.type(screen.getByLabelText('持续时间（秒）'), '9')
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
})
```

- [ ] **步骤 2：运行测试验证失败**

运行：`npm --prefix frontend run test -- --run src/features/feishu-alerts/FeishuAlertForm.test.tsx`

预期：FAIL，组件不存在。

- [ ] **步骤 3：添加共享类型和 API 客户端**

定义：

```ts
export interface FeishuAlertRule { id?: string; groupId: string; topic: string; lagThreshold: number; growthSeconds: number; enabled: boolean }
export interface FeishuAlertSettings { accessTokenConfigured: boolean; maskedAccessToken: string | null; rules: FeishuAlertRule[] }
export interface SaveFeishuAlertSettings { accessToken?: string; clearAccessToken: boolean; rules: FeishuAlertRule[] }
```

`feishuAlertsApi` 提供 `get(id, signal)`、`save(id, value)`、`test(id, accessToken?)`，路径与后端一致。

- [ ] **步骤 4：实现表单与统一样式**

组件接收 `snapshot` 生成去重排序的 Consumer Group/Topic 下拉选项；没有新鲜快照时禁用新增和组合修改，但允许 Token 测试、Token 清除和关闭。四列控件与 Token 输入统一使用同一个 CSS 基类；阈值宽 112px、时间宽 96px、高 42px，窄屏只让 `.feishu-rules-scroll` 横向滚动。

眼睛按钮必须有 `aria-label="显示 Token"` / `"隐藏 Token"`，使用内联 SVG 或项目内 React SVG，不新增图片文件。

- [ ] **步骤 5：运行组件和类型检查**

运行：

```bash
npm --prefix frontend run test -- --run src/features/feishu-alerts/FeishuAlertForm.test.tsx
npm --prefix frontend run typecheck
```

预期：PASS。

- [ ] **步骤 6：提交**

```bash
git add frontend/src/api/types.ts frontend/src/api/client.ts frontend/src/features/feishu-alerts frontend/src/index.css
git commit -m "feat: 添加飞书告警配置表单"
```

---

### 任务 7：把飞书告警弹窗接入集群设置页

**文件：**
- 修改：`frontend/src/features/clusters/ClustersPage.tsx`
- 修改：`frontend/src/features/clusters/ClustersPage.test.tsx`

- [ ] **步骤 1：编写失败的页面集成测试**

覆盖卡片“未配置”/“Token 已配置 · N 条规则”、独立配置按钮、弹窗使用当前集群快照或按需配置会话、保存后刷新摘要、测试成功提示、关闭释放会话、切换集群不串配置。

```tsx
it('opens the independent Feishu editor and refreshes the card summary', async () => {
  render(<ClustersPage snapshot={snapshotFixture} />)
  expect(await screen.findByText('飞书告警：Token 已配置 · 2 条 Topic + Group 规则')).toBeInTheDocument()
  await user.click(screen.getByRole('button', { name: '配置飞书告警' }))
  expect(await screen.findByRole('dialog', { name: '配置飞书告警' })).toBeInTheDocument()
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(await screen.findByText('飞书告警配置已保存')).toBeInTheDocument()
})
```

- [ ] **步骤 2：运行测试验证失败**

运行：`npm --prefix frontend run test -- --run src/features/clusters/ClustersPage.test.tsx`

预期：FAIL，卡片和按钮不存在。

- [ ] **步骤 3：实现卡片摘要和独立编辑器**

在 `ClustersPage.tsx` 增加小型 `FeishuAlertSummary` 子组件，每个集群通过 TanStack Query 读取设置；增加 `feishuClusterId` 状态和 `FeishuAlertEditor`，复用现有 `ConfigurationSession` 获取 Group/Topic 列表。打开飞书弹窗时关闭 Lag 规则/连接编辑，关闭弹窗时配置会话释放。

保存成功后同时更新 `['feishu-alerts', cluster.id]` cache 并 invalidate；测试通知不保存草稿。删除集群时清理飞书弹窗状态。

- [ ] **步骤 4：运行页面、全部前端测试和构建**

运行：

```bash
npm --prefix frontend run test -- --run src/features/clusters/ClustersPage.test.tsx
npm --prefix frontend run test -- --run
npm --prefix frontend run build
```

预期：全部 PASS；构建无 TypeScript 错误。

- [ ] **步骤 5：提交**

```bash
git add frontend/src/features/clusters/ClustersPage.tsx frontend/src/features/clusters/ClustersPage.test.tsx
git commit -m "feat: 在集群设置接入飞书告警"
```

---

### 任务 8：补齐浏览器验收、中文文档和发行验证

**文件：**
- 修改：`frontend/e2e/kafka-dashboard.spec.ts`
- 修改：`README.md`
- 修改：`packaging/README.txt`

- [ ] **步骤 1：编写失败的浏览器验收**

在现有一次性 Kafka E2E 中：打开生产样式集群卡片 → 配置 Token 与 Group/Topic 规则 → 验证 9 秒被拒绝、10 秒可保存 → 关闭重开只显示掩码 → 验证四列控件统一 class/尺寸 → 清除 Token 后摘要变为未配置。测试通知请求用 Playwright route 仅替换该 POST 响应，避免向真实飞书发送；保存规则仍走真实后端，阈值设为极大值避免后台实际触发。

- [ ] **步骤 2：运行 E2E 验证失败**

运行：`npm --prefix frontend run test:e2e`

预期：FAIL，飞书配置交互断言尚未实现或文案不匹配。

- [ ] **步骤 3：更新中文 README 与发行说明**

两个文档都必须明确：

- 在飞书群添加自定义机器人并从 Webhook 复制 `access_token`，不要粘贴完整 URL；
- Token 按集群配置，规则按 Consumer Group + Topic 配置；
- 阈值与全部 Partition Lag 总和比较，严格大于才开始计时；
- 任一 Partition 未知/未提交/失败/陈旧时不使用部分总和告警；
- 持续时间最少 10 秒，后台每 5 秒采样，每轮只通知一次，失败 30 秒重试且最多 3 次；
- 页面关闭后后台仍保持已启用集群的只读 Kafka 连接；
- Token 明文保存在 SQLite，服务无内置认证，必须限制数据库、sidecar、备份和 HTTP 访问；
- 如何测试通知、停用规则和清除 Token。

- [ ] **步骤 4：运行完整验证**

按顺序运行，禁止并发打包：

```bash
sh -n packaging/start.sh tools/verify-release.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test scripts/e2e-preview.test.mjs
node --test tests/release_scripts.test.mjs
npm --prefix frontend run test:e2e
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
git diff --check
```

预期：全部退出 0。若本机不具备 Docker、Apple Silicon 或 Linux/amd64 构建能力，必须如实记录未执行项，不得报告为通过。

- [ ] **步骤 5：执行秘密扫描和差异自审**

```bash
rg -n --hidden --glob '!.git/**' --glob '!target/**' --glob '!node_modules/**' --glob '!dist/**' '(access_token=|open-apis/bot/v2/hook/[A-Za-z0-9_-]{8,}|fixture-token-secret)' .
git status --short
git diff --check
```

预期：源码只出现占位符、测试常量和固定无 Token 的官方路径；没有真实 Token、数据库、日志、PID、trace 或生成目录进入提交。

- [ ] **步骤 6：提交**

```bash
git add README.md packaging/README.txt frontend/e2e/kafka-dashboard.spec.ts
git commit -m "docs: 补充飞书告警配置与验收"
```

- [ ] **步骤 7：最终核对**

```bash
git status -sb
git log --oneline -8
```

预期：工作区干净；任务 1–8 的单一目的提交完整存在。未经用户明确授权不推送、不创建 tag、不发布 Release。
