# Kafka Dashboard 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 构建一个 React + Rust 的只读 Kafka 仪表盘，只在页面使用期间连接 Kafka，并展示 Topic Offset、Consumer Group Lag、最近一小时趋势及可配置状态判定规则。

**架构：** React 前端通过 REST 创建短租约监控会话，以 3/5/10 秒间隔拉取快照，并在浏览器内维护最近一小时样本和持续超限计时。Axum 后端使用 rust-rdkafka 采集 metadata、watermarks、consumer groups 和 committed offsets，使用 SQLx + SQLite 保存集群及 Lag 规则，并用 AES-256-GCM 加密认证字段。生产环境由同一 Rust 进程提供 API 和 Vite 静态资源，发行目录同时携带 macOS ARM64 与 Linux x86_64 二进制。

**技术栈：** Rust、Axum、Tokio、rust-rdkafka 0.39、SQLx/SQLite、AES-GCM、React、TypeScript、Vite、TanStack Query 5、Apache ECharts、Vitest、Testing Library、Playwright、Docker Compose Kafka 测试容器、Docker Buildx、musl。

---

## 文件结构

### 根目录

- `Cargo.toml`：Rust workspace，只包含 `backend` crate。
- `.gitattributes`：固定 SQL migration 为 LF，保证 SQLx migration hash 稳定。
- `docker-compose.test.yml`：本地与 CI 使用的单节点 KRaft Kafka 测试容器。
- `README.md`：开发、配置、测试和生产部署说明。
- `AGENTS.md`：架构边界、开发命令、TDD、Context7 和安全约束。
- `packaging/Dockerfile.linux-amd64`：在 Linux/amd64 构建静态 musl 后端。
- `packaging/start.sh`：发行包平台识别和单进程启动入口。
- `packaging/.env.example`：不含秘密的运行环境变量示例。
- `packaging/README.txt`：发行目录内的离线运行说明。
- `tools/build-release.sh`：唯一的跨平台发行包构建入口。
- `tools/verify-release.sh`：架构、校验和、健康检查和静态资源验证。
- `tests/release_scripts.test.mjs`：启动脚本和发行目录的行为测试。

### 后端

- `backend/Cargo.toml`：后端依赖和 feature。
- `backend/src/main.rs`：加载配置、连接数据库、启动 Axum、优雅退出。
- `backend/src/lib.rs`：对测试暴露应用构造函数和模块。
- `backend/src/config.rs`：环境变量、监听地址、主密钥和超时配置。
- `backend/src/error.rs`：统一错误码、request ID 和 HTTP 响应。
- `backend/src/crypto.rs`：AES-256-GCM 敏感字段加解密。
- `backend/src/domain/cluster.rs`：集群配置、脱敏 DTO 和安全协议。
- `backend/src/domain/lag_rule.rs`：默认阈值、自定义规则和校验。
- `backend/src/domain/snapshot.rs`：Topic、Partition、Consumer Group 快照 DTO。
- `backend/src/domain/offset.rs`：watermark、最大消息 Offset 和 Lag 的纯函数。
- `backend/src/db/mod.rs`：SQLite pool、embedded migrations 和事务入口。
- `backend/src/db/clusters.rs`：集群配置 repository。
- `backend/src/db/lag_rules.rs`：Lag 规则 repository 和原子替换。
- `backend/src/kafka/config.rs`：业务配置到 librdkafka 属性的映射。
- `backend/src/kafka/client.rs`：连接测试、AdminClient/BaseConsumer 创建。
- `backend/src/kafka/collector.rs`：metadata、watermarks、groups 和 offsets 聚合。
- `backend/src/kafka/group_consumers.rs`：按 group.id 缓存只读 BaseConsumer，会话结束时释放。
- `backend/src/sessions/manager.rs`：创建、续租、删除和清理监控会话。
- `backend/src/api/mod.rs`：路由组合和 AppState。
- `backend/src/api/health.rs`：健康检查。
- `backend/src/api/clusters.rs`：集群 CRUD 和连接测试。
- `backend/src/api/lag_rules.rs`：默认阈值及覆盖规则 API。
- `backend/src/api/sessions.rs`：会话和快照 API。
- `backend/src/api/static_assets.rs`：生产静态资源、SPA fallback 和 API 404 边界。
- `backend/migrations/0001_initial.sql`：clusters 与 lag_status_rules 表。
- `backend/tests/`：API、会话和 Kafka 集成测试。

### 前端

- `frontend/package.json`、`frontend/vite.config.ts`：Vite、Vitest 和开发代理。
- `frontend/src/main.tsx`、`frontend/src/app/App.tsx`：应用入口、QueryClient 和路由。
- `frontend/src/api/client.ts`：统一 fetch、错误解析和 request ID。
- `frontend/src/api/types.ts`：API DTO。
- `frontend/src/components/MetricHelp.tsx`：表头字段说明提示。
- `frontend/src/components/StatusBadge.tsx`：状态与“自定义规则”标记。
- `frontend/src/features/clusters/`：集群列表、表单和连接测试。
- `frontend/src/features/lag-rules/`：默认区间、自定义规则和联动下拉框。
- `frontend/src/features/dashboard/useMonitorSession.ts`：可见性、会话和轮询生命周期。
- `frontend/src/features/dashboard/history.ts`：最近一小时环形缓冲。
- `frontend/src/features/dashboard/DashboardLayout.tsx`：侧边栏和顶部状态栏。
- `frontend/src/features/topics/TopicsPage.tsx`：Topic 汇总及 Partition 展开表格。
- `frontend/src/features/consumer-groups/status.ts`：持续超限状态机。
- `frontend/src/features/consumer-groups/ConsumerGroupsPage.tsx`：分组、筛选、状态及详情。
- `frontend/src/features/overview/OverviewPage.tsx`：指标卡、异常组和总 Lag 趋势。
- `frontend/src/features/charts/LagLineChart.tsx`：ECharts 初始化、更新、resize 和 dispose。
- `frontend/src/test/`：MSW handlers、render helper 和 fixture。
- `frontend/e2e/`：Playwright 端到端测试。

## 任务 1：建立可运行的前后端骨架

**文件：**
- 创建：`Cargo.toml`
- 创建：`.gitattributes`
- 创建：`backend/Cargo.toml`
- 创建：`backend/src/lib.rs`
- 创建：`backend/src/main.rs`
- 创建：`backend/src/api/mod.rs`
- 创建：`backend/src/api/health.rs`
- 创建：`backend/tests/health_api.rs`
- 创建：`frontend/` Vite React TypeScript 工程
- 创建：`frontend/src/app/App.tsx`
- 创建：`frontend/src/app/App.test.tsx`
- 修改：`.gitignore`

- [ ] **步骤 1：创建 workspace、后端 crate 和前端 Vite 工程**

运行：

```bash
cargo new backend --bin
npm create vite@latest frontend -- --template react-ts
npm --prefix frontend install
npm --prefix frontend install -D vitest jsdom @testing-library/react @testing-library/jest-dom @testing-library/user-event
```

在根 `Cargo.toml` 写入：

```toml
[workspace]
members = ["backend"]
resolver = "2"
```

在 `backend/Cargo.toml` 保持 package 名为 `backend`，增加 `[lib] name = "kafka_dashboard"` 供集成测试导入，并增加 `[[bin]] name = "kafka-dashboard"` 生成发行二进制。使用 `cargo add` 添加 `axum`、`tokio --features full,test-util`、`serde --features derive`、`serde_json`、`tower-http --features cors,trace,request-id,limit`、`tracing`、`tracing-subscriber`、`uuid --features v4,serde`、`thiserror`；dev dependency 添加 `tower --features util` 和 `http-body-util`。配置 Vitest 的 `jsdom` 环境、Testing Library setup file 以及 `test` script。将 `target/`、`frontend/node_modules/`、`frontend/dist/`、`*.db` 加入 `.gitignore`；`.gitattributes` 写入 `*.sql text eol=lf`。

- [ ] **步骤 2：先写失败的 health API 测试**

```rust
#[tokio::test]
async fn health_returns_ok_json() {
    let response = kafka_dashboard::app_for_test()
        .oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&body[..], br#"{"status":"ok"}"#);
}
```

- [ ] **步骤 3：运行测试确认失败**

运行：`cargo test -p backend --test health_api`

预期：FAIL，提示 `app_for_test` 或 `kafka_dashboard` 尚不存在。

- [ ] **步骤 4：实现最小 Axum 应用与优雅退出**

```rust
pub fn app_for_test() -> Router {
    Router::new().route("/api/health", get(|| async { Json(json!({"status": "ok"})) }))
}
```

`main.rs` 使用 `TcpListener`、`axum::serve` 和 `with_graceful_shutdown(tokio::signal::ctrl_c())`。前端 `App.tsx` 先渲染 `Kafka Lens` 标题和四个导航名称。

- [ ] **步骤 5：补前端冒烟测试并验证两端**

```tsx
it('renders the dashboard navigation', () => {
  render(<App />)
  expect(screen.getByText('Kafka Lens')).toBeInTheDocument()
  expect(screen.getByText('Consumer Groups')).toBeInTheDocument()
})
```

运行：

```bash
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
```

预期：全部 PASS，Vite 构建成功。

- [ ] **步骤 6：Commit**

```bash
git add Cargo.toml .gitattributes backend frontend
git commit -m "feat: bootstrap kafka dashboard workspace"
```

## 任务 2：锁定 Offset、Lag 与 DTO 语义

**文件：**
- 创建：`backend/src/domain/mod.rs`
- 创建：`backend/src/domain/offset.rs`
- 创建：`backend/src/domain/snapshot.rs`
- 修改：`backend/src/lib.rs`
- 测试：`backend/src/domain/offset.rs`

- [ ] **步骤 1：为 Offset 边界写失败测试**

```rust
#[test]
fn derives_partition_metrics_without_off_by_one() {
    assert_eq!(latest_message_offset(120, 98_241), Some(98_240));
    assert_eq!(retained_offset_span(120, 98_241), 98_121);
    assert_eq!(lag(98_241, Some(90_031)), Some(8_210));
}

#[test]
fn empty_and_uncommitted_are_explicit() {
    assert_eq!(latest_message_offset(5, 5), None);
    assert_eq!(retained_offset_span(5, 5), 0);
    assert_eq!(lag(5, None), None);
}
```

- [ ] **步骤 2：运行测试确认失败**

运行：`cargo test -p backend domain::offset`

预期：FAIL，三个函数未定义。

- [ ] **步骤 3：实现纯函数与序列化 DTO**

```rust
pub fn latest_message_offset(low: i64, high: i64) -> Option<i64> {
    (high > low).then_some(high - 1)
}

pub fn retained_offset_span(low: i64, high: i64) -> u64 {
    high.saturating_sub(low) as u64
}

pub fn lag(high: i64, committed: Option<i64>) -> Option<u64> {
    committed.map(|offset| high.saturating_sub(offset) as u64)
}
```

定义 `PartitionWatermark`、`TopicSnapshot`、`GroupPartitionSnapshot`、`ConsumerGroupTopicSnapshot`、`PartialFailure` 和 `ClusterSnapshot`，所有 JSON 字段使用 `camelCase`。

- [ ] **步骤 4：增加汇总测试并运行**

验证 Topic 汇总使用 `min(low)`、`max(high - 1)` 和 `sum(high - low)`；验证 Group + Topic 只累加已提交 Partition，并单独计数未知 Partition。

运行：`cargo test -p backend domain::`

预期：PASS。

- [ ] **步骤 5：Commit**

```bash
git add backend/src/domain backend/src/lib.rs
git commit -m "feat: define kafka offset and snapshot semantics"
```

## 任务 3：实现 SQLite、敏感字段加密和 repositories

**文件：**
- 创建：`backend/src/config.rs`
- 创建：`backend/src/crypto.rs`
- 创建：`backend/src/db/mod.rs`
- 创建：`backend/src/db/clusters.rs`
- 创建：`backend/src/db/lag_rules.rs`
- 创建：`backend/src/domain/cluster.rs`
- 创建：`backend/src/domain/lag_rule.rs`
- 创建：`backend/migrations/0001_initial.sql`
- 测试：`backend/tests/repositories.rs`

- [ ] **步骤 1：写 encryption round-trip 与 nonce 唯一性失败测试**

先添加依赖：

```bash
cargo add -p backend sqlx --features runtime-tokio,sqlite,migrate,macros,chrono
cargo add -p backend aes-gcm base64 rand
cargo add -p backend chrono --features serde
```

```rust
#[test]
fn ciphertext_round_trips_and_uses_unique_nonce() {
    let cipher = SecretCipher::from_key([7_u8; 32]);
    let first = cipher.encrypt(b"secret").unwrap();
    let second = cipher.encrypt(b"secret").unwrap();
    assert_ne!(first, second);
    assert_eq!(cipher.decrypt(&first).unwrap(), b"secret");
}
```

- [ ] **步骤 2：运行测试确认失败**

运行：`cargo test -p backend ciphertext_round_trips`

预期：FAIL，`SecretCipher` 尚不存在。

- [ ] **步骤 3：实现 AES-256-GCM 密文格式**

使用 32 字节 base64 主密钥。密文格式固定为 `version(1 byte) || nonce(12 bytes) || ciphertext+tag`：

```rust
let nonce_bytes: [u8; 12] = rand::random();
let nonce = Nonce::from_slice(&nonce_bytes);
let encrypted = self.inner.encrypt(nonce, plaintext)?;
Ok([&[1_u8][..], &nonce_bytes, &encrypted].concat())
```

解密时拒绝未知版本、短密文和认证失败。任何错误文本不得包含明文。

- [ ] **步骤 4：写 migration 与 repository 失败测试**

测试内容：集群写入后读取 DTO 不含密码；密文字段不等于明文；默认阈值为 `999/5000/30`；Lag 规则原子替换；重复 group/topic 被唯一索引拒绝。

```sql
CREATE TABLE clusters (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  bootstrap_servers TEXT NOT NULL,
  security_protocol TEXT NOT NULL,
  sasl_mechanism TEXT,
  username TEXT,
  encrypted_secrets BLOB,
  connection_timeout_ms INTEGER NOT NULL DEFAULT 10000,
  default_normal_lag_max INTEGER NOT NULL DEFAULT 999,
  default_high_lag_min INTEGER NOT NULL DEFAULT 5000,
  default_over_limit_seconds INTEGER NOT NULL DEFAULT 30,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
```

- [ ] **步骤 5：实现 embedded migration 和 repositories**

```rust
pub static MIGRATOR: Migrator = sqlx::migrate!();

pub async fn connect(url: &str) -> Result<SqlitePool, sqlx::Error> {
    let pool = SqlitePoolOptions::new().max_connections(8).connect(url).await?;
    MIGRATOR.run(&pool).await?;
    Ok(pool)
}
```

规则更新必须在单个 transaction 中先校验再替换，防止只保存部分规则。

在 `domain/cluster.rs` 为无认证测试场景提供明确构造函数：

```rust
impl DecryptedClusterConfig {
    pub fn plaintext(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            security_protocol: SecurityProtocol::Plaintext,
            sasl_mechanism: None,
            username: None,
            password: None,
            ca_pem: None,
            client_certificate_pem: None,
            client_key_pem: None,
            connection_timeout: Duration::from_secs(10),
        }
    }
}
```

- [ ] **步骤 6：运行 repository 测试**

运行：`cargo test -p backend --test repositories`

预期：PASS。

- [ ] **步骤 7：Commit**

```bash
git add backend/src/config.rs backend/src/crypto.rs backend/src/db backend/src/domain backend/migrations
git commit -m "feat: persist encrypted kafka cluster settings"
```

## 任务 4：提供集群配置与 Lag 规则 REST API

**文件：**
- 创建：`backend/src/error.rs`
- 创建：`backend/src/api/clusters.rs`
- 创建：`backend/src/api/lag_rules.rs`
- 修改：`backend/src/api/mod.rs`
- 创建：`backend/tests/support/mod.rs`
- 测试：`backend/tests/clusters_api.rs`
- 测试：`backend/tests/lag_rules_api.rs`

- [ ] **步骤 1：写集群 CRUD 与脱敏失败测试**

```rust
#[tokio::test]
async fn create_and_read_cluster_never_returns_password() {
    let app = test_app().await;
    let created = post_json(&app, "/api/clusters", json!({
        "name": "prod",
        "bootstrapServers": "kafka:9093",
        "securityProtocol": "SASL_SSL",
        "saslMechanism": "SCRAM-SHA-512",
        "username": "reader",
        "password": "top-secret"
    })).await;
    assert_eq!(created.status(), StatusCode::CREATED);
    assert!(!response_text(created).await.contains("top-secret"));
}
```

- [ ] **步骤 2：写 Lag 规则边界失败测试**

验证 `normalMax >= 0`、`highMin > normalMax`、`overLimitSeconds >= 0`、同集群 group/topic 唯一；无效请求返回 `VALIDATION_ERROR` 和字段详情。

- [ ] **步骤 3：实现统一错误响应**

```rust
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: String,
    pub request_id: String,
    pub fields: BTreeMap<String, String>,
}
```

通过 middleware 生成或透传 `x-request-id`，日志只记录 cluster ID 和错误码。

测试 helper 使用真实 Router，不绕过 middleware：

```rust
pub async fn post_json(app: &Router, uri: &str, value: Value) -> Response {
    app.clone().oneshot(
        Request::post(uri)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(value.to_string()))
            .unwrap(),
    ).await.unwrap()
}

pub async fn response_text(response: Response) -> String {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}
```

`test_app()` 创建临时 SQLite pool、执行 `MIGRATOR`、注入固定测试密钥并返回完整 Router。

- [ ] **步骤 4：实现 CRUD 和原子规则更新 handlers**

路由严格对应规格：`/api/clusters`、`/api/clusters/:id`、`/api/clusters/:id/lag-rules`。更新集群时缺失/空密码表示保留现有密文；显式 `clearPassword: true` 才清除。

- [ ] **步骤 5：运行 API 测试**

运行：

```bash
cargo test -p backend --test clusters_api
cargo test -p backend --test lag_rules_api
```

预期：PASS，响应中无明文密码或私钥。

- [ ] **步骤 6：Commit**

```bash
git add backend/src/api backend/src/error.rs backend/tests
git commit -m "feat: add cluster and lag rule APIs"
```

## 任务 5：实现 Kafka 安全配置和连接测试

**文件：**
- 创建：`backend/src/kafka/mod.rs`
- 创建：`backend/src/kafka/config.rs`
- 创建：`backend/src/kafka/client.rs`
- 修改：`backend/src/api/clusters.rs`
- 测试：`backend/src/kafka/config.rs`
- 测试：`backend/tests/kafka_connection.rs`

- [ ] **步骤 1：写安全协议映射失败测试**

先添加 Kafka 依赖：

```bash
cargo add -p backend rdkafka --no-default-features --features cmake-build,ssl-vendored,tokio,tracing
```

```rust
#[test]
fn maps_scram_ssl_without_leaking_secret() {
    let props = build_client_properties(&fixture_cluster()).unwrap();
    assert_eq!(props.get("security.protocol"), Some("SASL_SSL"));
    assert_eq!(props.get("sasl.mechanism"), Some("SCRAM-SHA-512"));
    assert_eq!(props.get("enable.auto.commit"), Some("false"));
}
```

分别覆盖 PLAINTEXT、SSL、SASL_PLAINTEXT、SASL_SSL、PLAIN、SCRAM-SHA-256 和 SCRAM-SHA-512；不支持的组合必须在创建 librdkafka client 前失败。

`ssl-vendored` 为 TLS 与 SASL/SCRAM 静态编译 OpenSSL；不启用 `sasl`/`gssapi`，因为第一版不支持 Kerberos，SASL/PLAIN 与 SASL/SCRAM 由 librdkafka 配合 OpenSSL 提供。关闭默认 `libz` feature 可避免 Linux 发行包动态依赖系统 zlib；本项目只读取 metadata 与 offset，不消费压缩消息体。

`fixture_cluster()` 在测试模块中返回完整配置，且生产 DTO 不派生 `Debug`：

```rust
fn fixture_cluster() -> DecryptedClusterConfig {
    DecryptedClusterConfig {
        bootstrap_servers: "kafka:9093".into(),
        security_protocol: SecurityProtocol::SaslSsl,
        sasl_mechanism: Some(SaslMechanism::ScramSha512),
        username: Some("reader".into()),
        password: Some("test-password".into()),
        ca_pem: Some(TEST_CA.into()),
        client_certificate_pem: None,
        client_key_pem: None,
        connection_timeout: Duration::from_secs(10),
    }
}
```

`TEST_CA` 是测试模块中的固定非敏感 PEM fixture；断言和 Debug 输出中不得出现 `test-password`。

- [ ] **步骤 2：运行测试确认失败**

运行：`cargo test -p backend kafka::config`

预期：FAIL，配置映射函数不存在。

- [ ] **步骤 3：实现 ClientConfig builder 和连接测试接口**

`test_connection` 创建临时 AdminClient，调用 `list_groups(timeout)` 与 metadata 获取，返回 broker 数、topic 数和耗时，随后 drop client。错误映射为 `KAFKA_AUTH_FAILED`、`KAFKA_TLS_FAILED`、`KAFKA_DNS_FAILED` 或 `KAFKA_TIMEOUT`。

- [ ] **步骤 4：添加 PLAINTEXT 测试容器**

创建 `docker-compose.test.yml` 的单节点 KRaft Kafka。集成测试从 `TEST_KAFKA_BROKERS` 读取地址；未设置时标记 ignored，CI 脚本启动容器后执行：

```bash
TEST_KAFKA_BROKERS=127.0.0.1:19092 cargo test -p backend --test kafka_connection -- --ignored
```

预期：连接成功并返回至少一个 broker。

- [ ] **步骤 5：接入 `/api/clusters/:id/test` 并验证**

连接测试不得创建 MonitorSession，不得在响应和日志中输出安全配置。

- [ ] **步骤 6：Commit**

```bash
git add backend/src/kafka backend/src/api/clusters.rs backend/tests/kafka_connection.rs docker-compose.test.yml
git commit -m "feat: validate kafka connections and security settings"
```

## 任务 6：实现短租约监控会话

**文件：**
- 创建：`backend/src/sessions/mod.rs`
- 创建：`backend/src/sessions/manager.rs`
- 创建：`backend/src/api/sessions.rs`
- 修改：`backend/src/api/mod.rs`
- 测试：`backend/src/sessions/manager.rs`
- 测试：`backend/tests/sessions_api.rs`

- [ ] **步骤 1：用暂停时间的 Tokio 测试写租约失败用例**

```rust
#[tokio::test(start_paused = true)]
async fn expires_session_after_fifteen_seconds_without_renewal() {
    let drops = Arc::new(AtomicUsize::new(0));
    let factory = FakeSessionClientFactory::new(drops.clone());
    let manager = SessionManager::new(Duration::from_secs(15), factory);
    let id = manager.create(DecryptedClusterConfig::plaintext("kafka:9092")).await.unwrap();
    tokio::time::advance(Duration::from_secs(16)).await;
    manager.reap_expired().await;
    assert!(matches!(manager.get(id).await, Err(SessionError::Expired)));
}
```

- [ ] **步骤 2：运行测试确认失败**

运行：`cargo test -p backend sessions::manager`

预期：FAIL，SessionManager 未定义。

- [ ] **步骤 3：实现 SessionManager**

`SessionEntry` 包含 cluster ID、AdminClient、group consumer cache、最后续租 `Instant` 和 `tokio::sync::Mutex<()>` 采集锁。`get_and_renew` 必须原子更新租约；后台清理任务每 5 秒调用 `reap_expired`。

测试模块中的 `FakeSessionClientFactory` 实现与生产 factory 相同的 trait，返回一个在 `Drop` 时递增 `Arc<AtomicUsize>` 的 fake client，用于精确断言资源释放次数。

- [ ] **步骤 4：实现会话 API 和删除集群级联关闭**

```text
POST   /api/clusters/:id/sessions -> 201 { sessionId, leaseSeconds: 15 }
DELETE /api/sessions/:id          -> 204
```

删除集群前调用 `close_for_cluster(cluster_id)`。应用 shutdown 时调用 `close_all()`。

- [ ] **步骤 5：测试并检查资源释放**

Fake client 在 Drop 时递增计数；验证主动删除、租约过期、集群删除和 shutdown 都恰好释放一次。

运行：`cargo test -p backend sessions`

预期：PASS。

- [ ] **步骤 6：Commit**

```bash
git add backend/src/sessions backend/src/api backend/tests/sessions_api.rs
git commit -m "feat: manage leased kafka monitoring sessions"
```

## 任务 7：采集完整 Kafka 快照和部分失败

**文件：**
- 创建：`backend/src/kafka/group_consumers.rs`
- 创建：`backend/src/kafka/collector.rs`
- 修改：`backend/src/api/sessions.rs`
- 测试：`backend/src/kafka/collector.rs`
- 测试：`backend/tests/kafka_snapshot.rs`

- [ ] **步骤 1：定义 KafkaProbe trait 并写聚合失败测试**

先添加异步 trait 和有界并发依赖：

```bash
cargo add -p backend async-trait futures
```

```rust
#[async_trait]
pub trait KafkaProbe: Send + Sync {
    async fn topics(&self) -> Result<Vec<TopicMetadata>, ProbeError>;
    async fn watermarks(&self, topic: &str, partition: i32) -> Result<(i64, i64), ProbeError>;
    async fn groups(&self) -> Result<Vec<GroupMetadata>, ProbeError>;
    async fn committed(&self, group: &str, partitions: &[TopicPartition])
        -> Result<Vec<CommittedOffset>, ProbeError>;
}
```

FakeProbe 返回两个 Topic、一个未提交 Partition 和一个 watermark 错误；断言快照保留健康数据、正确计算 `high - 1`/Lag，并附带 `partialFailures`。

- [ ] **步骤 2：运行测试确认失败**

运行：`cargo test -p backend kafka::collector`

预期：FAIL，collector 尚不存在。

- [ ] **步骤 3：实现有界并发聚合器**

watermark 查询通过 `Semaphore` 限制并发数，单操作套 `tokio::time::timeout`。结果按 Topic、Partition、Group 排序，确保 API 与测试稳定。局部失败写入 `PartialFailure { scope, resource, code, message }`。

- [ ] **步骤 4：实现 rust-rdkafka probe**

AdminClient 负责 metadata 和 `list_groups`。rust-rdkafka 0.39 查询 arbitrary group committed offsets 时，为每个 group.id 创建不订阅 Topic、关闭 auto commit 的 BaseConsumer；在当前 MonitorSession 的 `GroupConsumerCache` 中复用，创建过程有界并发，会话关闭时整体释放。

- [ ] **步骤 5：防止快照重叠并接入 API**

`GET /api/sessions/:id/snapshot` 先 `get_and_renew`，再尝试获取会话采集锁。上一轮仍运行时返回 `409 SNAPSHOT_IN_PROGRESS`，不启动第二轮。

- [ ] **步骤 6：运行单元与 Kafka 集成测试**

```bash
cargo test -p backend kafka::collector
TEST_KAFKA_BROKERS=127.0.0.1:19092 cargo test -p backend --test kafka_snapshot -- --ignored
```

预期：Topic 水位、committed offset、Lag 和部分失败结构正确。

- [ ] **步骤 7：Commit**

```bash
git add backend/src/kafka backend/src/api/sessions.rs backend/tests/kafka_snapshot.rs
git commit -m "feat: collect kafka topic and consumer snapshots"
```

## 任务 8：实现前端 API 层、集群设置和 Lag 规则配置

**文件：**
- 创建：`frontend/src/api/client.ts`
- 创建：`frontend/src/api/types.ts`
- 创建：`frontend/src/features/clusters/ClustersPage.tsx`
- 创建：`frontend/src/features/clusters/ClusterForm.tsx`
- 创建：`frontend/src/features/lag-rules/LagRulesForm.tsx`
- 创建：`frontend/src/test/handlers.ts`
- 测试：对应 `*.test.tsx`

- [ ] **步骤 1：写 API 脱敏和表单行为失败测试**

先添加运行时和测试依赖：

```bash
npm --prefix frontend install @tanstack/react-query
npm --prefix frontend install -D msw
```

测试新增/编辑/删除/连接测试；编辑时密码字段为空表示保持原值；错误响应显示字段错误和 request ID。

- [ ] **步骤 2：写规则下拉联动失败测试**

```tsx
it('limits topics to the selected consumer group', async () => {
  render(<LagRulesForm snapshot={snapshotFixture} value={ruleSet} onChange={vi.fn()} />)
  await user.selectOptions(screen.getByLabelText('Consumer Group'), 'flink-order-enrich')
  expect(screen.getByRole('option', { name: 'raw-orders' })).toBeInTheDocument()
  expect(screen.queryByRole('option', { name: 'audit-log' })).not.toBeInTheDocument()
})
```

- [ ] **步骤 3：运行测试确认失败**

运行：`npm --prefix frontend run test -- --run src/features`

预期：FAIL，组件和 API client 尚不存在。

- [ ] **步骤 4：实现 API client 与 DTO**

`apiFetch<T>` 统一设置 JSON、解析非 2xx 的 `{code,message,requestId,fields}`，支持 `AbortSignal`。类型字段与 Rust `camelCase` DTO 完全一致。

- [ ] **步骤 5：实现集群表单与规则表单**

规则页只包含“集群默认规则”和“自定义 Lag 状态判定区间”。Consumer Group 和 Topic 使用下拉框；Topic 由所选 group 的 snapshot 数据派生；group/topic 组合重复、`highMin <= normalMax` 或负数时禁用保存。

已保存但当前不存在的值显示为 `名称（当前不可用）`，只能保留或删除，不能自由输入。

- [ ] **步骤 6：运行前端测试**

运行：`npm --prefix frontend run test -- --run`

预期：PASS。

- [ ] **步骤 7：Commit**

```bash
git add frontend/src/api frontend/src/features/clusters frontend/src/features/lag-rules frontend/src/test
git commit -m "feat: manage kafka clusters and lag rules in ui"
```

## 任务 9：实现页面可见性、会话轮询和一小时历史

**文件：**
- 创建：`frontend/src/features/dashboard/useMonitorSession.ts`
- 创建：`frontend/src/features/dashboard/history.ts`
- 创建：`frontend/src/features/dashboard/DashboardLayout.tsx`
- 测试：`frontend/src/features/dashboard/*.test.ts*`

- [ ] **步骤 1：写环形缓冲失败测试**

```ts
it('keeps only samples from the latest hour', () => {
  const now = Date.parse('2026-10-15T10:00:00Z')
  const result = appendSample([{ at: now - 3_600_001, totalLag: 1 }], { at: now, totalLag: 2 })
  expect(result).toEqual([{ at: now, totalLag: 2 }])
})
```

- [ ] **步骤 2：写会话生命周期失败测试**

使用 fake timers 和 `visibilitychange` 验证：进入页面创建会话；3/5/10 秒轮询；隐藏时停止并清空持续超限计时；恢复时创建或续用有效会话并立即采集；切换集群和卸载时 DELETE 会话。

- [ ] **步骤 3：运行测试确认失败**

运行：`npm --prefix frontend run test -- --run src/features/dashboard`

预期：FAIL，hook 和 history 函数不存在。

- [ ] **步骤 4：实现 history 与 TanStack Query hook**

```ts
useQuery({
  queryKey: ['snapshot', sessionId],
  queryFn: ({ signal }) => api.snapshot(sessionId!, signal),
  enabled: Boolean(sessionId) && document.visibilityState === 'visible',
  refetchInterval: refreshSeconds * 1000,
  refetchIntervalInBackground: false,
})
```

会话过期时只自动重建并重试一次；持续失败后停止轮询，提供“重新连接”。清理使用普通 DELETE，并在 `pagehide` 中用 `fetch(..., {method:'DELETE', keepalive:true})` 兜底。

- [ ] **步骤 5：实现 DashboardLayout 顶栏与导航**

展示集群选择器、连接状态、最后更新时间、3/5/10 秒选择和手动刷新。切换集群必须先清空当前历史和状态机，再创建新会话。

- [ ] **步骤 6：运行测试与类型检查**

```bash
npm --prefix frontend run test -- --run src/features/dashboard
npm --prefix frontend run build
```

预期：PASS。

- [ ] **步骤 7：Commit**

```bash
git add frontend/src/features/dashboard frontend/src/app
git commit -m "feat: poll kafka snapshots only while dashboard is visible"
```

## 任务 10：实现 Topics 页面与字段解释

**文件：**
- 创建：`frontend/src/components/MetricHelp.tsx`
- 创建：`frontend/src/features/topics/TopicsPage.tsx`
- 测试：`frontend/src/features/topics/TopicsPage.test.tsx`

- [ ] **步骤 1：写 Topics 行与 tooltip 失败测试**

验证搜索、汇总、展开、空 Partition，以及键盘 focus `?` 时显示定义。

```tsx
expect(screen.getByText('105,101')).toBeInTheDocument()
await user.click(screen.getByRole('button', { name: '展开 order-events' }))
expect(screen.getByText('High watermark')).toBeInTheDocument()
await user.tab()
expect(screen.getByRole('tooltip')).toHaveTextContent('下一条消息将写入的位置')
```

- [ ] **步骤 2：运行测试确认失败**

运行：`npm --prefix frontend run test -- --run TopicsPage`

预期：FAIL，页面未实现。

- [ ] **步骤 3：实现表格和无障碍提示**

汇总表展示 Topic、Partitions、最小 Offset、最大消息 Offset、保留消息估算。提示同时支持 hover、focus 和触屏点击；说明 `high - 1`、空分区 `—`、`high - low` 只是 Offset 范围估算。

- [ ] **步骤 4：运行测试**

运行：`npm --prefix frontend run test -- --run TopicsPage`

预期：PASS。

- [ ] **步骤 5：Commit**

```bash
git add frontend/src/components/MetricHelp.tsx frontend/src/features/topics
git commit -m "feat: display topic and partition offsets"
```

## 任务 11：实现 Consumer Groups 状态机和页面

**文件：**
- 创建：`frontend/src/features/consumer-groups/status.ts`
- 创建：`frontend/src/components/StatusBadge.tsx`
- 创建：`frontend/src/features/consumer-groups/ConsumerGroupsPage.tsx`
- 测试：`frontend/src/features/consumer-groups/*.test.ts*`

- [ ] **步骤 1：写状态区间与持续超限失败测试**

```ts
it('keeps flink checkpoint lag normal until custom duration elapses', () => {
  const rule = { normalMax: 12_000, highMin: 30_000, overLimitSeconds: 120 }
  let state = initialLagState()
  ;({ state } = evaluateLag({ lag: 13_000, elapsedVisibleMs: 60_000, rule }, state))
  expect(state.status).toBe('normal')
  ;({ state } = evaluateLag({ lag: 13_500, elapsedVisibleMs: 60_000, rule }, state))
  expect(state.status).toBe('backlog')
  ;({ state } = evaluateLag({ lag: 200, elapsedVisibleMs: 5_000, rule }, state))
  expect(state.status).toBe('normal')
  expect(state.overLimitMs).toBe(0)
})
```

另测优先级：采集异常 > 未提交 > 高 Lag > 积压 > 正常；自定义规则精确匹配优先于集群默认规则。

- [ ] **步骤 2：运行测试确认失败**

运行：`npm --prefix frontend run test -- --run consumer-groups/status`

预期：FAIL，状态机未定义。

- [ ] **步骤 3：实现纯状态机**

`elapsedVisibleMs` 只由成功可见采样之间的间隔累加；隐藏、会话重建和集群切换调用 `resetAllLagStates()`。Lag 在积压和高 Lag 区间之间变化不重置计时，回到正常区间立即重置。

- [ ] **步骤 4：写页面与标记失败测试**

验证搜索、状态筛选、Lag 降序、Partition 展开、`已知 Lag + N 未知`、字段 tooltip，以及命中 override 时显示“自定义规则”并包含三个实际值。

- [ ] **步骤 5：实现 ConsumerGroupsPage**

每行键使用 `clusterId/group/topic`。默认规则不显示额外标记；override 行显示可 hover/focus 的“自定义规则”。趋势箭头只比较最近两个成功样本，不参与状态判定。

- [ ] **步骤 6：运行测试**

运行：`npm --prefix frontend run test -- --run consumer-groups`

预期：PASS。

- [ ] **步骤 7：Commit**

```bash
git add frontend/src/components/StatusBadge.tsx frontend/src/features/consumer-groups
git commit -m "feat: classify and display consumer group lag"
```

## 任务 12：实现概览、Lag 图表和错误状态

**文件：**
- 创建：`frontend/src/features/charts/LagLineChart.tsx`
- 创建：`frontend/src/features/overview/OverviewPage.tsx`
- 修改：`frontend/src/features/dashboard/DashboardLayout.tsx`
- 测试：对应 `*.test.tsx`

- [ ] **步骤 1：写图表生命周期失败测试**

先添加图表依赖：

```bash
npm --prefix frontend install echarts
```

mock ECharts，验证 mount 调用 `init`，数据变化调用 `setOption`，ResizeObserver 调用 `resize`，unmount 调用 `dispose`。

- [ ] **步骤 2：写概览聚合失败测试**

fixture 包含正常、积压、高 Lag、未提交和部分失败；断言指标卡数量、总已知 Lag、异常组数和按 Lag 降序列表正确。

- [ ] **步骤 3：运行测试确认失败**

运行：`npm --prefix frontend run test -- --run OverviewPage LagLineChart`

预期：FAIL，组件不存在。

- [ ] **步骤 4：实现 ECharts 组件**

按需注册 `LineChart`、`GridComponent`、`TooltipComponent`、`LegendComponent` 和 `CanvasRenderer`。用 `ResizeObserver` 而不是只监听 window resize；cleanup 必须 disconnect observer 并 `chart.dispose()`。

- [ ] **步骤 5：实现概览和错误体验**

首次加载显示 skeleton；普通刷新保留旧数据；局部失败标记行级陈旧；数据超过两倍刷新周期显示“数据已陈旧”；整个集群不可达时保留最后快照并显示断开和“重新连接”。

- [ ] **步骤 6：运行测试与构建**

```bash
npm --prefix frontend run test -- --run
npm --prefix frontend run build
```

预期：PASS，无 ECharts 未释放警告。

- [ ] **步骤 7：Commit**

```bash
git add frontend/src/features/charts frontend/src/features/overview frontend/src/features/dashboard
git commit -m "feat: add kafka overview and lag charts"
```

## 任务 13：完成端到端验收、硬化和运行文档

**文件：**
- 创建：`backend/tests/support/kafka_fixture.rs`
- 创建：`frontend/playwright.config.ts`
- 创建：`frontend/e2e/kafka-dashboard.spec.ts`
- 创建：`scripts/test-e2e.sh`
- 修改：`README.md`
- 修改：`.gitignore`

- [ ] **步骤 1：编写端到端失败用例**

先添加并安装 Playwright：

```bash
npm --prefix frontend install -D @playwright/test
npm --prefix frontend exec playwright install chromium
```

覆盖：新增集群、测试连接、打开仪表盘、查看 Topic Partition、查看 Consumer Group Lag、切换 3/5/10 秒、编辑默认规则、从下拉框添加自定义规则、看到“自定义规则”标记、切换集群和离开页面后会话关闭。

- [ ] **步骤 2：运行端到端测试确认失败**

运行：`npm --prefix frontend run test:e2e`

预期：FAIL，找不到完整服务启动脚本或 Kafka fixture 数据。

- [ ] **步骤 3：完成 Kafka fixture 与一键 E2E 脚本**

脚本顺序：启动 `docker-compose.test.yml`；等待 Kafka ready；Rust fixture 创建 Topic、生产固定消息、使用测试 group 提交 offset；启动临时 SQLite 后端和 Vite preview；运行 Playwright；最后无论成功失败都关闭子进程和容器。

- [ ] **步骤 4：增加安全和并发回归检查**

测试日志与 API 响应不含 fixture 密码；请求体大小限制生效；同会话并发 snapshot 只有一个采集；删除集群释放会话；SIGTERM 后端能优雅退出。

- [ ] **步骤 5：编写 README**

文档必须包含：系统要求、`KAFKA_DASHBOARD_MASTER_KEY` 生成方式、SQLite URL、前后端开发命令、SASL/TLS 字段、Docker Kafka 测试、Offset/Lag 语义、短租约连接行为、生产反向代理与 HTTPS 建议。

- [ ] **步骤 6：运行完整验证**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
npm --prefix frontend run test:e2e
git diff --check
```

预期：全部命令退出码为 0；浏览器连续采样测试中一小时以前的样本被淘汰；页面隐藏后没有 Kafka 轮询请求。

- [ ] **步骤 7：Commit**

```bash
git add backend/tests frontend/e2e frontend/playwright.config.ts scripts README.md .gitignore docker-compose.test.yml
git commit -m "test: verify kafka dashboard end to end"
```

## 任务 14：实现前后端一体化生产运行

**文件：**
- 创建：`backend/src/api/static_assets.rs`
- 创建：`backend/tests/static_assets_api.rs`
- 修改：`backend/src/api/mod.rs`
- 修改：`backend/src/config.rs`
- 修改：`backend/src/main.rs`
- 修改：`backend/Cargo.toml`

- [ ] **步骤 1：编写静态资源与 SPA fallback 失败测试**

添加 `tempfile` dev dependency。测试目录写入 `index.html` 与 `assets/app.js`，再通过测试应用断言真实文件、前端路由回退和 API 404 严格分离：

```rust
#[tokio::test]
async fn serves_assets_and_falls_back_to_index_without_hiding_api_404() {
    let web = tempfile::tempdir().unwrap();
    std::fs::create_dir(web.path().join("assets")).unwrap();
    std::fs::write(web.path().join("index.html"), "<main>dashboard</main>").unwrap();
    std::fs::write(web.path().join("assets/app.js"), "console.log('ok')").unwrap();
    let app = app_for_test_with_web_root(web.path()).await;

    assert_body(app.clone(), "/assets/app.js", "console.log('ok')").await;
    assert_body(app.clone(), "/clusters/demo/topics", "<main>dashboard</main>").await;
    assert_eq!(get(app, "/api/not-found").await.status(), StatusCode::NOT_FOUND);
}
```

- [ ] **步骤 2：运行测试确认失败**

运行：`cargo test -p backend --test static_assets_api`

预期：FAIL，`app_for_test_with_web_root` 与静态资源路由尚不存在。

- [ ] **步骤 3：实现生产静态资源服务**

使用 `tower-http` 的 `ServeDir` 与 `ServeFile`。先挂载全部 `/api` 路由，再把静态服务作为 fallback；fallback 对缺失静态路径返回 `index.html`，但 `/api/*` 永远返回 JSON 404。`KAFKA_DASHBOARD_WEB_DIR` 显式设置时必须存在且含 `index.html`，否则启动失败；开发测试可不配置静态目录。

`main.rs` 解析 `--version` 后直接输出 Cargo package version 并退出，使发行包可以无数据库、无密钥执行冒烟检查。

- [ ] **步骤 4：运行后端回归测试**

```bash
cargo test -p backend --test static_assets_api
cargo test --workspace
cargo run -p backend --bin kafka-dashboard -- --version
```

预期：测试通过，版本命令输出非空版本号并以 0 退出。

- [ ] **步骤 5：Commit**

```bash
git add backend/Cargo.toml backend/src/api backend/src/config.rs backend/src/main.rs backend/tests/static_assets_api.rs
git commit -m "feat: serve dashboard assets from backend"
```

## 任务 15：生成并验证双平台单目录发行包

**文件：**
- 创建：`packaging/Dockerfile.linux-amd64`
- 创建：`packaging/start.sh`
- 创建：`packaging/.env.example`
- 创建：`packaging/README.txt`
- 创建：`tools/build-release.sh`
- 创建：`tools/verify-release.sh`
- 创建：`tests/release_scripts.test.mjs`
- 修改：`.gitignore`
- 修改：`README.md`

- [ ] **步骤 1：编写启动脚本与目录契约失败测试**

用 Node 内置测试运行临时发行目录，不引入额外测试依赖。每个用例在临时 `PATH` 前放置伪 `uname`，并放置记录 argv/env 的伪二进制：

```js
test('Darwin arm64 selects the mac binary and forwards arguments', async () => {
  const fixture = await makeReleaseFixture({ os: 'Darwin', arch: 'arm64' });
  const result = await fixture.run(['--bind', '127.0.0.1:9090']);
  assert.equal(result.code, 0);
  assert.match(result.stdout, /kafka-dashboard-darwin-arm64/);
  assert.match(result.stdout, /127\.0\.0\.1:9090/);
  assert.match(result.stdout, /KAFKA_DASHBOARD_WEB_DIR=.*\/web/);
});
```

另测 Linux/x86_64 选择 Linux 二进制、缺少 `KAFKA_DASHBOARD_MASTER_KEY` 时拒绝启动、不支持的平台显示支持列表，以及 `data/` 自动创建。

- [ ] **步骤 2：运行测试确认失败**

运行：`node --test tests/release_scripts.test.mjs`

预期：FAIL，`packaging/start.sh` 尚不存在。

- [ ] **步骤 3：实现可移动的启动脚本和离线配置文件**

`start.sh` 只使用 POSIX shell；通过脚本自身绝对目录定位 `bin/`、`web/` 和 `data/`，验证主密钥，设置默认 `DATABASE_URL=sqlite://<release>/data/kafka-dashboard.db?mode=rwc` 与 `KAFKA_DASHBOARD_WEB_DIR=<release>/web`，最后用 `exec` 启动对应二进制并原样传递 `"$@"`。`.env.example` 只包含占位值和注释，不由脚本自动加载。

- [ ] **步骤 4：实现 Linux/amd64 静态构建 Dockerfile**

固定 Rust 基础镜像大版本并明确 `--platform=linux/amd64`。安装 musl C 工具链、CMake、make、Perl 和 vendored OpenSSL 编译所需工具；使用 Cargo 锁文件执行：

```dockerfile
RUN cargo build --locked --release --target x86_64-unknown-linux-musl -p backend --bin kafka-dashboard
FROM scratch AS artifact
COPY --from=builder /src/target/x86_64-unknown-linux-musl/release/kafka-dashboard /kafka-dashboard-linux-amd64
```

最终 `artifact` stage 只导出一个二进制，不构建运行镜像。

- [ ] **步骤 5：实现唯一打包入口**

`tools/build-release.sh` 使用 `set -eu`，只允许在 Darwin/arm64 构建机运行，并按顺序执行：

```bash
npm --prefix frontend ci
npm --prefix frontend run build
cargo build --locked --release --target aarch64-apple-darwin -p backend --bin kafka-dashboard
docker buildx build --platform linux/amd64 \
  --file packaging/Dockerfile.linux-amd64 \
  --output type=local,dest="$linux_output" .
```

脚本在 `mktemp -d` 中完成组装，复制两个二进制、`frontend/dist`、启动脚本、示例配置和离线说明，设置可执行位，使用 `shasum -a 256` 生成稳定排序的 `SHA256SUMS`，再原子替换 `dist/kafka-dashboard` 并创建 `dist/kafka-dashboard.tar.gz`。trap 只删除该次创建的明确临时目录。

- [ ] **步骤 6：实现发行包验证脚本**

`tools/verify-release.sh` 验证固定文件清单和 SHA-256；用 `file` 断言 Mach-O arm64 与 ELF x86-64；拒绝 `.env`、SQLite、源码和 `node_modules`；运行两个二进制的 `--version`（Linux 通过 `docker run --platform linux/amd64` 挂载执行）；分别启动 macOS 二进制和 Linux/amd64 容器，轮询 `/api/health`，验证 `/`、`/assets/*` 与 SPA fallback，然后发送 TERM 并确认进程退出。

- [ ] **步骤 7：补充发行文档并运行验证**

在 `README.md` 增加构建主机要求、单命令打包、目录结构、复制运行、支持平台和静态链接边界。执行：

```bash
node --test tests/release_scripts.test.mjs
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
git diff --check
```

预期：所有命令退出码为 0，发行包在两个目标环境完成健康检查与静态页面验证。

- [ ] **步骤 8：Commit**

```bash
git add packaging tools tests/release_scripts.test.mjs README.md .gitignore
git commit -m "build: add dual-platform release package"
```

## 任务 16：完成代理文档、全量验证与 GitHub 交付

**文件：**
- 创建：`AGENTS.md`
- 修改：`README.md`
- 修改：`.gitignore`

- [ ] **步骤 1：编写根目录 AGENTS.md**

写明目录职责、API 与静态资源边界、Kafka 会话生命周期、数据库与密钥规则、前后端常用命令、测试分层、TDD 与提交纪律、禁止提交 `dist/`/数据库/秘密，以及以下 Context7 规则：遇到库、框架、SDK、API、CLI 或云服务问题时，先 `resolve-library-id`，再按单一概念调用 `query-docs`，优先使用官方高信誉文档。

- [ ] **步骤 2：完成 README 交付检查**

逐项确认 README 包含：功能与限制、截图位置、快速启动、环境变量、四类 Kafka 连接、Offset/Lag 字段解释、默认与自定义状态区间、持续超限、仅页面打开时连接、开发命令、测试、打包、目标平台、安全部署和故障排查。所有命令必须能从仓库根目录复制执行，不出现真实凭据。

- [ ] **步骤 3：运行全量验证与秘密扫描**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
npm --prefix frontend run test:e2e
node --test tests/release_scripts.test.mjs
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
git diff --check
git grep -nE '(BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY|password[[:space:]]*=[[:space:]]*[^<])' -- ':!docs/superpowers/plans/*'
```

预期：全部验证成功；秘密扫描无结果；`git status --short` 只显示本任务预期的 README、AGENTS 和 ignore 变更，`dist/` 不被跟踪。

- [ ] **步骤 4：提交最终文档**

```bash
git add README.md AGENTS.md .gitignore
git commit -m "docs: add project and agent guides"
```

- [ ] **步骤 5：配置并核对 GitHub 远端**

运行 `git remote -v`。没有 `origin` 时执行：

```bash
git remote add origin https://github.com/zhangblue/kafka_dashbod.git
git ls-remote --heads origin
```

预期：远端没有分支；若远端已有未知提交，停止并报告，不强推、不覆盖。若 `origin` 已存在，必须精确等于上述 URL，否则停止并报告。

- [ ] **步骤 6：推送 main 并核验**

```bash
git push -u origin main
git ls-remote --heads origin main
```

预期：远端 `refs/heads/main` 的 SHA 与本地 `git rev-parse HEAD` 完全一致，工作区干净。推送后报告仓库 URL、最终 commit、测试结果和发行包本地路径。

## 最终规格覆盖检查

- 多集群与 SASL/TLS：任务 3–5、8。
- 页面打开才连接、短租约与可见性：任务 6、9、13。
- Topic low/high/max/retained 语义与提示：任务 2、7、10。
- 全部消费组、committed offset、Lag 与部分失败：任务 7、11。
- 3/5/10 秒刷新和最近一小时内存历史：任务 9、12。
- 默认区间、自定义 group/topic 区间、持续超限与标记：任务 3、4、8、11。
- Flink checkpoint 型锯齿 Lag：任务 11 的可见采样持续时间状态机。
- 安全、错误和核心测试：任务 3–5、12、13、16。
- 前后端单进程生产运行与 SPA fallback：任务 14。
- macOS ARM64 + Linux x86_64 单目录发行包：任务 15。
- README、AGENTS、秘密扫描与 GitHub main 交付：任务 13、15、16。
