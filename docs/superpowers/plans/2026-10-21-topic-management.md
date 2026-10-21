# Kafka Lens Topic 管理实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 在现有 Topics 页面中安全地创建、删除 Topic，并实时查看和修改 8 个白名单 Topic 配置。

**架构：** 管理 API 绑定现有 15 秒短租约会话并复用其中的 `AdminClient`，与快照采集共享会话互斥锁。领域层负责白名单、中文元数据和输入校验；Kafka 适配层负责 rust-rdkafka Admin API 及 librdkafka 增量配置 FFI；前端只显示 Kafka 实时返回的值和来源，不保存配置副本。

**技术栈：** Rust 2024、Axum 0.8、rust-rdkafka 0.39、rdkafka-sys/librdkafka、Tokio、React 19、TypeScript 6、TanStack Query 5、Vitest、Testing Library、Playwright。

---

## 文件结构

- 创建 `backend/src/domain/topic_admin.rs`：请求/响应类型、8 个配置定义、名称与数值校验、稳定错误模型。
- 修改 `backend/src/domain/mod.rs`：导出 Topic 管理领域模块。
- 创建 `backend/src/kafka/topic_admin.rs`：原生 AdminClient 创建/删除/查询配置适配器和增量配置安全封装。
- 修改 `backend/src/kafka/mod.rs`：导出 Kafka Topic 管理模块。
- 修改 `backend/Cargo.toml`：直接声明与 rust-rdkafka 锁定版本一致的 `rdkafka-sys`，供缺失的 IncrementalAlterConfigs 安全封装使用。
- 修改 `backend/src/sessions/manager.rs`：为 `SessionClientFactory` 增加 Topic 管理方法，并由会话拥有管理任务、互斥和取消后的资源释放。
- 创建 `backend/src/api/topics.rs`：4 个会话级 Topic 管理处理器与错误映射。
- 修改 `backend/src/api/mod.rs`：注册 Topic 管理路由。
- 创建 `backend/tests/topic_admin_api.rs`：使用 fake factory 覆盖 API 契约、安全校验和错误映射。
- 修改 `backend/tests/kafka_fixture.rs`、`backend/tests/support/kafka_fixture.rs`：一次性 Kafka 管理 fixture 与真实 Admin API 验收。
- 修改 `frontend/src/api/types.ts`、`frontend/src/api/client.ts`：Topic 管理 DTO 与唯一 HTTP 客户端方法。
- 修改 `frontend/src/features/dashboard/useMonitorSession.ts`：安全暴露当前有效 `sessionId`，管理后复用现有 `refresh()`。
- 创建 `frontend/src/features/topics/topicConfig.ts`：8 个参数的中文名称、说明、单位、选项和前端校验元数据。
- 创建 `frontend/src/features/topics/CreateTopicModal.tsx`：创建 Topic 表单。
- 创建 `frontend/src/features/topics/TopicConfigModal.tsx`：实时配置读取、来源展示、修改和恢复默认。
- 创建 `frontend/src/features/topics/DeleteTopicModal.tsx`：不可恢复提示与名称双重确认。
- 修改 `frontend/src/features/topics/TopicsPage.tsx`、`frontend/src/index.css`：工具栏、操作列、弹窗状态和响应式样式。
- 创建/修改 `frontend/src/features/topics/*.test.tsx`、`frontend/src/test/handlers.ts`：组件与请求交互测试。
- 修改 `frontend/e2e/kafka-dashboard.spec.ts`：一次性 Topic 的创建、配置、恢复默认和删除流程。
- 修改 `README.md`、`packaging/README.txt`、`AGENTS.md`：解除 Topic 管理范围内的只读描述，补充 ACL、风险和参数说明。

### 任务 1：领域契约与白名单校验

**文件：**
- 创建：`backend/src/domain/topic_admin.rs`
- 修改：`backend/src/domain/mod.rs`

- [ ] **步骤 1：编写失败的领域测试**

在新模块的 `#[cfg(test)]` 中定义表驱动测试，覆盖合法名称、空名称、NUL、`.`、`..`、超过 249 字符、非法字符、内部 Topic 删除、分区数、副本因子以及每个白名单配置。例如：

```rust
#[test]
fn validates_topic_creation_and_rejects_unknown_configs() {
    let input = CreateTopicInput {
        name: "orders.v1".into(),
        partitions: 3,
        replication_factor: None,
        configs: BTreeMap::from([("retention.ms".into(), "3600000".into())]),
    };
    assert!(input.validate().is_ok());
    let invalid = CreateTopicInput {
        configs: BTreeMap::from([("unclean.leader.election.enable".into(), "true".into())]),
        ..input
    };
    assert_eq!(invalid.validate().unwrap_err()["configs"], "Unsupported topic configuration");
}

#[test]
fn delete_rejects_internal_topic_and_mismatched_confirmation() {
    assert_eq!(validate_delete("__consumer_offsets", "__consumer_offsets").unwrap_err().code(), "INTERNAL_TOPIC_PROTECTED");
    assert!(validate_delete("orders", "payments").is_err());
}
```

- [ ] **步骤 2：运行测试并确认因模块缺失而失败**

运行：`cargo test -p backend domain::topic_admin -- --nocapture`

预期：FAIL，提示 `topic_admin` 模块或相关类型不存在。

- [ ] **步骤 3：实现领域类型和校验**

定义并序列化以下稳定类型：

```rust
pub const TOPIC_CONFIG_KEYS: [&str; 8] = [
    "retention.ms", "retention.bytes", "cleanup.policy", "segment.ms",
    "segment.bytes", "min.insync.replicas", "max.message.bytes", "compression.type",
];

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateTopicInput {
    pub name: String,
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    pub replication_factor: Option<i32>,
    #[serde(default)]
    pub configs: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigValue {
    pub key: String,
    pub value: Option<String>,
    pub source: TopicConfigSource,
    pub read_only: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateTopicConfigsInput {
    pub changes: BTreeMap<String, Option<String>>,
    pub expected_values: BTreeMap<String, Option<String>>,
}

pub enum TopicConfigUpdateOutcome {
    Confirmed(Vec<TopicConfigValue>),
    AppliedButUnconfirmed,
}
```

数值校验使用十进制字符串；允许 `retention.ms`、`retention.bytes` 为 `-1`，其他数值必须为正数；`cleanup.policy` 仅允许 `delete`、`compact`、`delete,compact`；`compression.type` 仅允许 `producer`、`gzip`、`snappy`、`lz4`、`zstd`、`uncompressed`。校验错误只返回字段和固定文案，不拼接原始值。

- [ ] **步骤 4：运行领域测试**

运行：`cargo test -p backend domain::topic_admin -- --nocapture`

预期：PASS，覆盖 8 个配置键及删除保护。

- [ ] **步骤 5：提交**

```bash
git add backend/src/domain/mod.rs backend/src/domain/topic_admin.rs
git commit -m "feat: 定义 Topic 管理领域契约"
```

### 任务 2：Kafka Topic 管理适配器

**文件：**
- 创建：`backend/src/kafka/topic_admin.rs`
- 修改：`backend/src/kafka/mod.rs`
- 修改：`backend/Cargo.toml`
- 修改：`Cargo.lock`

- [ ] **步骤 1：编写失败的适配器单元测试**

先测试不依赖 broker 的纯转换边界：只保留白名单、正确识别 `ConfigSource::DynamicTopic`、保留 `is_read_only`、逐资源错误映射且不携带原生消息。

```rust
#[test]
fn maps_described_entries_without_exposing_unknown_or_sensitive_values() {
    let entries = vec![
        entry("retention.ms", Some("60000"), ConfigSource::DynamicTopic, false, false),
        entry("ssl.keystore.password", None, ConfigSource::Unknown, false, true),
    ];
    assert_eq!(map_entries(entries), vec![TopicConfigValue {
        key: "retention.ms".into(), value: Some("60000".into()),
        source: TopicConfigSource::TopicOverride, read_only: false,
    }]);
}

#[test]
fn maps_native_authorization_error_to_stable_safe_error() {
    let error = map_code(RDKafkaErrorCode::TopicAuthorizationFailed);
    assert_eq!(error.code(), "TOPIC_AUTHORIZATION_FAILED");
    assert!(!error.to_string().contains("broker"));
}
```

- [ ] **步骤 2：运行测试并确认失败**

运行：`cargo test -p backend kafka::topic_admin -- --nocapture`

预期：FAIL，提示适配器函数和错误类型不存在。

- [ ] **步骤 3：实现创建、删除和查询配置**

在 `TopicAdminClient` 中封装 `Arc<AdminClient<SessionContext>>` 和操作超时：

```rust
pub struct TopicAdminClient {
    admin: Arc<AdminClient<SessionContext>>,
    timeout: Duration,
}

pub async fn create(&self, input: &CreateTopicInput) -> Result<(), TopicAdminError>;
pub async fn delete(&self, topic: &str) -> Result<(), TopicAdminError>;
pub async fn describe(&self, topic: &str) -> Result<Vec<TopicConfigValue>, TopicAdminError>;
pub async fn update(&self, topic: &str, input: &UpdateTopicConfigsInput) -> Result<TopicConfigUpdateOutcome, TopicAdminError>;
```

创建使用 `NewTopic` 和 `TopicReplication::Fixed(input.replication_factor.unwrap_or(-1))`；创建和删除都检查返回向量中唯一资源的错误。查询使用 `describe_configs([ResourceSpecifier::Topic(topic)])`，只映射 8 个白名单项。

- [ ] **步骤 4：实现 IncrementalAlterConfigs 的最小安全 FFI**

rust-rdkafka 0.39 尚未暴露 IncrementalAlterConfigs；禁止使用已废弃且具有整体替换语义的 `alter_configs`。在同一模块的私有子模块中使用版本一致的 `rdkafka-sys`：

```rust
enum IncrementalChange<'a> { Set(&'a str, &'a str), Delete(&'a str) }

fn incremental_alter_topic(
    admin: Arc<AdminClient<SessionContext>>,
    topic: String,
    changes: Vec<(String, Option<String>)>,
    timeout: Duration,
) -> Result<(), TopicAdminError>;
```

该函数在 `spawn_blocking` 内创建独立 native queue、`rd_kafka_ConfigResource_t` 和 AdminOptions；`Some(value)` 使用 `RD_KAFKA_ALTER_CONFIG_OP_TYPE_SET`，`None` 使用 `RD_KAFKA_ALTER_CONFIG_OP_TYPE_DELETE`；调用 `rd_kafka_IncrementalAlterConfigs` 后有界轮询结果。为 queue、resource、options 和 event 定义局部 RAII 包装，所有提前返回路径都释放原生资源。只向上返回稳定错误种类，不保留 `rd_kafka_error_string`。

- [ ] **步骤 5：实现保存前比较及保存后重读**

`update` 先重新 `describe`，仅比较 `expected_values` 中出现的键；不一致返回 `TOPIC_CONFIG_CHANGED`。增量修改成功后再次 `describe` 并返回 `TopicConfigUpdateOutcome::Confirmed`；若修改成功但重读失败，返回 `TopicConfigUpdateOutcome::AppliedButUnconfirmed`，使 API 能给出“已提交但暂时无法确认”的明确响应。

- [ ] **步骤 6：运行适配器测试、格式和静态检查**

运行：

```bash
cargo test -p backend kafka::topic_admin -- --nocapture
cargo fmt --all -- --check
cargo clippy -p backend --all-targets -- -D warnings
```

预期：全部 PASS，且没有未文档化的 `unsafe` 块；每个 `unsafe` 块前说明指针所有权、有效期和释放责任。

- [ ] **步骤 7：提交**

```bash
git add Cargo.lock backend/Cargo.toml backend/src/kafka/mod.rs backend/src/kafka/topic_admin.rs
git commit -m "feat: 实现 Kafka Topic 管理适配器"
```

### 任务 3：会话管理操作与生命周期

**文件：**
- 修改：`backend/src/sessions/manager.rs`

- [ ] **步骤 1：编写失败的会话并发测试**

扩展 fake factory，使管理操作可阻塞。测试管理操作等待正在进行的快照、后续快照得到 `CollectionInProgress`、HTTP 调用任务被取消后管理操作仍由 manager 持有，以及 `close`、`close_for_cluster`、`close_all` 等待管理操作和客户端析构。

```rust
#[tokio::test]
async fn close_waits_for_cancelled_topic_operation_to_finish() {
    let (manager, factory) = blocking_topic_manager();
    let id = manager.create("cluster-a", config()).await.unwrap();
    let request = tokio::spawn({ let manager = manager.clone(); let id = id.clone();
        async move { manager.create_topic(&id, valid_create()).await }
    });
    factory.started.notified().await;
    request.abort();
    let closing = tokio::spawn({ let manager = manager.clone(); async move { manager.close(&id).await } });
    assert!(!closing.is_finished());
    factory.proceed.notify_one();
    closing.await.unwrap().unwrap();
}
```

- [ ] **步骤 2：运行测试并确认失败**

运行：`cargo test -p backend sessions::manager::tests -- --nocapture`

预期：FAIL，提示 `create_topic` 等管理方法不存在。

- [ ] **步骤 3：扩展 factory 并实现会话拥有的管理任务**

为 `SessionClientFactory` 增加 `create_topic`、`delete_topic`、`describe_topic_configs`、`update_topic_configs` 默认方法；`KafkaSessionClientFactory` 委托给任务 2 的适配器。

在 `SessionEntry` 增加等待型 `lock_operation()`：先检查 `closed`，等待 `collection_lock.lock()`，再检查 `closed`。`SessionManager` 的四个公开方法先 `get_and_renew`，然后 `tokio::spawn` 一个拥有 `Arc<SessionEntry>` 的任务；任务持有 operation lock 和 client lock直到 Admin Future 完成。这样 HTTP Future 被取消时，关闭流程仍会等待客户端锁。

- [ ] **步骤 4：运行会话测试**

运行：`cargo test -p backend sessions::manager::tests -- --nocapture`

预期：PASS；现有 15 秒租约、快照互斥和析构测试保持通过。

- [ ] **步骤 5：提交**

```bash
git add backend/src/sessions/manager.rs
git commit -m "feat: 将 Topic 操作纳入会话生命周期"
```

### 任务 4：Topic 管理 HTTP API

**文件：**
- 创建：`backend/src/api/topics.rs`
- 修改：`backend/src/api/mod.rs`
- 创建：`backend/tests/topic_admin_api.rs`
- 修改：`backend/tests/error_logging.rs`

- [ ] **步骤 1：编写失败的 API 契约测试**

使用可记录调用的 fake factory，覆盖 201 创建、200 查询、200 更新、204 删除、URL 编码、422 校验错误、409 已存在/配置冲突、403 权限错误、404 Topic 不存在、410 会话过期、504 超时和内部 Topic 保护。

```rust
#[tokio::test]
async fn topic_routes_validate_and_forward_only_safe_values() {
    let (app, session_id, calls) = app_with_topic_factory().await;
    let response = request(&app, "POST", &format!("/api/sessions/{session_id}/topics"),
        Some(json!({"name":"orders.v2","partitions":3,"replicationFactor":null,
          "configs":{"retention.ms":"3600000"}}))).await;
    assert_eq!(response.status(), 201);
    assert_eq!(calls.lock().await.created[0].name, "orders.v2");
}
```

- [ ] **步骤 2：运行测试并确认 404/模块缺失失败**

运行：`cargo test -p backend --test topic_admin_api -- --nocapture`

预期：FAIL，Topic 管理路由返回 404 或模块不存在。

- [ ] **步骤 3：实现 handlers 和安全错误映射**

处理器只负责 Axum 提取、领域校验、调用 manager 和响应状态；错误映射集中实现 `From<TopicAdminError> for ApiError`。删除 handler 同时验证 path Topic、`confirmationName` 和内部 Topic。成功状态：创建 201、读取/更新 200、删除 204；“已应用但未确认”返回 202 和固定状态对象。

- [ ] **步骤 4：验证日志脱敏**

在 `error_logging.rs` 使用包含伪 broker 文本和配置值的 fake 原生错误触发各路由，断言响应和捕获日志都只含稳定错误码、Request ID、cluster ID 和路由模板。

- [ ] **步骤 5：运行 API 回归**

运行：

```bash
cargo test -p backend --test topic_admin_api -- --nocapture
cargo test -p backend --test error_logging -- --nocapture
cargo test -p backend --test sessions_api -- --nocapture
```

预期：全部 PASS。

- [ ] **步骤 6：提交**

```bash
git add backend/src/api/mod.rs backend/src/api/topics.rs backend/tests/topic_admin_api.rs backend/tests/error_logging.rs
git commit -m "feat: 提供 Topic 管理 API"
```

### 任务 5：前端 API、配置元数据与会话入口

**文件：**
- 修改：`frontend/src/api/types.ts`
- 修改：`frontend/src/api/client.ts`
- 修改：`frontend/src/features/dashboard/useMonitorSession.ts`
- 创建：`frontend/src/features/topics/topicConfig.ts`
- 创建：`frontend/src/features/topics/topicConfig.test.ts`

- [ ] **步骤 1：编写失败的前端契约测试**

测试 URL 编码、DELETE 确认体、`null` 恢复默认，以及 8 个配置定义的顺序、中文说明、单位、选项与校验。

```ts
it('defines exactly the supported topic settings', () => {
  expect(topicConfigDefinitions.map(item => item.key)).toEqual([
    'retention.ms', 'retention.bytes', 'cleanup.policy', 'segment.ms',
    'segment.bytes', 'min.insync.replicas', 'max.message.bytes', 'compression.type',
  ])
  expect(topicConfigDefinitions.every(item => item.description.length > 12)).toBe(true)
})
```

- [ ] **步骤 2：运行测试并确认失败**

运行：`npm --prefix frontend run test -- --run src/features/topics/topicConfig.test.ts`

预期：FAIL，模块和 API 方法不存在。

- [ ] **步骤 3：实现类型和唯一 HTTP 客户端**

增加 `CreateTopicInput`、`TopicConfigValue`、`TopicConfigsResponse`、`UpdateTopicConfigsInput` 和 `TopicMutationResult`。新增 `topicsApi.create/configs/update/delete`，所有 session ID 和 Topic 路径段均使用 `encodeURIComponent`。

`useMonitorSession` 只在当前 runtime、当前 lease 且已有成功快照时返回 `sessionId`；会话切换、隐藏、过期或出错时不得暴露陈旧 ID。

- [ ] **步骤 4：实现配置展示元数据和前端校验**

`topicConfig.ts` 为每个配置定义 `label`、`description`、`risk`、`unit`、输入类型和合法选项。前端校验与后端一致，但后端仍是权威校验方。

- [ ] **步骤 5：运行测试和类型检查**

运行：

```bash
npm --prefix frontend run test -- --run src/features/topics/topicConfig.test.ts
npm --prefix frontend run build
```

预期：PASS。

- [ ] **步骤 6：提交**

```bash
git add frontend/src/api/client.ts frontend/src/api/types.ts frontend/src/features/dashboard/useMonitorSession.ts frontend/src/features/topics/topicConfig.ts frontend/src/features/topics/topicConfig.test.ts
git commit -m "feat: 增加 Topic 管理前端契约"
```

### 任务 6：创建与配置弹窗

**文件：**
- 创建：`frontend/src/features/topics/CreateTopicModal.tsx`
- 创建：`frontend/src/features/topics/CreateTopicModal.test.tsx`
- 创建：`frontend/src/features/topics/TopicConfigModal.tsx`
- 创建：`frontend/src/features/topics/TopicConfigModal.test.tsx`
- 修改：`frontend/src/index.css`
- 修改：`frontend/src/test/handlers.ts`

- [ ] **步骤 1：编写创建弹窗失败测试**

验证默认分区数为 1、副本因子默认为集群默认、未填写配置不发送、中文说明存在、重复提交被禁用、成功调用 `onApplied`、失败保留输入。

- [ ] **步骤 2：编写配置弹窗失败测试**

MSW 返回 Topic 覆盖值和 Broker 默认值，断言来源徽标、实时值、只读项禁用、恢复默认生成 `null`、只提交变更字段并携带 `expectedValues`、保存成功后重新 GET。GET 失败时保存按钮必须禁用。

```tsx
expect(await screen.findByLabelText('消息保留时间')).toHaveValue(604800000)
expect(screen.getByText('Topic 覆盖值')).toBeVisible()
await user.click(screen.getByRole('button', { name: '恢复 消息保留时间 为 Broker 默认值' }))
await user.click(screen.getByRole('button', { name: '保存配置' }))
expect(received.changes['retention.ms']).toBeNull()
```

- [ ] **步骤 3：运行两个测试文件并确认失败**

运行：`npm --prefix frontend run test -- --run src/features/topics/CreateTopicModal.test.tsx src/features/topics/TopicConfigModal.test.tsx`

预期：FAIL，组件不存在。

- [ ] **步骤 4：实现创建弹窗**

复用 `Modal`，表单分为基础信息和可选配置。字段错误与 API `fields` 合并显示；提交期间 `busy=true`；关闭有未保存内容时先确认。不得把 Demo 示例值预填成真实配置。

- [ ] **步骤 5：实现配置弹窗**

打开时立即 GET；加载期间显示骨架，失败时显示错误与重试按钮且禁用保存。按确认 Demo 使用桌面两列参数卡片、窄屏单列；显示 Kafka key、值、单位、来源、中文说明和风险。保存成功后重新 GET，202 时显示“已提交但暂时无法确认当前值”。

- [ ] **步骤 6：运行组件测试和构建**

运行：

```bash
npm --prefix frontend run test -- --run src/features/topics/CreateTopicModal.test.tsx src/features/topics/TopicConfigModal.test.tsx
npm --prefix frontend run build
```

预期：PASS。

- [ ] **步骤 7：提交**

```bash
git add frontend/src/features/topics/CreateTopicModal.tsx frontend/src/features/topics/CreateTopicModal.test.tsx frontend/src/features/topics/TopicConfigModal.tsx frontend/src/features/topics/TopicConfigModal.test.tsx frontend/src/index.css frontend/src/test/handlers.ts
git commit -m "feat: 添加 Topic 创建与配置弹窗"
```

### 任务 7：Topics 页面集成与安全删除

**文件：**
- 创建：`frontend/src/features/topics/DeleteTopicModal.tsx`
- 创建：`frontend/src/features/topics/DeleteTopicModal.test.tsx`
- 修改：`frontend/src/features/topics/TopicsPage.tsx`
- 修改：`frontend/src/features/topics/TopicsPage.test.tsx`
- 修改：`frontend/src/index.css`

- [ ] **步骤 1：编写失败的页面集成测试**

验证工具栏“新建 Topic”、普通 Topic 的配置/删除按钮、内部 Topic 不显示删除、没有有效 session 时管理按钮禁用、成功后调用 `monitor.refresh()`，以及 202 元数据传播提示。

- [ ] **步骤 2：编写失败的删除确认测试**

验证完整名称匹配前按钮禁用、大小写严格匹配、请求包含 `confirmationName`、失败保留弹窗、成功关闭并刷新。

- [ ] **步骤 3：运行测试并确认失败**

运行：`npm --prefix frontend run test -- --run src/features/topics/TopicsPage.test.tsx src/features/topics/DeleteTopicModal.test.tsx`

预期：FAIL，页面没有管理入口和删除弹窗。

- [ ] **步骤 4：集成三个弹窗与操作状态**

`TopicsPage` 只管理当前打开的弹窗目标和页面级成功提示。创建、配置、删除组件各自拥有表单状态；成功统一调用 `await monitor.refresh()`。内部 Topic 判断同时用于隐藏删除按钮，但不得替代后端保护。

- [ ] **步骤 5：完成桌面与窄屏样式**

操作列保持低视觉权重；危险按钮使用现有 `danger-button`。配置卡片在桌面两列、700px 以下单列；表格仍可横向滚动。确保弹窗标题、输入标签、错误 `role=alert`、状态 `role=status` 和关闭后的焦点恢复。

- [ ] **步骤 6：运行 Topics 全部测试**

运行：`npm --prefix frontend run test -- --run src/features/topics`

预期：PASS，包括现有 Offset 与可访问性测试。

- [ ] **步骤 7：提交**

```bash
git add frontend/src/features/topics/DeleteTopicModal.tsx frontend/src/features/topics/DeleteTopicModal.test.tsx frontend/src/features/topics/TopicsPage.tsx frontend/src/features/topics/TopicsPage.test.tsx frontend/src/index.css
git commit -m "feat: 在 Topics 页面集成管理操作"
```

### 任务 8：真实 Kafka 验收、文档与完整回归

**文件：**
- 修改：`backend/tests/kafka_fixture.rs`
- 修改：`backend/tests/support/kafka_fixture.rs`
- 修改：`frontend/e2e/kafka-dashboard.spec.ts`
- 修改：`README.md`
- 修改：`packaging/README.txt`
- 修改：`AGENTS.md`

- [ ] **步骤 1：先扩展一次性 Kafka fixture 测试**

fixture 仅接受 `127.0.0.1:<port>` 和 `E2E_KAFKA_FIXTURE_PERMIT=disposable-compose`。新增随机后缀管理 Topic，测试创建、读取 `retention.ms`、设置覆盖、删除覆盖恢复默认、删除 Topic；使用清理 guard 确保失败时也只删除本次创建的 Topic。

- [ ] **步骤 2：运行 ignored fixture 验证失败原因**

运行：`cargo test -p backend --test kafka_fixture -- --ignored --nocapture`

预期：在没有脚本提供的一次性 Kafka 时因安全前置条件失败；通过 `scripts/test-e2e.sh` 执行时进入真实验收。

- [ ] **步骤 3：扩展 Playwright 流程**

在现有一次性集群测试中创建 `dashboard-e2e-managed-<run>`，确认列表出现；打开配置弹窗，验证实时 `retention.ms` 来源，修改并重新打开确认回显；恢复默认；输入完整名称删除并等待列表消失。禁止复用 seed Topic 作为删除目标。

- [ ] **步骤 4：更新中文文档**

README 与发行说明明确：

- Kafka Lens 不再是完全只读应用，但消息生产和 Offset 修改仍禁止；
- 4 类 Topic 管理操作和 8 个白名单参数；
- 当前值、Topic 覆盖值、Broker 默认值和恢复默认语义；
- 创建、删除、读取配置、修改配置所需的最小 Kafka ACL；
- 删除不可恢复，降低保留参数可能删除已有数据；
- 服务无内置认证，必须使用受控网络或带认证的 HTTPS 反向代理；
- 不同 Kafka 版本和托管服务可能限制部分参数。

同步修改 `AGENTS.md` 中“禁止创建 Topic”的旧约束，改为只允许本设计定义的受控 Admin 操作；继续禁止生产消息、删除消息、提交/重置 Offset、分区扩容和副本重分配。

- [ ] **步骤 5：运行快速回归**

运行：

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test scripts/e2e-preview.test.mjs
node --test tests/release_scripts.test.mjs
git diff --check
```

预期：全部 PASS，broker 测试只有明确标记的 ignored 项未在普通 `cargo test` 中执行。

- [ ] **步骤 6：运行一次性 Kafka E2E**

运行：`npm --prefix frontend run test:e2e`

预期：PASS；脚本创建并清理唯一测试环境，Topic 管理只触及本次测试创建的资源。

- [ ] **步骤 7：构建并验证发行包**

按顺序运行，禁止并发打包：

```bash
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
```

预期：Apple Silicon macOS 与 Linux/amd64 产物构建成功，校验和、静态页面、`.env`、后台启动生命周期和双平台二进制验证全部通过。

- [ ] **步骤 8：执行秘密与生成文件检查**

运行：

```bash
git status --short
git diff --check
git ls-files | rg '(^|/)(dist|target|node_modules|playwright-report|test-results|data)(/|$)|(^|/)\.env$|\.db($|-)|\.pem$|\.key$|\.pid$|kafka-dashboard\.log$'
```

预期：最后一个命令无输出；提交不包含发行产物、数据库、秘密、日志、PID、锁目录或测试 trace。

- [ ] **步骤 9：提交**

```bash
git add AGENTS.md README.md packaging/README.txt backend/tests/kafka_fixture.rs backend/tests/support/kafka_fixture.rs frontend/e2e/kafka-dashboard.spec.ts
git commit -m "docs: 完成 Topic 管理验收说明"
```

## 最终审查门

- [ ] 使用 `superpowers:requesting-code-review` 检查规格符合性、安全边界、FFI 资源所有权、取消语义和 UI 可访问性。
- [ ] 修复所有必须修复项后，使用 `superpowers:verification-before-completion` 重新运行受影响测试与最终验证命令。
- [ ] 确认工作树只包含计划内提交，不包含 `.superpowers/`、`dist/`、`frontend/dist/`、`target/`、`node_modules/`、SQLite、日志或凭据。
- [ ] 使用 `superpowers:finishing-a-development-branch` 选择合并、PR 或保留分支；不得自动推送或覆盖远端历史。
