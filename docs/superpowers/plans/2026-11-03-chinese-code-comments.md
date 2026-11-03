# 全量中文代码注释实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框跟踪进度。

**目标：** 为仓库生产代码和测试代码中的全部具名函数补充中文职责与调用时机说明，并在关键调用点解释顺序、安全或资源所有权约束。

**架构：** 本次不改变运行架构，只按职责分批补充注释。每批先枚举函数定义，再阅读调用方与测试确认真实用途，最后仅提交注释差异；Rust、前端和脚本分别使用各自惯用的文档注释形式。

**技术栈：** Rust 2024、Axum、Tokio、SQLx、rdkafka、React、TypeScript、Vite、Vitest、Playwright、Node.js、POSIX Shell。

---

## 文件结构与统一格式

- AGENTS.md：规定新增或修改代码时必须维护中文函数说明和关键调用说明。
- backend/src/**/*.rs：领域、仓储、Kafka、会话、告警、API 与进程入口的中文文档注释。
- backend/tests/**/*.rs：测试辅助函数用途和测试函数覆盖的回归风险。
- frontend/src/**/*.{ts,tsx}：API、组件、Hook、纯函数和交互处理函数的职责与调用生命周期。
- frontend/e2e、frontend/scripts：浏览器验收步骤、安全探针与报告器的调用时机。
- packaging、scripts、tools、tests：启动、构建、验证、发布函数和脚本测试的入口与安全边界。

不新增依赖，不修改接口与实现，不触碰生成目录、锁文件、迁移 SQL、HTML、CSS 或 JSON。

唯一例外是已获用户确认的基线兼容修复：将测试代码中已弃用的 `AtomicUsize::fetch_update` 更名为语义一致的 `try_update`，使项目规定的 `Clippy -D warnings` 门禁恢复可用。

Rust 公共接口与重要内部函数使用文档注释：

~~~rust
/// 汇总分区水位并保留未知状态；Kafka 快照采集完成后构建 Topic 视图时调用。
fn summarize_topic(...) -> ... {
~~~

TypeScript、TSX 的导出函数、组件和 Hook 使用 JSDoc，内部函数可用紧邻定义的中文单行注释：

~~~ts
/** 管理仪表盘会话和轮询；仪表盘布局挂载并选择有效集群后调用。 */
export function useMonitorSession(...) {
~~~

Shell 和测试辅助函数使用定义前注释：

~~~sh
# 校验 PID 对应的完整进程身份；status、stop 和重复启动检查时调用。
inspect_process() {
~~~

关键调用点说明原因而非翻译语句：

~~~rust
// 先停止并等待旧 worker 析构，避免新旧 Kafka 客户端同时使用不同版本配置。
let _ = worker.task.await;
~~~

### 任务 0：恢复 Clippy 基线

**文件：**
- 修改：backend/tests/alert_monitor.rs

- [ ] **步骤 1：确认弃用错误**

~~~bash
cargo clippy --workspace --all-targets -- -D warnings
~~~

预期：仅因 `AtomicUsize::fetch_update` 已弃用而失败，并建议使用 `try_update`。

- [ ] **步骤 2：进行语义一致的最小替换**

将 `DropGate::wait` 中的调用改为：

~~~rust
self.remaining
    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
~~~

不改动闭包、内存顺序或测试流程。

- [ ] **步骤 3：验证并提交基线修复**

~~~bash
cargo test --test alert_monitor
cargo clippy --workspace --all-targets -- -D warnings
git diff --check
git add backend/tests/alert_monitor.rs
git commit -m "fix: 更新原子计数测试接口"
~~~

预期：告警监控测试和 Clippy 均通过。

### 任务 1：写入长期中文注释约束

**文件：**
- 修改：AGENTS.md

- [ ] **步骤 1：增加“中文代码注释”章节**

写明每个具名函数、方法、React 组件、Hook 和脚本函数都要说明职责与调用时机；测试函数说明验证的行为或风险；关键异步、锁、事务、Kafka、会话、告警、Topic 和发布调用说明原因与顺序。

- [ ] **步骤 2：检查约束完整性**

~~~bash
rg -n '中文代码注释|负责什么|何时调用|关键函数调用|行为变化' AGENTS.md
git diff --check
~~~

预期：五类约束均能定位，且无空白错误。

- [ ] **步骤 3：提交约束**

~~~bash
git add AGENTS.md
git commit -m "docs: 增加中文代码注释约束"
~~~

### 任务 2：注释 Rust 领域、配置与仓储代码

**文件：**
- 修改：backend/src/config.rs
- 修改：backend/src/error.rs
- 修改：backend/src/domain/cluster.rs
- 修改：backend/src/domain/feishu_alert.rs
- 修改：backend/src/domain/lag_rule.rs
- 修改：backend/src/domain/offset.rs
- 修改：backend/src/domain/snapshot.rs
- 修改：backend/src/domain/topic_admin.rs
- 修改：backend/src/db/mod.rs
- 修改：backend/src/db/clusters.rs
- 修改：backend/src/db/feishu_alerts.rs
- 修改：backend/src/db/lag_rules.rs

- [ ] **步骤 1：列出本批具名函数和已有中文说明**

~~~bash
rg -n '^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+' backend/src/config.rs backend/src/error.rs backend/src/domain backend/src/db
rg -n '^\s*///.*[一-龥]|^\s*//.*[一-龥]' backend/src/config.rs backend/src/error.rs backend/src/domain backend/src/db
~~~

- [ ] **步骤 2：补充函数职责与调用时机**

领域计算说明输入语义、未知值保留和调用阶段；仓储函数说明由哪个 API、后台监控或启动迁移流程调用；事务函数说明原子替换和回滚边界。模块内测试说明所覆盖的边界或回归风险。

- [ ] **步骤 3：注释关键调用点**

解释数据库事务开启与提交、敏感配置合并、告警 revision 更新、Lag 未知值传播，以及输入校验为何必须在持久化或原生调用前完成。

- [ ] **步骤 4：验证并提交**

~~~bash
cargo fmt --all -- --check
cargo test --workspace domain::
cargo test --test repositories
git diff --check
git add backend/src/config.rs backend/src/error.rs backend/src/domain backend/src/db
git commit -m "docs: 补充领域与仓储中文注释"
~~~

### 任务 3：注释 Rust Kafka、会话、告警与通知代码

**文件：**
- 修改：backend/src/kafka/client.rs
- 修改：backend/src/kafka/collector.rs
- 修改：backend/src/kafka/config.rs
- 修改：backend/src/kafka/group_consumers.rs
- 修改：backend/src/kafka/mod.rs
- 修改：backend/src/kafka/topic_admin.rs
- 修改：backend/src/sessions/manager.rs
- 修改：backend/src/sessions/mod.rs
- 修改：backend/src/alerts/monitor.rs
- 修改：backend/src/alerts/mod.rs
- 修改：backend/src/notifications/feishu.rs
- 修改：backend/src/notifications/mod.rs

- [ ] **步骤 1：枚举函数和原生边界**

~~~bash
rg -n '^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+' backend/src/kafka backend/src/sessions backend/src/alerts backend/src/notifications
rg -n 'spawn|spawn_blocking|JoinSet|rd_kafka_|await|shutdown|close' backend/src/kafka backend/src/sessions backend/src/alerts backend/src/notifications
~~~

- [ ] **步骤 2：补充职责、调用时机和关键调用原因**

说明 Kafka 客户端创建、快照采集、Group 关联、Topic Admin、会话租约、告警协调 worker、飞书格式化与发送的调用场景。重点解释有界并发、阻塞任务所有权、原生指针释放、会话互斥、关闭等待、revision 复核、worker 退役、告警重试和 HTTP 安全边界。

- [ ] **步骤 3：验证并提交**

~~~bash
cargo fmt --all -- --check
cargo test --workspace kafka::
cargo test --workspace sessions::
cargo test --workspace notifications::
cargo test --test alert_monitor
git diff --check
git add backend/src/kafka backend/src/sessions backend/src/alerts backend/src/notifications
git commit -m "docs: 补充 Kafka 与并发流程中文注释"
~~~

### 任务 4：注释 Rust API、静态资源与进程入口

**文件：**
- 修改：backend/src/api/clusters.rs
- 修改：backend/src/api/feishu_alerts.rs
- 修改：backend/src/api/health.rs
- 修改：backend/src/api/lag_rules.rs
- 修改：backend/src/api/mod.rs
- 修改：backend/src/api/sessions.rs
- 修改：backend/src/api/static_assets.rs
- 修改：backend/src/api/static_assets/filesystem.rs
- 修改：backend/src/api/topics.rs
- 修改：backend/src/lib.rs
- 修改：backend/src/main.rs

- [ ] **步骤 1：枚举 handler、校验器和启动函数**

~~~bash
rg -n '^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+' backend/src/api backend/src/lib.rs backend/src/main.rs
~~~

- [ ] **步骤 2：补充职责、调用时机和安全顺序**

说明 handler 对应请求、校验阶段、静态资源与 SPA fallback、初始化和优雅退出。解释请求上限、Request ID、错误脱敏、API 404、目录能力、符号链接防护、会话关闭和 SIGTERM 资源排空。

- [ ] **步骤 3：验证并提交**

~~~bash
cargo fmt --all -- --check
cargo test --workspace --test clusters_api --test feishu_alerts_api --test sessions_api --test static_assets_api --test topic_admin_api --test server_process
git diff --check
git add backend/src/api backend/src/lib.rs backend/src/main.rs
git commit -m "docs: 补充 API 与进程入口中文注释"
~~~

### 任务 5：注释全部 Rust 集成测试与辅助代码

**文件：**
- 修改：backend/tests/alert_monitor.rs
- 修改：backend/tests/cluster_drafts_api.rs
- 修改：backend/tests/cluster_secrets_api.rs
- 修改：backend/tests/clusters_api.rs
- 修改：backend/tests/error_logging.rs
- 修改：backend/tests/feishu_alerts_api.rs
- 修改：backend/tests/health_api.rs
- 修改：backend/tests/kafka_connection.rs
- 修改：backend/tests/kafka_fixture.rs
- 修改：backend/tests/kafka_snapshot.rs
- 修改：backend/tests/lag_rules_api.rs
- 修改：backend/tests/repositories.rs
- 修改：backend/tests/server_process.rs
- 修改：backend/tests/sessions_api.rs
- 修改：backend/tests/static_assets_api.rs
- 修改：backend/tests/topic_admin_api.rs
- 修改：backend/tests/support/kafka_fixture.rs
- 修改：backend/tests/support/mod.rs

- [ ] **步骤 1：枚举测试与辅助函数**

~~~bash
rg -n '^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+' backend/tests
~~~

- [ ] **步骤 2：添加测试意图和关键测试调用注释**

每个测试说明验证的行为与防止的回归；辅助函数说明由哪些测试阶段调用。解释故障注入、取消安全、敏感信息断言、一次性 Kafka 写入许可、真实进程信号与清理 guard 的顺序。

- [ ] **步骤 3：验证并提交**

~~~bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
git add backend/tests
git commit -m "docs: 补充 Rust 测试中文注释"
~~~

### 任务 6：注释前端 API、公共组件与仪表盘基础设施

**文件：**
- 修改：frontend/src/api/client.ts
- 修改：frontend/src/api/client.test.ts
- 修改：frontend/src/api/types.ts
- 修改：frontend/src/app/App.tsx
- 修改：frontend/src/app/App.test.tsx
- 修改：frontend/src/main.tsx
- 修改：frontend/src/components/MetricHelp.tsx
- 修改：frontend/src/components/Modal.tsx
- 修改：frontend/src/components/StaleNotice.tsx
- 修改：frontend/src/components/StatusBadge.tsx
- 修改：frontend/src/features/dashboard/DashboardLayout.tsx
- 修改：frontend/src/features/dashboard/DashboardLayout.test.tsx
- 修改：frontend/src/features/dashboard/displaySnapshot.ts
- 修改：frontend/src/features/dashboard/displaySnapshot.test.ts
- 修改：frontend/src/features/dashboard/history.ts
- 修改：frontend/src/features/dashboard/history.test.ts
- 修改：frontend/src/features/dashboard/sessionTransitions.ts
- 修改：frontend/src/features/dashboard/useDashboard.ts
- 修改：frontend/src/features/dashboard/useMonitorSession.ts
- 修改：frontend/src/features/dashboard/useMonitorSession.test.tsx
- 修改：frontend/src/features/dashboard/useSnapshotStale.ts

- [ ] **步骤 1：枚举函数、组件、Hook 和具名箭头函数**

~~~bash
rg -n '^(export\s+)?(async\s+)?function\s+|^(export\s+)?const\s+[A-Za-z0-9_]+\s*=\s*(async\s*)?\(' frontend/src/api frontend/src/app frontend/src/components frontend/src/features/dashboard
~~~

另人工检查 useCallback、useMemo 和测试文件中的具名辅助函数。

- [ ] **步骤 2：补充职责、生命周期和关键清理原因**

说明 API 请求阶段、组件渲染场景、上下文消费位置、会话 Hook 挂载/切换/卸载、历史和陈旧状态计算。解释请求取消、代次防陈旧覆盖、页面可见性、会话关闭和定时器清理。

- [ ] **步骤 3：验证并提交**

~~~bash
npm --prefix frontend run test -- --run src/api src/app src/components src/features/dashboard
npm --prefix frontend run build
git diff --check
git add frontend/src/api frontend/src/app frontend/src/main.tsx frontend/src/components frontend/src/features/dashboard
git commit -m "docs: 补充前端基础设施中文注释"
~~~

### 任务 7：注释前端业务功能、测试与浏览器验收

**文件：**
- 修改：frontend/src/features/charts
- 修改：frontend/src/features/clusters
- 修改：frontend/src/features/consumer-groups
- 修改：frontend/src/features/feishu-alerts
- 修改：frontend/src/features/lag-rules
- 修改：frontend/src/features/overview
- 修改：frontend/src/features/topics
- 修改：frontend/src/test
- 修改：frontend/e2e/kafka-dashboard.spec.ts
- 修改：frontend/e2e/token-safety.mjs
- 修改：frontend/scripts/token-safety-probes/config.mjs
- 修改：frontend/scripts/token-safety-probes/probe.spec.mjs
- 修改：frontend/scripts/token-safety-probes/reporter.mjs
- 修改：frontend/scripts/token-safety.test.mjs

- [ ] **步骤 1：枚举业务函数与测试辅助函数**

~~~bash
rg -n '^(export\s+)?(async\s+)?function\s+|^(export\s+)?const\s+[A-Za-z0-9_]+\s*=\s*(async\s*)?\(' frontend/src/features frontend/src/test frontend/e2e frontend/scripts
~~~

人工补查组件内部事件处理器、useCallback 和 Playwright helper。

- [ ] **步骤 2：补充职责、调用时机和关键交互原因**

页面组件说明路由渲染时机；弹窗与表单说明打开、编辑、保存或删除阶段；数据转换说明快照到视图调用链；测试与 E2E helper 说明交互风险和秘密保护。解释 Token 显隐与清空、规则快照、Topic 删除确认、配置冲突、请求取消、测试拦截和附件扫描。

- [ ] **步骤 3：验证并提交**

~~~bash
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test frontend/scripts/token-safety.test.mjs
git diff --check
git add frontend/src/features frontend/src/test frontend/e2e frontend/scripts
git commit -m "docs: 补充前端业务与验收中文注释"
~~~

### 任务 8：注释启动、构建、验证、发布脚本及测试

**文件：**
- 修改：packaging/start.sh
- 修改：scripts/e2e-preview.mjs
- 修改：scripts/e2e-preview.test.mjs
- 修改：scripts/test-e2e.sh
- 修改：tools/build-release.sh
- 修改：tools/publish-release.mjs
- 修改：tools/verify-release.sh
- 修改：tests/ci_workflow.test.mjs
- 修改：tests/frontend_styles.test.mjs
- 修改：tests/publish_release.test.mjs
- 修改：tests/release_scripts.test.mjs

- [ ] **步骤 1：枚举 Shell 与 Node.js 函数**

~~~bash
rg -n '^[A-Za-z_][A-Za-z0-9_]*\(\)\s*\{|^(export\s+)?(async\s+)?function\s+|^const\s+[A-Za-z0-9_]+\s*=\s*(async\s*)?\(' packaging scripts tools tests
~~~

- [ ] **步骤 2：补充职责、入口和关键安全调用原因**

说明启动器子命令、平台选择、构建临时目录与归档、发布草稿与附件校验、测试 fixture 的调用时机。解释锁、PID 身份、SIGTERM、部署目录保护、commit/dirty、checksum、逐字节核验和失败取消草稿的顺序。

- [ ] **步骤 3：验证并提交**

~~~bash
sh -n packaging/start.sh tools/build-release.sh tools/verify-release.sh scripts/test-e2e.sh
node --check scripts/e2e-preview.mjs
node --check tools/publish-release.mjs
node --test scripts/e2e-preview.test.mjs
node --test tests/ci_workflow.test.mjs tests/frontend_styles.test.mjs tests/publish_release.test.mjs tests/release_scripts.test.mjs
git diff --check
git add packaging scripts tools tests
git commit -m "docs: 补充启动与发布脚本中文注释"
~~~

### 任务 9：全仓库覆盖审计与最终验证

**文件：**
- 复核：AGENTS.md
- 复核：backend/src 与 backend/tests
- 复核：frontend/src、frontend/e2e 与 frontend/scripts
- 复核：packaging、scripts、tools 与 tests

- [ ] **步骤 1：生成剩余函数候选清单**

~~~bash
rg -n '^\s*(pub(\([^)]*\))?\s+)?(async\s+)?fn\s+' backend/src backend/tests
rg -n '^(export\s+)?(async\s+)?function\s+|^(export\s+)?const\s+[A-Za-z0-9_]+\s*=\s*(async\s*)?\(' frontend/src frontend/e2e frontend/scripts scripts tools tests
rg -n '^[A-Za-z_][A-Za-z0-9_]*\(\)\s*\{' packaging scripts tools
~~~

逐个检查定义前邻近注释是否含中文，并同时描述职责与调用时机。正则无法证明语义，最终结论必须结合人工复核。

- [ ] **步骤 2：检查关键调用注释覆盖**

~~~bash
rg -n 'spawn|spawn_blocking|JoinSet|transaction|commit\(|rollback|rd_kafka_|shutdown|close\(|AbortController|clearInterval|kill |gh release|sha256|shasum' backend/src frontend/src packaging scripts tools
~~~

逐项确认复杂调用附近有解释原因或顺序的中文注释；简单 getter、展示逻辑和测试 mock 不制造噪声注释。

- [ ] **步骤 3：运行完整验证**

~~~bash
sh -n packaging/start.sh tools/build-release.sh tools/verify-release.sh scripts/test-e2e.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test scripts/e2e-preview.test.mjs
node --test tests/ci_workflow.test.mjs tests/frontend_styles.test.mjs tests/publish_release.test.mjs tests/release_scripts.test.mjs
git diff --check
~~~

预期：全部命令退出 0。注释不改变行为，因此不重新构建三平台发行包或运行真实 Kafka E2E；若代码 token 被误改，则恢复后重新验证。

- [ ] **步骤 4：检查差异与秘密**

~~~bash
git diff main...HEAD --stat
git diff main...HEAD --check
git grep -n -I -E '(access_token=|hook/[A-Za-z0-9_-]{20,}|BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY)' -- ':!packaging/.env.example' ':!frontend/package-lock.json'
~~~

预期：差异只包含 AGENTS.md、设计/计划文档和注释；秘密扫描不出现真实凭据。

- [ ] **步骤 5：提交审计修正**

若审计补充了遗漏注释：

~~~bash
git add AGENTS.md backend frontend packaging scripts tools tests
git commit -m "docs: 完成中文代码注释覆盖审计"
~~~

若没有遗漏，不创建空提交。

## 完成定义

- 每个具名函数、方法、组件、Hook 和脚本函数都有中文职责与调用时机说明。
- 承载关键业务逻辑的匿名回调或闭包有中文意图说明。
- 关键资源、并发、安全和发布调用点说明调用原因及顺序约束。
- 测试注释解释验证目标与回归风险，不逐行翻译断言。
- 除已确认的原子计数弃用接口更名外，所有差异均为开发约束、设计/计划文档或注释，不改变可执行行为。
- 完整验证通过，且未引入秘密或生成文件。
