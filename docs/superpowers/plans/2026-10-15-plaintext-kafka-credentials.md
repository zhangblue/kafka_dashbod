# Kafka 凭据明文存储实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 删除 `KAFKA_DASHBOARD_MASTER_KEY`，将 Kafka 敏感配置明文保存在 SQLite，并明确拒绝旧加密数据库。

**架构：** `ClusterRepository` 直接序列化/反序列化 `ClusterSecrets` JSON，不再持有密码器；`Config` 只负责数据库 URL 和静态目录。仅供新数据库建表的初始 SQL 文件把 `encrypted_secrets` 改为 `secrets_json`；不编写任何旧数据迁移、转换或自动删除逻辑。SQLx 的 `MigrateError::VersionMismatch` 只映射为安全、明确的手动删除提示；发行启动器不再接收或检查主密钥。

**技术栈：** Rust 2024、SQLx 0.9/SQLite、Serde JSON、Axum、Node.js test runner、POSIX shell、Docker Buildx。

---

## 文件结构

- 删除 `backend/src/crypto.rs`：删除 AES-256-GCM 密码器。
- 修改 `backend/src/lib.rs`：取消导出 `crypto` 模块。
- 修改 `backend/src/config.rs`：移除主密钥配置，只保留数据库 URL 和 Web 目录。
- 修改 `backend/src/db/clusters.rs`：以 JSON 字节直接保存和读取 `ClusterSecrets`。
- 修改 `backend/src/db/mod.rs`：删除加密错误，增加数据库初始化错误和旧迁移不兼容映射。
- 修改 `backend/src/api/mod.rs`、`backend/src/main.rs`：应用状态不再注入密码器，显示安全的数据库兼容错误。
- 修改 `backend/migrations/0001_initial.sql`：将 `encrypted_secrets` 改为 `secrets_json`。
- 修改 `backend/Cargo.toml`、`Cargo.lock`：删除仅用于凭据加密的依赖。
- 修改 `backend/tests/repositories.rs`、`backend/tests/cluster_secrets_api.rs`：验证明文落库和 API 不回显。
- 修改 `backend/tests/server_process.rs`、`backend/tests/static_assets_api.rs`：验证无主密钥启动及旧数据库拒绝。
- 修改 `backend/tests/support/mod.rs`、`backend/tests/sessions_api.rs`、`backend/tests/kafka_snapshot.rs`、`backend/tests/cluster_drafts_api.rs`：适配无密码器构造函数。
- 修改 `packaging/start.sh`、`packaging/.env.example`、`packaging/README.txt`：删除主密钥要求并保留平台选择逻辑。
- 修改 `tools/verify-release.sh`、`tests/release_scripts.test.mjs`：验证无主密钥的双平台发行启动。
- 修改 `scripts/test-e2e.sh`：删除测试主密钥生成与日志扫描输入。
- 修改 `README.md`、`AGENTS.md`：记录明文风险、不兼容升级步骤和二进制选择规则。

### 任务 1：用明文 JSON 替换加密凭据仓库

**文件：**
- 删除：`backend/src/crypto.rs`
- 修改：`backend/src/lib.rs`
- 修改：`backend/src/db/clusters.rs`
- 修改：`backend/src/db/mod.rs`
- 修改：`backend/src/api/mod.rs`
- 修改：`backend/migrations/0001_initial.sql`
- 修改：`backend/Cargo.toml`
- 修改：`Cargo.lock`
- 修改：`backend/tests/repositories.rs`
- 修改：`backend/tests/cluster_secrets_api.rs`
- 修改：`backend/tests/support/mod.rs`
- 修改：`backend/tests/sessions_api.rs`
- 修改：`backend/tests/kafka_snapshot.rs`
- 修改：`backend/tests/cluster_drafts_api.rs`
- 修改：`backend/tests/static_assets_api.rs`

- [ ] **步骤 1：编写明文持久化失败测试**

在 `backend/tests/repositories.rs` 将现有加密 round-trip 测试替换为行为测试。这个测试要抓住的破坏是“仓库仍写入密文或换了错误字段名”：

```rust
#[tokio::test]
async fn clusters_store_plaintext_secret_json_but_public_reads_only_expose_flags() {
    let pool = db::connect("sqlite::memory:").await.unwrap();
    let repo = ClusterRepository::new(pool.clone());
    let input = secure_cluster_input("plain-secret", "client-key-passphrase");
    let created = repo.create(&input).await.unwrap();

    let stored: Vec<u8> = sqlx::query_scalar(
        "SELECT secrets_json FROM clusters WHERE id = ?",
    )
    .bind(&created.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&stored).unwrap();
    assert_eq!(json["password"], "plain-secret");
    assert_eq!(json["client_key_passphrase"], "client-key-passphrase");

    let dto = repo.get(&created.id).await.unwrap().unwrap();
    assert!(dto.password_configured);
    assert!(dto.client_key_passphrase_configured);
    assert!(!serde_json::to_string(&dto).unwrap().contains("plain-secret"));
}
```

在 `backend/tests/cluster_secrets_api.rs` 把读取并解密 `encrypted_secrets` 的辅助函数改成直接读取 `secrets_json`，继续覆盖五类敏感字段的保留、替换、清除、协议切换和不回显。

- [ ] **步骤 2：运行测试验证正确红灯**

运行：

```bash
cargo test --test repositories clusters_store_plaintext_secret_json_but_public_reads_only_expose_flags -- --exact
```

预期：FAIL，SQLite 报告没有 `secrets_json` 列，证明测试能抓住旧加密实现。

- [ ] **步骤 3：实现最小明文仓库**

将迁移字段改为：

```sql
secrets_json BLOB,
```

将 `ClusterRepository` 改为只持有连接池，并用专用方法隔离序列化边界：

```rust
#[derive(Clone)]
pub struct ClusterRepository {
    pool: SqlitePool,
}

impl ClusterRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    fn encode_secrets(secrets: &ClusterSecrets) -> Result<Vec<u8>, RepositoryError> {
        serde_json::to_vec(secrets).map_err(|_| RepositoryError::InvalidInput)
    }

    fn decode_secrets(stored: Option<Vec<u8>>) -> Result<ClusterSecrets, RepositoryError> {
        match stored {
            Some(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| RepositoryError::InvalidStoredConfig),
            None => Ok(ClusterSecrets::default()),
        }
    }
}
```

把所有 SQL、row 字段和事务查询从 `encrypted_secrets` 改为 `secrets_json`。`AppState::new(pool)` 和所有测试 helper 直接调用 `ClusterRepository::new(pool)`，不再构造密码器。

删除 `RepositoryError::Crypto`、`backend/src/crypto.rs`、`pub mod crypto`，并从 `backend/Cargo.toml` 删除 `aes-gcm`、`base64`；只有在 `rg 'rand::|rand =' backend` 确认无其他生产用途时才删除 `rand`。运行 `cargo update -p backend` 不适用；直接执行 `cargo check --workspace` 让 Cargo 更新锁文件后确认差异只移除不再可达的包。

- [ ] **步骤 4：运行定向和完整后端测试验证绿灯**

运行：

```bash
cargo test --test repositories
cargo test --test cluster_secrets_api
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

预期：仓库/API 测试全绿，API 响应仍不包含五类敏感值，完整 Rust 套件无失败。

- [ ] **步骤 5：变异检查与提交**

临时把 `encode_secrets` 返回空 JSON，确认明文字段断言失败；恢复实现并重跑定向测试。随后：

```bash
git add Cargo.toml Cargo.lock backend
git commit -m "refactor: store Kafka credentials as plaintext JSON"
```

### 任务 2：无主密钥启动并安全拒绝旧数据库

**文件：**
- 修改：`backend/src/config.rs`
- 修改：`backend/src/db/mod.rs`
- 修改：`backend/src/main.rs`
- 修改：`backend/tests/server_process.rs`
- 修改：`backend/tests/static_assets_api.rs`

- [ ] **步骤 1：编写无密钥启动和旧数据库失败测试**

从 `configured_listener_serves_health_and_sigterm_exits_successfully` 删除主密钥环境变量，并显式 `.env_remove("KAFKA_DASHBOARD_MASTER_KEY")`。这会在旧实现上因缺少密钥而失败。

新增进程测试：先用当前 `db::connect` 创建临时 SQLite，再把 `_sqlx_migrations` 中版本 1 的 checksum 改为 `x'00'`，关闭连接池后启动二进制。断言非零退出，stderr 包含固定安全提示，但不包含 checksum、SQL 或数据库内容：

```rust
assert!(stderr.contains("configuration database is incompatible"));
assert!(stderr.contains("delete it and configure clusters again"));
assert!(!stderr.contains("checksum"));
assert!(!stderr.contains("SELECT"));
```

- [ ] **步骤 2：运行进程测试验证红灯**

运行：

```bash
cargo test --test server_process -- --nocapture
```

预期：无密钥健康检查失败于 `KAFKA_DASHBOARD_MASTER_KEY`；旧校验和测试只得到旧的泛化数据库错误，缺少兼容提示。

- [ ] **步骤 3：删除主密钥配置并映射 SQLx 迁移错误**

将 `Config` 收敛为：

```rust
pub struct Config {
    pub database_url: String,
    pub web_dir: Option<PathBuf>,
}
```

`Config::from_env()` 不读取 `KAFKA_DASHBOARD_MASTER_KEY`。保留 Web 目录在 SQLite 初始化前的校验顺序。

为数据库初始化定义只输出安全文案的错误：

```rust
#[derive(Debug, thiserror::Error)]
pub enum DatabaseInitError {
    #[error("configuration database is incompatible; delete it and configure clusters again")]
    Incompatible,
    #[error("failed to initialize configuration database")]
    Database,
}
```

按照 SQLx 0.9 文档，`Migrator::run` 返回 `MigrateError`，其中 `VersionMismatch(i64)` 表示已应用迁移校验和不同。只把这个变体映射为 `Incompatible`；连接、权限及其他迁移错误统一映射为 `Database`，不输出底层详情。`main` 使用 `error.to_string()` 创建 `io::Error`，并调用 `AppState::new(pool)`。

- [ ] **步骤 4：验证启动行为和错误脱敏**

运行：

```bash
cargo test --test server_process -- --nocapture
cargo test --test static_assets_api
cargo test --workspace
```

预期：无密钥启动通过；Web 目录仍先于数据库失败；伪造旧 checksum 被明确拒绝且不泄漏 SQLx 细节。

- [ ] **步骤 5：提交**

```bash
git add backend/src/config.rs backend/src/db/mod.rs backend/src/main.rs backend/tests/server_process.rs backend/tests/static_assets_api.rs
git commit -m "feat: start without a credential master key"
```

### 任务 3：更新发行启动器、验收脚本和用户文档

**文件：**
- 修改：`packaging/start.sh`
- 修改：`packaging/.env.example`
- 修改：`packaging/README.txt`
- 修改：`tools/verify-release.sh`
- 修改：`tests/release_scripts.test.mjs`
- 修改：`scripts/test-e2e.sh`
- 修改：`README.md`
- 修改：`AGENTS.md`

- [ ] **步骤 1：先把发行测试改为期望无密钥启动**

在 `tests/release_scripts.test.mjs` 的 fixture 中删除自动注入的 `KAFKA_DASHBOARD_MASTER_KEY`，把旧的“缺少主密钥拒绝启动”测试替换为：

```javascript
test('starts without a master key or env file', async t => {
  const f = await fixture(t);
  const result = f.run([], { KAFKA_DASHBOARD_MASTER_KEY: '' });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(existsSync(path.join(f.release, 'data')), true);
});
```

保留并继续断言 `Darwin/arm64` 只输出 `darwin-arm64`、`Linux/x86_64` 只输出 `linux-amd64`，参数边界和退出码不变。

- [ ] **步骤 2：运行脚本测试验证红灯**

运行：

```bash
node --test tests/release_scripts.test.mjs
```

预期：FAIL，`packaging/start.sh` 仍报告主密钥必填。

- [ ] **步骤 3：实现无密钥发行行为**

删除 `packaging/start.sh` 的主密钥检查。`.env.example` 只保留可选的 bind、database、web-dir 示例；`packaging/README.txt` 删除密钥生成、导出和备份说明，增加“SQLite 明文保存 Kafka 凭据”的醒目警告。

从 `tools/verify-release.sh` 删除 placeholder grep、随机密钥生成、容器 `-e KAFKA_DASHBOARD_MASTER_KEY` 和主密钥日志比较；改为在 macOS 与 Linux 启动前显式 `unset KAFKA_DASHBOARD_MASTER_KEY`，证明成品没有隐式依赖。保留 checksum、文件 allowlist、SQLite、健康检查、静态资源、SPA fallback 和 SIGTERM 验证。

从 `scripts/test-e2e.sh` 删除主密钥生成，并从日志敏感值数组移除该变量；真实 Kafka fixture 的 SASL/TLS 测试秘密仍按现有规则扫描。

- [ ] **步骤 4：更新当前用户文档与维护约束**

更新 `README.md`：本地启动示例不再导出主密钥；环境变量表删除该行；连接与凭据章节改为明文存储警告；发行升级步骤要求删除旧 DB 和 sidecar 后重新配置；说明 `start.sh` 的两个平台映射。

更新 `AGENTS.md`：删除密钥格式要求，改为“SQLite 含明文 Kafka 凭据，API/日志仍不得回显”。历史设计与历史实现计划保留为决策记录，不把其中旧描述当成当前操作指南。

- [ ] **步骤 5：运行脚本、文档和 E2E 验证**

运行：

```bash
node --test tests/release_scripts.test.mjs
rg -n "KAFKA_DASHBOARD_MASTER_KEY" README.md AGENTS.md packaging tools scripts tests backend/src backend/tests
npm --prefix frontend run test:e2e
git diff --check
```

预期：Node 测试和 E2E 全绿；`rg` 无当前代码/运维文档命中；历史 specs/plans 不在扫描范围内。

- [ ] **步骤 6：提交**

```bash
git add README.md AGENTS.md packaging tools scripts tests
git commit -m "docs: remove master key deployment requirement"
```

### 任务 4：独立审查、完整验证和双平台交付

**文件：**
- 验证：全仓库
- 生成（忽略）：`dist/kafka-dashboard/`
- 生成（忽略）：`dist/kafka-dashboard.tar.gz`

- [ ] **步骤 1：请求独立代码审查**

以 `524dcf4` 为 base、当前 HEAD 为 head 创建审查包。审查重点：明文范围是否只限 SQLite、API/日志是否继续脱敏、旧数据库是否安全拒绝、无主密钥启动、两个平台二进制选择、是否残留加密依赖或配置要求。Critical/Important 必须在继续前修复并复审。

- [ ] **步骤 2：运行完整源码验证**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run lint
npm --prefix frontend run build
node --test tests/release_scripts.test.mjs
npm --prefix frontend run test:e2e
git diff --check
git status -sb
```

预期：全部退出码为 0。前端既有 chunk size 警告可记录，但不能有编译或 lint 失败。

- [ ] **步骤 3：运行授权 Kafka 的只读集成测试**

```bash
TEST_KAFKA_BROKERS=192.168.250.106:9092 \
  cargo test --test kafka_connection plaintext_broker_connection_succeeds -- --ignored --exact
TEST_KAFKA_BROKERS=192.168.250.106:9092 \
  cargo test --test kafka_snapshot readonly_broker_snapshot_contains_all_discoverable_resources -- --ignored --exact
```

预期：两项通过；测试不创建 Topic、不生产、不订阅、不提交 Offset。

- [ ] **步骤 4：重建并验证双平台发行包**

```bash
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
```

预期：发行包同时包含 Darwin/arm64 与 Linux/x86_64 二进制；两个平台在没有 `KAFKA_DASHBOARD_MASTER_KEY` 时通过健康、SQLite、静态资源、SPA fallback 和 SIGTERM 验证。

- [ ] **步骤 5：秘密与交付状态检查**

```bash
rg -n --hidden --glob '!.git/**' --glob '!dist/**' \
  '(password|passwd|secret|token|api[_-]?key|private[_-]?key)\s*[:=]\s*["'\''`][^"'\''`]{4,}' .
git status -sb
git log --oneline 524dcf4..HEAD
```

逐条检查扫描命中，确认只有测试 fixture 或字段名，没有真实凭据。工作树必须干净，再按用户选择执行合并和推送。
