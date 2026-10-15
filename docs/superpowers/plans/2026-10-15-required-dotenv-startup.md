# Kafka Lens 强制加载 `.env` 实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 让 Kafka Lens 正常启动时由 Rust 强制读取当前工作目录的 `.env`，让发行启动器停止注入配置，并提供中文发行说明。

**架构：** 二进制入口在处理正常服务启动前调用 `dotenvy::from_path(".env")`，以精确限定当前工作目录而不向父目录搜索；父进程环境变量由 dotenvy 的非 override 语义保留。`start.sh` 只负责发行目录定位、平台选择、私有权限和进程替换，发行验证在临时副本中从 `.env.example` 创建 `.env`。

**技术栈：** Rust 2024、dotenvy、Cargo 集成测试、POSIX shell、Node.js 内置测试运行器

---

## 文件结构

- 修改 `backend/Cargo.toml`：加入 `dotenvy` 运行时依赖。
- 修改 `Cargo.lock`：锁定新增依赖版本。
- 修改 `backend/src/main.rs`：在正常启动路径中安全加载必需的 `.env`。
- 修改 `backend/tests/server_process.rs`：用真实二进制覆盖缺失、成功加载、环境覆盖、格式错误和版本查询行为，并让既有进程测试提供合法 `.env`。
- 修改 `packaging/start.sh`：删除配置默认值和导出，仅保留启动职责。
- 修改 `tests/release_scripts.test.mjs`：验证启动器不注入配置、保持当前目录和外部环境，并适配真实二进制发行测试。
- 修改 `tools/verify-release.sh`：在临时发行副本内创建运行所需 `.env`，不污染或扩大发行文件白名单。
- 修改 `packaging/.env.example`：提供可直接复制的无秘密发行配置模板。
- 修改 `packaging/README.txt`：改为中文，完整说明配置、运行、安全、升级与排错。
- 修改 `README.md`：同步源码运行、环境变量和发行运行行为。

### 任务 1：Rust 强制加载 `.env`

**文件：**
- 修改：`backend/Cargo.toml`
- 修改：`Cargo.lock`
- 修改：`backend/src/main.rs:1-14`
- 测试：`backend/tests/server_process.rs`

- [ ] **步骤 1：为既有进程测试建立显式 `.env` fixture**

在 `backend/tests/server_process.rs` 增加只供测试使用的辅助函数；它创建 `.env`，将命令工作目录固定到该临时目录，并移除会影响断言的应用环境变量：

```rust
fn command_with_dotenv(directory: &std::path::Path, contents: &str) -> Command {
    std::fs::write(directory.join(".env"), contents).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"));
    command
        .current_dir(directory)
        .env_remove("DATABASE_URL")
        .env_remove("KAFKA_DASHBOARD_BIND")
        .env_remove("KAFKA_DASHBOARD_WEB_DIR")
        .env_remove("RUST_LOG");
    command
}
```

把现有四个正常启动二进制的测试改为通过该辅助函数构造 `Command`，再用各自原有的 `.env(...)` 显式覆盖测试所需变量。测试临时目录的生命周期必须覆盖子进程运行。

- [ ] **步骤 2：编写缺少 `.env` 和 `--version` 的进程测试**

先点名要捕获的破坏：若正常启动未调用 `.env` 加载，缺失文件测试会错误地继续初始化服务；若版本分支移到加载之后，版本测试会错误失败。

```rust
#[test]
fn startup_requires_dotenv_in_current_directory() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .current_dir(directory.path())
        .env_remove("DATABASE_URL")
        .env_remove("KAFKA_DASHBOARD_BIND")
        .env_remove("KAFKA_DASHBOARD_WEB_DIR")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("required .env"), "{stderr}");
}

#[test]
fn version_does_not_require_dotenv() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .current_dir(directory.path())
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    assert!(String::from_utf8_lossy(&output.stdout).starts_with("kafka-dashboard "));
}
```

- [ ] **步骤 3：运行新增测试验证红灯**

运行：

```bash
cargo test -p backend --test server_process startup_requires_dotenv_in_current_directory -- --exact
cargo test -p backend --test server_process version_does_not_require_dotenv -- --exact
```

预期：缺失 `.env` 测试 FAIL，因为当前程序会继续到数据库/监听逻辑；版本测试 PASS，记录需要保留的既有契约。

- [ ] **步骤 4：编写从 `.env` 启动和外部环境覆盖文件的测试**

先点名要捕获的破坏：若 Rust 不读文件，第一个测试无法绑定文件指定的端口；若错误使用 override API，第二个测试会采用文件中的无效数据库地址而退出。

复用现有健康检查和 SIGTERM 逻辑，将 `configured_listener_serves_health_and_sigterm_exits_successfully` 的配置改为只写入 `.env`：

```rust
let dotenv = format!("KAFKA_DASHBOARD_BIND={address}\nDATABASE_URL=sqlite::memory:\n");
let mut command = command_with_dotenv(directory.path(), &dotenv);
let mut server = Server(command.stdout(Stdio::null()).stderr(Stdio::null()).spawn().unwrap());
```

新增外部优先测试，其中 `.env` 写入必然失败的 `DATABASE_URL=invalid://from-dotenv`，但命令用 `.env("DATABASE_URL", "sqlite::memory:")` 和外部监听地址启动；通过真实健康请求证明外部变量获胜，最后发送 SIGTERM 并等待成功退出。

- [ ] **步骤 5：编写格式错误且不泄密的测试**

先点名要捕获的破坏：若入口直接向用户传播 dotenvy 原始解析错误，测试秘密可能出现在 stderr。

```rust
#[test]
fn malformed_dotenv_exits_without_echoing_values() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join(".env"), "DATABASE_URL='dotenv-secret\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_kafka-dashboard"))
        .current_dir(directory.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("required .env"), "{stderr}");
    assert!(!stderr.contains("dotenv-secret"), "{stderr}");
}
```

- [ ] **步骤 6：运行加载相关测试验证红灯**

运行：

```bash
cargo test -p backend --test server_process configured_listener_serves_health_and_sigterm_exits_successfully -- --exact
cargo test -p backend --test server_process external_environment_overrides_dotenv -- --exact
cargo test -p backend --test server_process malformed_dotenv_exits_without_echoing_values -- --exact
```

预期：至少文件加载和格式错误安全消息测试 FAIL，失败原因是尚未加载 `.env`；不得接受由 fixture 拼写或端口竞争导致的错误。

- [ ] **步骤 7：添加依赖并实现最小加载逻辑**

在 `backend/Cargo.toml` 的 `[dependencies]` 中加入与当前 crates.io 稳定版匹配的锁定范围：

```toml
dotenvy = "0.15"
```

在 `backend/src/main.rs` 的 `--version` 提前返回之后、日志初始化之前加入精确当前目录加载。不要使用会向父目录搜索的 `dotenvy::dotenv()`：

```rust
dotenvy::from_path(".env")
    .map_err(|_| std::io::Error::other("failed to load required .env configuration"))?;
tracing_subscriber::fmt::init();
```

运行一次不修改源码的 Cargo 命令生成或更新 `Cargo.lock`。

- [ ] **步骤 8：运行 Rust 进程测试验证绿灯**

运行：

```bash
cargo test -p backend --test server_process
```

预期：全部 PASS；输出不包含测试秘密或非预期警告。

- [ ] **步骤 9：提交 Rust 行为变更**

```bash
git add backend/Cargo.toml Cargo.lock backend/src/main.rs backend/tests/server_process.rs
git commit -m "feat: 启动时强制加载 env 配置"
```

### 任务 2：发行启动器不再注入配置

**文件：**
- 修改：`packaging/start.sh:10-17`
- 修改：`tests/release_scripts.test.mjs:12-105`
- 修改：`tools/verify-release.sh:73-135`

- [ ] **步骤 1：修改启动器测试 fixture 以观察真实边界行为**

先点名要捕获的破坏：如果 `start.sh` 仍写入默认配置，假二进制打印出的环境变量不会为空；如果启动器未切换到发行目录，打印出的 `pwd` 会指向测试临时根目录。

让假二进制依次打印平台、当前目录、`DATABASE_URL`、`KAFKA_DASHBOARD_WEB_DIR` 和参数：

```javascript
await executable(
  path.join(release, 'bin', `kafka-dashboard-${platform}`),
  `#!/bin/sh\nprintf '%s\\n' '${platform}' "$PWD" "${DATABASE_URL-}" "${KAFKA_DASHBOARD_WEB_DIR-}" "$@"\n` +
    'exit "${FAKE_EXIT:-0}"\n',
);
```

更新两个平台选择测试的字面量期望：平台正确、第二行为发行目录、接下来两个环境变量为空、参数边界不变。将“无 env 文件即可启动”测试改名为“启动器不要求或读取 env 文件”，只验证假二进制被执行和 `data/` 被创建。移动目录测试断言打印出的当前目录等于移动后的目录，不再期待自动生成 web 路径。

- [ ] **步骤 2：运行启动器测试验证红灯**

运行：

```bash
node --test --test-name-pattern='selects its binary|launcher does not|moved package|explicit database' tests/release_scripts.test.mjs
```

预期：不注入配置的断言 FAIL，实际输出仍包含 `sqlite://data/kafka-dashboard.db?mode=rwc` 和发行目录下的 `web` 路径。

- [ ] **步骤 3：删除启动器配置赋值和导出**

将 `packaging/start.sh` 收敛为：

```sh
cd "$release_dir"
umask 077
mkdir -p "$release_dir/data"
exec "$release_dir/bin/kafka-dashboard-$platform" "$@"
```

保留现有平台判断和 `set -eu`，删除 SQLite URI 注释、两个默认赋值及 `export`。

- [ ] **步骤 4：运行启动器测试验证绿灯**

运行：

```bash
node --test --test-name-pattern='selects its binary|launcher does not|moved package|explicit database' tests/release_scripts.test.mjs
```

预期：全部匹配测试 PASS。显式外部变量测试仍证明启动器原样保留调用者提供的值。

- [ ] **步骤 5：让真实发行运行测试提供 `.env`**

在 `real binary starts after relocation` 测试移动发行目录之前，写入无秘密 `.env`：

```javascript
await writeFile(path.join(f.release, '.env'), [
  'DATABASE_URL=sqlite://data/kafka-dashboard.db?mode=rwc',
  'KAFKA_DASHBOARD_WEB_DIR=web',
  '',
].join('\n'));
```

保留测试进程外部注入的临时监听地址和日志级别，从而同时覆盖“外部环境优先”。`.env` 只存在于测试临时目录，不进入构建产物。

- [ ] **步骤 6：让双平台验证的临时副本创建 `.env`**

在 `tools/verify-release.sh` 完成两个临时发行目录复制后加入：

```sh
cp "$mac_release/.env.example" "$mac_release/.env"
cp "$linux_release/.env.example" "$linux_release/.env"
```

`.env.example` 将在任务 3 提供发行目录可用的数据库和 web 相对路径。验证仍先对原始发行目录执行严格白名单与哈希检查，因此真实 `.env` 不会被打包或绕过产物检查。

- [ ] **步骤 7：运行全部发行脚本测试**

运行：

```bash
node --test tests/release_scripts.test.mjs
```

预期：全部 PASS；只有依赖现有 `dist` 原生二进制的测试可按其既有条件 SKIP。

- [ ] **步骤 8：提交发行运行行为变更**

```bash
git add packaging/start.sh tests/release_scripts.test.mjs tools/verify-release.sh
git commit -m "refactor: 由 Rust 统一加载发行配置"
```

### 任务 3：配置模板和中文发行说明

**文件：**
- 修改：`packaging/.env.example`
- 修改：`packaging/README.txt`
- 修改：`README.md:20-60,132-155`

- [ ] **步骤 1：将 `.env.example` 改为可直接复制的发行配置**

使用无秘密值并明确优先级：

```dotenv
# 复制本文件为 .env 后再运行 ./start.sh。
# 父进程已经设置的同名环境变量优先于本文件。
KAFKA_DASHBOARD_BIND=127.0.0.1:3000
DATABASE_URL=sqlite://data/kafka-dashboard.db?mode=rwc
KAFKA_DASHBOARD_WEB_DIR=web
RUST_LOG=info
```

相对路径依赖 `start.sh` 将工作目录切换到发行目录；模板不得包含密码、PEM、真实 broker 地址或其他秘密。

- [ ] **步骤 2：将发行 `README.txt` 完整改为中文**

文档按以下顺序给出可执行说明：

1. 支持 macOS Apple Silicon 与 Linux x86-64，以及发行包无需 Node.js/Rust 的说明。
2. 校验 `SHA256SUMS`。
3. 执行 `cp .env.example .env`，逐项解释监听地址、SQLite URL、web 目录和日志级别。
4. 执行 `./start.sh`、访问 `http://127.0.0.1:3000`、访问 `/api/health`、使用 Ctrl-C/SIGTERM 停止。
5. 说明缺少/不可读/格式错误 `.env` 会启动失败，外部同名环境变量优先，`./start.sh --version` 例外。
6. 说明 SQLite 与 `.env` 权限、明文 Kafka 凭据、备份、只读 Kafka 权限、反向代理和 HTTPS。
7. 说明旧加密数据库必须停服后手动删除数据库及 sidecar，再重新配置。
8. 列出数据库不可写、端口占用、页面不可用、Kafka/TLS 连接失败等排查入口，禁止用户提交秘密诊断材料。

不要对人类文档编写脆弱的文本匹配测试；通过人工审阅内容和构建测试确认文件被纳入发行包。

- [ ] **步骤 3：同步仓库 README**

把源码运行示例改为先在仓库根目录创建 `.env`，再执行 Cargo：

```bash
cp packaging/.env.example .env
cargo run -p backend --bin kafka-dashboard
```

说明源码运行若直接复制发行模板，应按需要移除或调整 `KAFKA_DASHBOARD_WEB_DIR=web`，因为源码开发通常由 Vite 提供页面。更新环境变量章节为“Rust 正常启动时强制读取当前工作目录 `.env`，父进程变量优先”；更新发行树注释和运行步骤，删除所有“不自动加载”及“无需配置即可启动”的旧描述。

- [ ] **步骤 4：检查文档和模板安全性与一致性**

运行：

```bash
rg -n 'never reads|不会自动读取|无需必填环境配置|optional configuration examples only|Run without any required' README.md packaging
rg -n -- '-----BEGIN .*PRIVATE KEY-----|AKIA[0-9A-Z]{16}|password\s*=|secret\s*=' packaging/.env.example packaging/README.txt README.md
git diff --check
```

预期：第一条没有匹配；第二条没有秘密材料匹配；`git diff --check` 退出 0。

- [ ] **步骤 5：运行构建装配回归测试**

运行：

```bash
node --test --test-name-pattern='build assembles' tests/release_scripts.test.mjs
```

预期：PASS，产物仍只包含 `.env.example` 而不包含 `.env`，且中文 `README.txt` 被纳入校验和与归档。

- [ ] **步骤 6：提交文档和模板**

```bash
git add packaging/.env.example packaging/README.txt README.md
git commit -m "docs: 补充中文发行配置与运行说明"
```

### 任务 4：全量相关验证与最终自审

**文件：**
- 检查：本计划涉及的全部文件

- [ ] **步骤 1：运行 Rust 格式、静态检查和测试**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

预期：三个命令均退出 0，无 warning 或失败测试。

- [ ] **步骤 2：运行前端与发行脚本回归**

```bash
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test scripts/e2e-preview.test.mjs
node --test tests/release_scripts.test.mjs
```

预期：全部退出 0；如果前端依赖尚未安装，应按锁文件运行 `npm --prefix frontend ci` 后重新执行，而不是把未运行报告为通过。

- [ ] **步骤 3：检查差异、秘密和工作区状态**

```bash
git diff --check
git diff --stat HEAD~3..HEAD
git status --short
rg -n --hidden --glob '!target/**' --glob '!frontend/node_modules/**' --glob '!Cargo.lock' -- '-----BEGIN .*PRIVATE KEY-----|AKIA[0-9A-Z]{16}' .
```

预期：差异检查退出 0；变更只覆盖计划文件；秘密扫描不出现新增真实秘密；工作区没有遗漏的交付变更。

- [ ] **步骤 4：按环境能力执行发行验证**

若当前机器已有完整 Apple Silicon macOS + Docker 构建条件，运行：

```bash
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
```

预期：双平台构建和验证退出 0。若下载依赖、Docker 或平台条件不可用，记录未运行或失败的具体限制，不将其报告为通过，也不提交 `dist/`。

- [ ] **步骤 5：最终需求核对**

逐项确认：正常启动强制当前目录 `.env`、格式错误安全失败、外部环境优先、`--version` 例外、启动器不设置配置、中文发行 README 含配置和运行说明、真实 `.env` 不进入发行包。检查提交范围，禁止自动推送。
