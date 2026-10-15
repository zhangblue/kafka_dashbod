# Kafka Lens 后台启动器实现计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development（推荐）或 superpowers:executing-plans 逐任务实现此计划。步骤使用复选框（`- [ ]`）语法来跟踪进度。

**目标：** 将发行包 `start.sh` 改为安全的后台进程管理入口，提供带明确中文结果的 `start`、`status`、`stop`、`restart` 与前台 `--version`，并让发行验证和中文文档与该生命周期一致。

**架构：** 启动器保持 POSIX `sh` 单文件实现，在发行目录的私有 `data/` 下管理日志、PID 与原子目录锁；所有信号操作前都读取并验证 PID 和进程命令中的发行二进制绝对路径。Node 发行脚本测试使用临时目录中的可控长驻假进程覆盖状态机，双平台验证则改为通过 PID 文件和启动器命令管理真实服务。

**技术栈：** POSIX `sh`、`nohup`、`ps`、Node.js `node:test`、Rust/Axum 发行二进制、Docker/BusyBox 发行验证。

---

## 文件结构

- 修改 `packaging/start.sh`：实现后台生命周期、原子 PID/锁、日志重定向、进程身份校验和中文命令结果。
- 修改 `tests/release_scripts.test.mjs`：构造可控长驻进程，回归启动、状态、停止、重启、异常 PID、身份不匹配、超时和并发锁。
- 修改 `tools/verify-release.sh`：真实 macOS 和 Linux 验证改用后台 PID 与 `start.sh stop`，失败清理仍能强制回收测试进程。
- 修改 `packaging/README.txt`：面向离线包用户说明中文命令、配置、日志、PID、故障处理和日志轮转边界。
- 修改 `README.md`：同步仓库级发行运行和验证说明。
- 修改 `AGENTS.md`：记录后台启动器约束，防止后续维护重新假设前台运行。

### 任务 1：用 TDD 建立后台启动与明确状态查询

**文件：**
- 修改：`tests/release_scripts.test.mjs:1-126`
- 修改：`packaging/start.sh:1-13`

- [ ] **步骤 1：把启动器夹具改为可控长驻进程，并编写后台启动失败测试**

在测试夹具中让假二进制记录启动信息后长驻，收到 TERM 正常退出；为每个已启动 PID 注册强制清理，避免失败测试遗留进程。增加默认命令与显式 `start` 的断言：调用快速返回、PID 文件是正整数、进程仍存活、日志包含平台和含空格的发行工作目录；再增加立即退出、重复启动和日志追加测试。

```js
const pidFile = path.join(release, 'data/kafka-dashboard.pid');
const logFile = path.join(release, 'data/kafka-dashboard.log');

async function readPid(release) {
  return Number((await readFile(path.join(release, 'data/kafka-dashboard.pid'), 'utf8')).trim());
}

await executable(binary, `#!/bin/sh
printf '%s\\n' '${platform}' "$PWD" "\${DATABASE_URL-}" "\${KAFKA_DASHBOARD_WEB_DIR-}" "$@"
[ "\${FAKE_EXIT-}" = immediate ] && exit 17
trap 'exit 0' TERM
while :; do sleep 1; done
`);

test('default command starts in background and writes pid and append-only log', async t => {
  const f = await fixture(t);
  const first = f.run();
  assert.equal(first.status, 0, first.stderr);
  assert.match(first.stdout, /Kafka Lens 已在后台启动，PID: [1-9][0-9]*/);
  const pid = await readPid(f.release);
  process.kill(pid, 0);
  const log = await readFile(path.join(f.release, 'data/kafka-dashboard.log'), 'utf8');
  assert.match(log, /darwin-arm64/);
  assert.match(log, new RegExp(f.release.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
});

test('immediate startup failure removes pid and points to the log', async t => {
  const f = await fixture(t);
  const result = f.run([], { FAKE_EXIT: 'immediate' });
  assert.notEqual(result.status, 0);
  assert.equal(existsSync(path.join(f.release, 'data/kafka-dashboard.pid')), false);
  assert.match(result.stderr, /日志/);
});
```

- [ ] **步骤 2：运行新启动测试并确认按预期失败**

运行：`node --test --test-name-pattern='background|startup|duplicate|append-only|working directory' tests/release_scripts.test.mjs`

预期：FAIL；现有 `exec` 会阻塞或缺少 `data/kafka-dashboard.pid`，证明失败来自尚未实现后台生命周期。

- [ ] **步骤 3：增加 `status`、PID 错误、锁和 `--version` 接口测试**

覆盖以下具体结果：无 PID 时输出 `Kafka Lens 未运行` 且退出 0；匹配进程时输出 `Kafka Lens 正在运行，PID: <pid>` 且退出 0；已退出的陈旧 PID 被清理并报告未运行；非法 PID、PID 路径不可读、活 PID 身份不匹配和无法获取锁均明确报错且非零；`--version` 不创建 `data/`、PID 或日志；未知参数显示中文用法并非零。

```js
test('status describes both running and stopped states with exit code zero', async t => {
  const f = await fixture(t);
  const stopped = f.run(['status']);
  assert.equal(stopped.status, 0, stopped.stderr);
  assert.match(stopped.stdout, /^Kafka Lens 未运行/m);
  assert.equal(f.run(['start']).status, 0);
  const pid = await readPid(f.release);
  const running = f.run(['status']);
  assert.equal(running.status, 0, running.stderr);
  assert.match(running.stdout, new RegExp(`Kafka Lens 正在运行，PID: ${pid}`));
});

test('invalid pid is an explicit script error', async t => {
  const f = await fixture(t);
  await mkdir(path.join(f.release, 'data'));
  await writeFile(path.join(f.release, 'data/kafka-dashboard.pid'), 'not-a-pid\n');
  const result = f.run(['status']);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /PID 文件内容无效/);
});
```

- [ ] **步骤 4：运行状态接口测试并确认按预期失败**

运行：`node --test --test-name-pattern='status|pid|lock|version|unknown argument' tests/release_scripts.test.mjs`

预期：FAIL；现有脚本把命令透传给二进制，不会输出中文状态，也没有 PID/锁校验。

- [ ] **步骤 5：实现最小后台启动和状态机**

在 `packaging/start.sh` 中保留平台映射和 `umask 077`，先特判 `--version` 为前台 `exec`，其余命令只接受空参数、`start`、`status`、`stop`、`restart`。使用固定路径和函数边界：

```sh
binary="$release_dir/bin/kafka-dashboard-$platform"
data_dir="$release_dir/data"
pid_file="$data_dir/kafka-dashboard.pid"
log_file="$data_dir/kafka-dashboard.log"
lock_dir="$data_dir/kafka-dashboard.lock"

usage() { echo '用法: ./start.sh [start|stop|status|restart|--version]' >&2; }
fail() { echo "$*" >&2; exit 1; }
release_lock() { [ ! -d "$lock_dir" ] || rmdir "$lock_dir" 2>/dev/null || :; }
acquire_lock() {
    mkdir "$lock_dir" 2>/dev/null || fail '无法执行操作：另一个启动器管理操作正在进行'
    trap release_lock EXIT
    trap 'release_lock; exit 129' HUP
    trap 'release_lock; exit 130' INT
    trap 'release_lock; exit 143' TERM
}
write_pid() {
    pid_tmp="$pid_file.$$"
    printf '%s\n' "$1" > "$pid_tmp"
    mv "$pid_tmp" "$pid_file"
}
```

实现 `inspect_process` 三态契约：返回 0 表示 PID 存活且命令包含 `$binary` 的绝对路径；返回 1 表示无 PID 或进程已退出（并清理陈旧 PID）；返回 2 表示 PID 文件读取/格式错误、检查失败或活进程身份不匹配（不删除 PID）。调用处必须放在 `if inspect_process; then ... else state=$?; ... fi` 中，避免 `set -e` 提前退出。

`start_service` 在确认未运行后执行：

```sh
nohup "$binary" "$@" </dev/null >>"$log_file" 2>&1 &
service_pid=$!
write_pid "$service_pid"
sleep 1
if ! kill -0 "$service_pid" 2>/dev/null; then
    rm -f "$pid_file"
    fail "Kafka Lens 启动失败，请查看日志: $log_file"
fi
echo "Kafka Lens 已在后台启动，PID: $service_pid"
echo "日志文件: $log_file"
```

`status_service` 对返回 0 输出运行中，对返回 1 输出未运行，并让这两种业务状态都返回 0；仅三态中的 2 传播非零。进程命令检查使用 macOS、GNU procps 和 BusyBox 均支持的 `ps -o pid= -o args=` 全表输出，再用 `awk -v target="$pid" '$1 == target'` 精确选 PID，并以 shell `case` 对 `$binary` 做字面子串匹配；命令不可读或活 PID 不匹配时返回脚本错误。Linux 容器验收必须实际覆盖该实现，不能仅靠 macOS 单元测试推断兼容性。

- [ ] **步骤 6：运行任务 1 测试并确认通过**

运行：`node --test tests/release_scripts.test.mjs`

预期：所有启动器与既有构建/安全测试 PASS；真实发行二进制相关测试仍只在产物存在时运行。

- [ ] **步骤 7：提交任务 1**

```bash
git add packaging/start.sh tests/release_scripts.test.mjs
git commit -m "feat: 后台启动并报告服务状态"
```

### 任务 2：用 TDD 完成安全停止与重启

**文件：**
- 修改：`tests/release_scripts.test.mjs:15-126`
- 修改：`packaging/start.sh`

- [ ] **步骤 1：编写停止、身份保护、超时和重启顺序的失败测试**

扩展假二进制支持通过文件记录 `START:<pid>` 与 `TERM:<pid>`，以及 `FAKE_IGNORE_TERM=1` 时忽略 TERM。覆盖：正常停止等待退出、删除 PID 并输出 `Kafka Lens 已停止`；未运行停止输出明确文本且退出 0；身份不匹配时不发送信号、不删除 PID；超时时不发送 KILL、保留 PID 并非零；`restart` 的记录严格为旧 PID TERM 后新 PID START；停止失败时没有第二次 START。

```js
test('stop terminates only the matching service and removes its pid file', async t => {
  const f = await fixture(t);
  assert.equal(f.run(['start']).status, 0);
  const pid = await readPid(f.release);
  const stopped = f.run(['stop']);
  assert.equal(stopped.status, 0, stopped.stderr);
  assert.match(stopped.stdout, /Kafka Lens 已停止/);
  assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
  assert.equal(existsSync(path.join(f.release, 'data/kafka-dashboard.pid')), false);
});

test('identity mismatch never signals or deletes an unrelated live pid', async t => {
  const f = await fixture(t);
  await mkdir(path.join(f.release, 'data'));
  await writeFile(path.join(f.release, 'data/kafka-dashboard.pid'), `${process.pid}\n`);
  const result = f.run(['stop']);
  assert.notEqual(result.status, 0);
  process.kill(process.pid, 0);
  assert.equal(existsSync(path.join(f.release, 'data/kafka-dashboard.pid')), true);
});
```

- [ ] **步骤 2：运行停止与重启测试并确认按预期失败**

运行：`node --test --test-name-pattern='stop|restart|identity|timeout' tests/release_scripts.test.mjs`

预期：FAIL；任务 1 尚未实现停止等待、超时和重启顺序。

- [ ] **步骤 3：实现有界安全停止和串行重启**

实现 `stop_service`：先调用 `inspect_process`；未运行时输出 `Kafka Lens 未运行` 并返回 0；匹配时只发送一次 `kill -TERM "$service_pid"`，最多循环 100 次、每次 `sleep 0.1`；退出后删除 PID 并输出成功。超时保持 PID，不发送 KILL：

```sh
kill -TERM "$service_pid" || fail "无法停止 Kafka Lens，PID: $service_pid"
attempt=0
while kill -0 "$service_pid" 2>/dev/null; do
    attempt=$((attempt + 1))
    [ "$attempt" -lt 100 ] || fail "Kafka Lens 在等待时间内未退出，PID 文件已保留: $pid_file"
    sleep 0.1
done
rm -f "$pid_file"
echo 'Kafka Lens 已停止'
```

`restart` 在同一把锁中先调用 `stop_service`，只有返回 0 才调用 `start_service`；不得嵌套获取锁。身份不匹配与检查错误沿用任务 1 的非零路径，确保不会误杀或覆盖 PID。

- [ ] **步骤 4：运行完整发行脚本测试并确认通过**

运行：`node --test tests/release_scripts.test.mjs`

预期：PASS，且测试清理钩子检查没有遗留长驻假进程。

- [ ] **步骤 5：执行 shell 静态边界检查**

运行：`sh -n packaging/start.sh && git diff --check`

预期：两条检查均退出 0，无 shell 语法错误或空白错误。

- [ ] **步骤 6：提交任务 2**

```bash
git add packaging/start.sh tests/release_scripts.test.mjs
git commit -m "feat: 安全停止并重启后台服务"
```

### 任务 3：让真实双平台发行验证使用后台生命周期

**文件：**
- 修改：`tools/verify-release.sh:27-148`
- 修改：`tests/release_scripts.test.mjs:196-222`

- [ ] **步骤 1：先修改验证回归测试，要求通过 PID 文件清理后台服务**

将“健康检查失败仍回收忽略 TERM 的服务”夹具改为让假二进制把自身 PID 写入测试目录外部的 `SERVER_PID_FILE`；`verify-release.sh` 内部仍须从临时发行副本的 `data/kafka-dashboard.pid` 管理它。脚本失败并删除内部临时目录后，通过外部记录的 PID 断言进程已不存在。另在假 Docker 命令中记录 Linux 验证确实执行了容器内 `start.sh start/status/stop`，而不是把后台启动器当作容器 PID 1。

```js
const serverPidFile = path.join(f.temp, 'server.pid');
const result = spawnSync('/bin/sh', [path.join(root, 'tools/verify-release.sh'), release], {
  encoding: 'utf8', timeout: 30000,
  env: {
    ...process.env,
    PATH: `${f.temp}/commands:${process.env.PATH}`,
    RUST_LOG: 'error',
    SERVER_PID_FILE: serverPidFile,
  },
});
assert.notEqual(result.status, 0);
const pid = Number(await readFile(serverPidFile, 'utf8'));
assert.throws(() => process.kill(pid, 0), { code: 'ESRCH' });
```

- [ ] **步骤 2：运行验证回归并确认按预期失败**

运行：`node --test --test-name-pattern='runtime verification|Linux lifecycle' tests/release_scripts.test.mjs`

预期：FAIL；当前验证脚本仍把 `$!` 记录为服务 PID，并把 `start.sh` 直接当作 Docker 容器主进程。

- [ ] **步骤 3：改造 macOS 真实运行验证**

执行 `"$mac_release/start.sh" start` 后，从 `$mac_release/data/kafka-dashboard.pid` 读取 `mac_pid`，从 `$mac_release/data/kafka-dashboard.log` 解析随机端口。健康与静态资源检查通过后执行 `"$mac_release/start.sh" stop`，再断言 PID 文件消失且原 PID 不存活。

验证脚本自身的 `cleanup()` 必须独立于产品的“禁止自动 SIGKILL”政策：若测试失败，先尝试 `start.sh stop`，再对仍存活的测试 PID 发送 KILL，确保 CI 不泄漏进程。

```sh
NO_COLOR=1 RUST_LOG=info KAFKA_DASHBOARD_BIND=127.0.0.1:0 "$mac_release/start.sh" start
mac_pid=$(cat "$mac_release/data/kafka-dashboard.pid")
mac_log="$mac_release/data/kafka-dashboard.log"
# 健康检查完成后：
"$mac_release/start.sh" stop
kill -0 "$mac_pid" 2>/dev/null && fail 'macOS process survived start.sh stop'
mac_pid=
```

- [ ] **步骤 4：改造 Linux/amd64 容器验证**

用一个长驻 BusyBox shell 保持测试容器存活，通过 `docker exec` 在同一容器内依次执行发行目录的 `start.sh start`、`status` 和 `stop`。端口映射、HTTP/SQLite/静态资源检查保持不变；停止后检查明确中文状态与容器内 PID 已退出，最后删除容器。

```sh
container=$(docker run -d --platform linux/amd64 \
    -v "$linux_release:$linux_mount" -p 127.0.0.1::3000 \
    -e KAFKA_DASHBOARD_BIND=0.0.0.0:3000 \
    "$runtime_image" sh -c 'while :; do sleep 3600; done')
docker exec "$container" "$linux_mount/start.sh" start
docker exec "$container" "$linux_mount/start.sh" status | grep -q 'Kafka Lens 正在运行，PID:'
# HTTP 检查完成后：
docker exec "$container" "$linux_mount/start.sh" stop
docker exec "$container" "$linux_mount/start.sh" status | grep -q 'Kafka Lens 未运行'
```

- [ ] **步骤 5：运行发行脚本测试并确认通过**

运行：`node --test tests/release_scripts.test.mjs`

预期：PASS（没有本机真实 `dist` 时，4 个真实二进制搬移用例仍按既有条件跳过）。

- [ ] **步骤 6：提交任务 3**

```bash
git add tools/verify-release.sh tests/release_scripts.test.mjs
git commit -m "test: 验证发行包后台生命周期"
```

### 任务 4：同步中文说明并完成全量验证

**文件：**
- 修改：`packaging/README.txt:36-51`
- 修改：`README.md:130-160`
- 修改：`AGENTS.md:38-60`

- [ ] **步骤 1：更新离线包中文运行说明**

将“按 Ctrl-C 停止”替换为可复制执行的命令，并明确业务状态靠文本表达，运行中与未运行的 `status` 都成功退出：

```text
./start.sh                 后台启动（等同于 ./start.sh start）
./start.sh status          输出“正在运行，PID: …”或“未运行”
./start.sh stop            发送 SIGTERM 并等待正常退出
./start.sh restart         正常停止后重新启动
./start.sh --version       前台显示版本，不要求 .env

PID：data/kafka-dashboard.pid
日志：data/kafka-dashboard.log（追加写入，不自动轮转）
```

说明重复启动会被拒绝、启动失败先查看日志、停止超时不会强制杀死进程、生产部署应配置系统日志轮转或服务管理器；保留 `.env` 配置方法和明文凭据安全警告。

- [ ] **步骤 2：同步仓库 README 与维护约定**

更新根 README 的发行树和运行段落：运行文件会在首次启动后出现在 `data/`，但不会进入发行归档；验证流程通过 `start/status/stop` 管理真实程序。更新 `AGENTS.md`，写明 `packaging/start.sh` 必须后台运行、使用固定 PID/日志文件、安全身份检查、`status` 明确文本且运行/停止均退出 0、停止不得自动 SIGKILL。

- [ ] **步骤 3：运行格式和核心回归**

运行：

```bash
sh -n packaging/start.sh tools/verify-release.sh
node --test tests/release_scripts.test.mjs
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test scripts/e2e-preview.test.mjs
git diff --check
```

预期：全部退出 0；Rust 测试仅保留既有、明确标记需要一次性 Kafka 的 ignored 用例。

- [ ] **步骤 4：运行浏览器和双平台发行验收**

运行：

```bash
npm --prefix frontend run test:e2e
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
```

预期：E2E 通过；Apple Silicon 和 Linux/amd64 发行均完成版本、后台启动、明确状态、SQLite、健康检查、页面资源、SPA fallback 与正常停止验证。若 Docker、浏览器或网络前置条件不可用，记录原始限制，不得声称该项通过。

- [ ] **步骤 5：自审秘密与生成物边界**

运行：

```bash
git status --short
git diff --stat
git diff --check
git grep -n -E -- '-----BEGIN .*PRIVATE KEY-----|AKIA[0-9A-Z]{16}' -- . ':!docs/ui/approved-dashboard-demo.html'
```

确认未暂存或提交 `dist/`、`frontend/dist/`、`target/`、`node_modules/`、`.env`、数据库、日志、PID、锁目录、真实证书或凭据；只保留源码、测试和中文文档变更。

- [ ] **步骤 6：提交任务 4**

```bash
git add README.md AGENTS.md packaging/README.txt
git commit -m "docs: 说明后台启动与管理方式"
```

- [ ] **步骤 7：最终审查并合并**

使用 `superpowers:requesting-code-review` 对规格覆盖、安全信号边界、测试清理和文档一致性做最终审查；修复后重跑受影响验证。确认分支相对 `main` 只有本计划内提交，再使用 `superpowers:finishing-a-development-branch` 按用户选择合并到 `main`；未经新授权不推送远端。
