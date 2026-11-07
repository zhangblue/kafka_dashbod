# 开发验证与端到端测试

## 代码与脚本检查

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test frontend/scripts/i18n-audit.test.mjs
node --test scripts/e2e-preview.test.mjs
node --test tests/release_scripts.test.mjs
git diff --check
```

## 浏览器端到端测试

执行下面的测试需要 Docker 和支持 `up --wait` 的 Compose v2。脚本用它们启动隔离的 Kafka，并在测试后清理容器。使用发行包连接已有 Kafka 时，无需为此安装 Docker。

```bash
npm --prefix frontend exec -- playwright install chromium
npm --prefix frontend run test:e2e
```

E2E 一键脚本使用独立 Compose project 和动态本地端口，启动 Apache Kafka 3.9.1 KRaft；Rust fixture 只向本次临时容器创建 `dashboard-e2e-orders` 的两个分区、在分区 0 写入 10 条消息，并为 `dashboard-e2e-reader` 提交位置 4 / 0，预期 Lag 为 6 / 0。fixture 必须同时收到精确的 `127.0.0.1:<非零端口>` 和 `E2E_KAFKA_FIXTURE_PERMIT=disposable-compose`，由脚本设置；不要自行设置许可并指向已有 Kafka。真实管理验收使用随机唯一的 `dashboard-e2e-managed-*` Topic，覆盖创建、读取保留时间和来源、设置覆盖、重新打开回显、删除覆盖恢复默认、删除及列表消失。Rust 额外验证无关配置不变和断言失败时清理 guard 生效；清理只针对本次成功创建的管理 Topic，绝不删除 seed Topic。

脚本自动使用临时 SQLite、后端和前端进程。Vite 直接绑定端口 0，脚本从子进程实际监听地址获取端口，并核验 PID 存活及本次运行标识，防止误用其他健康服务。正常结束、失败或收到 INT / TERM 均清理进程、容器、卷和临时文件。普通 `cargo test` 默认跳过需要 broker 的测试；单独强行运行 ignored fixture 而没有一次性环境时，安全前置检查失败是预期结果，不代表真实验收通过。此环境未启用 ACL，真实授权拒绝须在专用授权环境另验，普通 API 测试覆盖稳定权限错误映射。

浏览器验收覆盖集群 CRUD、测试连接、真实水位/Lag、规则保存与自定义标记、刷新频率、切换和离开时释放会话；飞书验收覆盖卡片入口、眼睛图标、真实保存、掩码、Group/Topic 下拉、阈值最小 0、持续时间最小 10 秒、中文测试反馈、切集群草稿隔离、清除 Token 和窄屏滚动。通知测试仅拦截该 POST 的响应，规则保存仍走真实后端且设置极高阈值，不调用真实飞书。另验证 API / 运行日志不泄漏测试秘密、超大请求、历史淘汰和隐藏时无轮询。历史测试只加速响应中的采集时间，指标仍来自 Kafka；headless 环境通过浏览器 visibility 事件模拟隐藏。Rust 测试使用本地假 sender 验证后台告警，额外覆盖采集互斥、删除释放和 SIGTERM。Playwright 关闭 trace / screenshot / video，避免持久化请求中的敏感字段。

仅需手动启动空白本地测试 broker 时：

```bash
docker compose -f docker-compose.test.yml -p kafka-lens-manual up --wait
# 默认入口 127.0.0.1:19092；冲突时在启动前设置 KAFKA_TEST_PORT
docker compose -f docker-compose.test.yml -p kafka-lens-manual down --volumes --remove-orphans
```

## E2E 无法启动

确认 Docker 正在运行、Compose v2 支持 `up --wait`、Playwright Chromium 已安装，且测试进程可以监听本地临时端口。

[返回项目首页](../../README.md)
