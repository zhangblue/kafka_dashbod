# Kafka Lens

Kafka 只读监控面板：管理集群连接配置，查看 Topic / Partition 水位、Consumer Group Lag、最近一小时趋势，以及集群默认和 Group / Topic 自定义 Lag 规则。后端为 Rust / Axum，前端为 React / Vite，SQLite 只保存配置与加密凭据。常规应用不会创建 Topic、生产消息或提交消费位置。

## 系统要求

- 支持 Rust 2024 edition 的稳定 Rust 工具链、Cargo、C/C++ 编译器、CMake、Perl 和 make（构建 vendored librdkafka / OpenSSL）。macOS 可使用 Xcode Command Line Tools；Linux 需安装相应编译工具。
- Node.js 22.12+（或 24 LTS）、npm。
- 可访问的 Kafka broker；测试需要 Docker Engine/Desktop 和支持 `up --wait` 的 Docker Compose v2。
- 浏览器：现代 Chromium、Firefox 或 Safari；自动化验收使用 Playwright Chromium。

## 本地运行

在项目根目录启动后端：

```bash
export KAFKA_DASHBOARD_MASTER_KEY="$(openssl rand -base64 32)"
export DATABASE_URL='sqlite://kafka-dashboard.db'
export KAFKA_DASHBOARD_BIND='127.0.0.1:3000'
cargo run -p backend --bin kafka-dashboard
```

主密钥必须是 **32 字节随机值的标准 Base64**。请将首次生成的值保存在安全的密钥管理位置，每次启动使用同一个值；重启时重新生成会导致已有密文无法解密。不要提交密钥、`.env` 或数据库，也不要在命令输出和日志中记录它们。缺少或格式不正确的密钥会使服务拒绝启动。

`DATABASE_URL` 默认 `sqlite://kafka-dashboard.db`；可指定绝对路径，例如 `sqlite:///var/lib/kafka-lens/config.db`，父目录需提前创建且可写。启动时自动迁移。备份数据库时也需安全保管原主密钥。绑定地址默认 `127.0.0.1:3000`，设为 `127.0.0.1:0` 可由系统选择临时端口；启动日志会报告地址。健康检查为 `GET /api/health`。

另开终端启动前端：

```bash
npm --prefix frontend ci
npm --prefix frontend run dev -- --host 127.0.0.1
```

打开 Vite 输出的地址，在「集群设置」中新增集群、测试连接，然后进入概览、Topics 或 Consumer Groups。开发及 preview 的 `/api` 请求代理到 `http://127.0.0.1:3000`；后端地址变化时设置 `KAFKA_DASHBOARD_API_TARGET` 再启动 Vite。构建与本地预览：

```bash
npm --prefix frontend run build
npm --prefix frontend run preview -- --host 127.0.0.1
```

## 连接与凭据

Bootstrap Servers 填写逗号分隔的 `host:port`。后端必须能访问 Kafka 返回的 advertised listeners 地址。支持 `PLAINTEXT`、`SSL`、`SASL_PLAINTEXT` 和 `SASL_SSL`；SASL Mechanism 可填 `PLAIN`、`SCRAM-SHA-256` 或 `SCRAM-SHA-512`，同时填写用户名和密码。TLS 使用 CA PEM，双向 TLS 另需成对提供客户端证书 PEM 和客户端私钥 PEM。

密码、CA、客户端证书和私钥以 AES-256-GCM 加密存入 SQLite，API 只返回“已配置”标志，不回显内容。编辑时敏感字段留空或省略表示保留；API 显式传入 `clearPassword: true` 可清空密码。请求体上限为 256 KiB，超限返回 413；连接错误及校验错误使用安全错误消息与 Request ID。

## Offset、Lag 与状态语义

- Low watermark 是最早保留的位置，High watermark 是下一条将写入的位置；非空分区的最大消息 Offset 为 `high - 1`，空分区显示 `—`。
- `high - low` 是 Offset 范围估算，不等于实际消息数（压缩、删除、事务记录都会影响）。各 Partition Offset 相互独立，Topic 汇总不能当成统一序列。
- Committed Offset 是消费组提交的下一条待消费位置，Lag 为 `max(high - committed, 0)`。未提交、不可用和采集失败显示未知，不能当成 0；部分失败保留旧显示值并标记陈旧。
- 自定义 Group / Topic 规则覆盖集群默认规则。正常为 `Lag ≤ 正常上限`；高 Lag 为达到高 Lag 起始值，中间为积压。超限需连续达到设定时长；隐藏页面、连接中断、重建会话或切换集群会重置连续计时。
- 图表仅保留当前页面内存中最近一小时样本；刷新页面清空历史。采样间隔可选 3 / 5 / 10 秒。

## 短租约与资源释放

保存配置不会保持 Kafka 连接。进入仪表盘或配置规则时创建按需监控会话，租约为 15 秒，快照请求续租。同一会话同时只允许一个采集，重叠请求返回 409。后台每 5 秒清理过期会话；隐藏页面停止仪表盘轮询，页面离开、切换集群和删除集群会主动关闭对应会话，关闭请求未送达时由租约兜底。SIGINT / SIGTERM 会关闭全部会话并等待在途采集完成后退出。

## 验证与临时 Kafka

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test scripts/e2e-preview.test.mjs
npm --prefix frontend exec -- playwright install chromium
npm --prefix frontend run test:e2e
git diff --check
```

E2E 一键脚本使用独立 Compose project 和动态本地端口，启动 Apache Kafka 3.9.1 KRaft；Rust fixture 只向本次临时容器创建 `dashboard-e2e-orders` 的两个分区、在分区 0 写入 10 条消息，并为 `dashboard-e2e-reader` 提交位置 4 / 0，预期 Lag 为 6 / 0。脚本自动使用临时 SQLite、随机主密钥、后端和前端进程。Vite 直接绑定端口 0，脚本从子进程实际监听地址获取端口，并核验 PID 存活及本次运行标识，防止误用其他健康服务。正常结束、失败或收到 INT / TERM 均清理进程、容器、卷和临时文件。不要手动把 fixture 指向已有 Kafka；普通 `cargo test` 默认跳过需要 broker 的测试。

浏览器验收覆盖集群 CRUD、测试连接、真实水位/Lag、规则保存与自定义标记、刷新频率、切换和离开时释放会话；另验证 API / 运行日志不泄漏测试秘密、超大请求、历史淘汰和隐藏时无轮询。历史测试只加速响应中的采集时间，指标仍来自 Kafka；headless 环境通过浏览器 visibility 事件模拟隐藏。Rust 测试额外覆盖采集互斥、删除释放和 SIGTERM。Playwright 默认关闭 trace / screenshot，避免持久化请求中的敏感字段。

仅需手动启动空白本地测试 broker 时：

```bash
docker compose -f docker-compose.test.yml -p kafka-lens-manual up --wait
# 默认入口 127.0.0.1:19092；冲突时在启动前设置 KAFKA_TEST_PORT
docker compose -f docker-compose.test.yml -p kafka-lens-manual down --volumes --remove-orphans
```

## 部署边界

当前服务没有内置用户登录或权限系统。生产环境应置于受保护的内网或带身份验证的反向代理之后；使用 HTTPS，对 `/api` 配置代理和访问控制，静态资源从 `frontend/dist` 提供，并将前端路由回退至 `index.html`。Vite preview 仅用于本地验收。后端默认只绑定回环地址；代理请求体限制应与 256 KiB 上限一致，超时需容纳 Kafka 采集。限制数据库及密钥的文件权限，Kafka 使用最小只读权限账户，优先选择 SASL_SSL / SSL。发布打包及更详细的部署流程另行维护。
