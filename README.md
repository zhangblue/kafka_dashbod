# Kafka Lens

Kafka 只读监控面板：管理集群连接配置，查看 Topic / Partition 水位、Consumer Group Lag、最近一小时趋势，以及集群默认和 Group / Topic 自定义 Lag 规则。后端为 Rust / Axum，前端为 React / Vite，SQLite 只保存配置与明文 Kafka 凭据。常规应用不会创建 Topic、生产消息或提交消费位置。

当前版本没有后台常驻采集、跨页面持久化指标、消息内容浏览、消费位置重置、通知渠道或内置用户认证。数据范围受 Kafka 账户权限限制。截图约定放在 `docs/screenshots/`（当前未附截图）；如补充截图，请使用临时测试集群并隐藏连接地址及所有凭据。

## 系统要求

- 支持 Rust 2024 edition 的稳定 Rust 工具链、Cargo、C/C++ 编译器、CMake、Perl 和 make（构建 vendored librdkafka / OpenSSL）。macOS 可使用 Xcode Command Line Tools；Linux 需安装相应编译工具。
- Node.js 22.12+（或 24 LTS）、npm。
- 可访问的 Kafka broker；测试需要 Docker Engine/Desktop 和支持 `up --wait` 的 Docker Compose v2。
- 浏览器：现代 Chromium、Firefox 或 Safari；自动化验收使用 Playwright Chromium。

## 页面视觉基准

所有页面统一遵循已确认的 [Kafka Dashboard Demo](docs/ui/approved-dashboard-demo.html)：桌面左侧导航、单行顶部控制栏、四张概览指标卡、Lag 趋势与分布双栏、紧凑可展开表格，以及集群卡片与配置弹窗。原稿仅作为样式参考，里面的模拟数值和历史配置文案不代表当前程序行为。

正式页面使用实时采集数据；Lag 分布按 Consumer Group 汇总多个 Topic，未知分区明确标注。进入集群设置会释放仪表盘会话；打开 Lag 规则弹窗才按需连接，关闭后释放。窄屏保留顶部导航并允许表格内部横向滚动。

## 本地运行

在项目根目录创建 `.env` 后启动后端。复制发行模板后，执行 Cargo 前先编辑 `.env`：源码开发通常由 Vite 提供页面，应移除 `KAFKA_DASHBOARD_WEB_DIR=web`，或调整为含 `index.html` 的实际构建目录；将 `DATABASE_URL` 改为 `sqlite://kafka-dashboard.db?mode=rwc`，或先创建模板所需的可写 `data/` 目录。

```bash
cp packaging/.env.example .env
# 按上文调整 .env 后再启动
cargo run -p backend --bin kafka-dashboard
```

Rust 正常启动时必须有可读且格式有效的 `.env`，即使全部配置已由外部环境变量提供。**警告：SQLite 明文保存 Kafka 凭据。** 限制 `.env`、数据库及备份的访问权限，不要提交这些文件，也不要在命令输出和日志中记录其中内容。

`DATABASE_URL` 默认 `sqlite://kafka-dashboard.db`；可指定绝对路径，例如 `sqlite:///var/lib/kafka-lens/config.db`，父目录需提前创建且可写。启动时初始化当前数据库结构；旧加密版本数据库不兼容，须按下文升级步骤手动删除后重新配置。绑定地址默认 `127.0.0.1:3000`，设为 `127.0.0.1:0` 可由系统选择临时端口；启动日志会报告地址。健康检查为 `GET /api/health`。

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

## 环境变量

| 变量 | 默认值 / 用途 |
| --- | --- |
| `DATABASE_URL` | 未设置时为 `sqlite://kafka-dashboard.db`；发行模板为 `sqlite://data/kafka-dashboard.db?mode=rwc` |
| `KAFKA_DASHBOARD_BIND` | `127.0.0.1:3000`，HTTP 监听地址 |
| `KAFKA_DASHBOARD_WEB_DIR` | 未设置时仅提供 API；发行模板为 `web`，自定义目录必须含 `index.html` |
| `KAFKA_DASHBOARD_API_TARGET` | Vite 开发/preview 代理目的地，默认 `http://127.0.0.1:3000` |
| `RUST_LOG` | 可用 `info` 设置日志级别；调试时也不得输出敏感配置 |
| `KAFKA_TEST_PORT` | 手动 Compose 测试 broker 默认宿主端口 `19092`；一键 E2E 自动选择 |

Rust 正常启动时强制读取当前工作目录的 `.env`，文件缺少、不可读或格式错误会启动失败；父进程已设置的同名环境变量优先于文件中的值。`--version` 查询例外，无需 `.env`。发行启动器切换到发行目录并创建 `data/`，不解析、设置或导出配置，也不透传运行参数；`packaging/.env.example` 是可直接复制的发行配置模板。源码运行的工作目录通常为仓库根目录，相对路径均以当前工作目录为基准。

## 连接与凭据

Bootstrap Servers 填写逗号分隔的 `host:port`。后端必须能访问 Kafka 返回的 advertised listeners 地址。支持 `PLAINTEXT`、`SSL`、`SASL_PLAINTEXT` 和 `SASL_SSL`；SASL Mechanism 可填 `PLAIN`、`SCRAM-SHA-256` 或 `SCRAM-SHA-512`，同时填写用户名和密码。TLS 使用 CA PEM，双向 TLS 另需成对提供客户端证书 PEM 和客户端私钥 PEM；加密私钥可填写客户端私钥口令（`clientKeyPassphrase`）。

| Security Protocol | 连接配置 |
| --- | --- |
| `PLAINTEXT` | 无 SASL 或 TLS；仅适合受信任网络 |
| `SSL` | TLS；按 broker 信任链提供 CA，双向认证时同时提供客户端证书和私钥 |
| `SASL_PLAINTEXT` | SASL mechanism、用户名、密码；传输不加密 |
| `SASL_SSL` | SASL mechanism、用户名、密码，加 TLS 配置 |

不支持 OAuth/OIDC、Kerberos 或任意 librdkafka 参数透传。TLS 客户端证书和私钥必须成对填写；不要把 SASL 字段配置到非 SASL 协议。

**警告：密码、CA、客户端证书、私钥和私钥口令以明文 JSON 存入 SQLite。** 任何能读取数据库、sidecar 或备份的人都能获得这些凭据；请限制文件权限并将备份保存在访问受控的位置；建议启用磁盘加密。API 只返回各字段的“已配置”标志，不回显内容，日志也不得输出凭据。编辑时敏感字段留空或省略表示保留；使用对应的清除选项可显式移除。API 字段为 `clearPassword`、`clearCaPem`、`clearClientCertificatePem`、`clearClientKeyPem` 和 `clearClientKeyPassphrase`，设为 `true` 时清除该字段；同一字段不能同时填写新值并要求清除。保存前会校验合并后的完整配置：切换协议时须清除不适用的凭据，证书与私钥仍须成对保留，私钥口令不能脱离私钥单独存在。

新增或编辑表单均可在保存前测试连接。新配置使用 `POST /api/clusters/test`，未保存的编辑使用 `POST /api/clusters/:id/test`，请求体均为完整表单字段；编辑草稿中省略或留空的敏感字段会复用已保存值，清除选项只影响此次测试。原有 `POST /api/clusters/:id/test` 空请求体继续测试已保存配置。测试不会写入配置，也不会创建监控会话；临时工作由会话管理器跟踪，删除集群或退出进程时会等待相关测试释放资源。请求体上限为 256 KiB，超限返回 413；连接错误及校验错误使用安全错误消息与 Request ID。

## Offset、Lag 与状态语义

- Low watermark 是最早保留的位置，High watermark 是下一条将写入的位置；非空分区的最大消息 Offset 为 `high - 1`，空分区显示 `—`。
- `high - low` 是 Offset 范围估算，不等于实际消息数（压缩、删除、事务记录都会影响）。各 Partition Offset 相互独立，Topic 汇总不能当成统一序列。
- Committed Offset 是消费组提交的下一条待消费位置，Lag 为 `max(high - committed, 0)`。未提交、不可用和采集失败显示未知，不能当成 0；部分失败保留旧显示值并标记陈旧。
- 自定义 Group / Topic 规则覆盖集群默认规则，并在对应行显示自定义标记。默认正常为 `0–999`，积压为 `1000–4999`，高 Lag 为 `≥5000`，持续超限时间为 `30 秒`。自定义时必须满足 `0 ≤ 正常上限 < 高 Lag 起始值`，持续时间可为 0。
- Lag 超过正常上限后，从首个有效超限样本开始计时；持续达到设定时长才显示积压或高 Lag，之前状态仍为正常。恢复正常会清零，隐藏页面、连接中断、采集异常、未提交、重建会话、规则变化或切换集群也会重置。Flink checkpoint 引起的短暂锯齿形 Lag 因此不会立刻变成告警状态；页面未打开期间不能推断持续超限。
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
node --test tests/release_scripts.test.mjs
npm --prefix frontend exec -- playwright install chromium
npm --prefix frontend run test:e2e
git diff --check
```

E2E 一键脚本使用独立 Compose project 和动态本地端口，启动 Apache Kafka 3.9.1 KRaft；Rust fixture 只向本次临时容器创建 `dashboard-e2e-orders` 的两个分区、在分区 0 写入 10 条消息，并为 `dashboard-e2e-reader` 提交位置 4 / 0，预期 Lag 为 6 / 0。脚本自动使用临时 SQLite、后端和前端进程。Vite 直接绑定端口 0，脚本从子进程实际监听地址获取端口，并核验 PID 存活及本次运行标识，防止误用其他健康服务。正常结束、失败或收到 INT / TERM 均清理进程、容器、卷和临时文件。不要手动把 fixture 指向已有 Kafka；普通 `cargo test` 默认跳过需要 broker 的测试。

浏览器验收覆盖集群 CRUD、测试连接、真实水位/Lag、规则保存与自定义标记、刷新频率、切换和离开时释放会话；另验证 API / 运行日志不泄漏测试秘密、超大请求、历史淘汰和隐藏时无轮询。历史测试只加速响应中的采集时间，指标仍来自 Kafka；headless 环境通过浏览器 visibility 事件模拟隐藏。Rust 测试额外覆盖采集互斥、删除释放和 SIGTERM。Playwright 默认关闭 trace / screenshot，避免持久化请求中的敏感字段。

仅需手动启动空白本地测试 broker 时：

```bash
docker compose -f docker-compose.test.yml -p kafka-lens-manual up --wait
# 默认入口 127.0.0.1:19092；冲突时在启动前设置 KAFKA_TEST_PORT
docker compose -f docker-compose.test.yml -p kafka-lens-manual down --volumes --remove-orphans
```

## 部署边界

当前服务没有内置用户登录或权限系统。生产环境应置于受保护的内网或带身份验证的反向代理之后；使用 HTTPS，对 `/api` 配置代理和访问控制。发行包由后端直接提供静态资源和 SPA fallback。Vite preview 仅用于本地验收。后端默认只绑定回环地址；代理请求体限制应与 256 KiB 上限一致，超时需容纳 Kafka 采集。限制数据库、sidecar 和备份的文件权限，受控保存备份；建议启用磁盘加密。Kafka 使用最小只读权限账户，优先选择 SASL_SSL / SSL。

## 双平台离线发行包

构建主机限定为 Apple Silicon macOS（Darwin/arm64），需要前述 Rust / C / Node 构建工具，以及能运行 `linux/amd64` 的 Docker Engine / Buildx。Rust 需安装 `aarch64-apple-darwin` target；Docker 构建阶段负责 Linux musl 工具链。首次构建需要网络下载 npm、Cargo 和容器镜像依赖。

唯一打包入口：

```bash
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
```

构建按锁文件安装依赖，先完成前端、macOS 和 Linux 编译，在临时目录组装、校验和压缩后通过同文件系统重命名发布；失败会恢复已有发行包。不要并发运行打包命令，也不要直接覆盖正在使用的发行目录。`dist/` 不进入 Git。产物为 `dist/kafka-dashboard.tar.gz` 及以下目录：

```text
kafka-dashboard/
├── bin/
│   ├── kafka-dashboard-darwin-arm64
│   └── kafka-dashboard-linux-amd64
├── web/                 # 已构建的前端页面和 assets
├── start.sh
├── .env.example         # 启动前复制为 .env 的配置模板
├── README.txt           # 离线运行说明
├── SHA256SUMS
└── data/                # 首次启动后创建，不进入发行归档
    ├── kafka-dashboard.pid
    ├── kafka-dashboard.log
    └── kafka-dashboard.db  # 模板配置的 SQLite 数据库
```

复制整个目录或解压归档到目标机器的可写位置，进入发行目录后核验 `shasum -a 256 -c SHA256SUMS`（Linux 也可用 `sha256sum -c SHA256SUMS`），执行 `cp .env.example .env`，按需编辑配置并执行 `chmod 600 .env` 限制权限。支持 **macOS arm64 和 Linux x86_64**；启动器将 `Darwin/arm64` 映射到 `bin/kafka-dashboard-darwin-arm64`，将 `Linux/x86_64` 映射到 `bin/kafka-dashboard-linux-amd64`，其他平台拒绝启动。

```bash
./start.sh                 # 后台启动，等同于 ./start.sh start
./start.sh status          # 输出“Kafka Lens 正在运行，PID: …”或“Kafka Lens 未运行”
./start.sh stop            # 发送 SIGTERM 并等待正常退出
./start.sh restart         # 正常停止后重新启动
./start.sh --version       # 前台显示版本，无需 .env
```

`status` 通过文本表达业务状态，运行中与未运行均以退出码 0 结束；PID 无效、身份不匹配等管理错误以非零退出码结束。重复启动会返回非零退出码并明确输出 `Kafka Lens 已在运行，PID: <pid>`。启动器固定使用 `data/kafka-dashboard.pid` 和 `data/kafka-dashboard.log`；日志追加写入，不自动轮转，启动失败时先查看此日志。`stop` 只发送 SIGTERM 并有界等待程序释放在途 Kafka 工作和会话，超时返回非零退出码、保留 PID 文件，不会自动发送 SIGKILL；`restart` 仅在 `stop` 成功后启动。生产部署应配置系统日志轮转，或使用服务管理器管理进程与日志。

模板默认访问地址为 `http://127.0.0.1:3000`，健康接口为 `/api/health`，用 `KAFKA_DASHBOARD_BIND` 修改监听地址。启动器切换到发行目录并创建私有权限的 `data/`；模板通过相对路径指定 `web/` 和 `data/kafka-dashboard.db`，可整体搬移。正常启动仍由 Rust 强制读取发行目录中可读且格式有效的 `.env`；启动器不解析、设置或导出配置，不透传运行参数。

从旧加密凭据版本升级时，先停止服务并确认当前 `DATABASE_URL` 指向的数据库（发行包默认为 `data/kafka-dashboard.db`，源码运行默认为仓库根目录的 `kafka-dashboard.db`）。手动删除该旧数据库及同名的 `-wal`、`-shm`、`-journal` sidecar，再运行新版本并重新配置全部集群和 Lag 规则。此操作会丢弃旧配置；程序不会转换旧凭据或自动删除数据库。

启动器将工作目录切换到发行目录，模板使用固定的相对 SQLite URL，目录名含 `?`、`%20`、空格或中文时也不会改变数据库实际位置。父进程显式设置的同名环境变量优先于 `.env`，其值原样保留，其中相对路径以发行目录为基准。

运行机器不需要 Node.js、Rust、系统 librdkafka 或系统 OpenSSL。Linux 二进制使用 musl，静态链接 librdkafka、SQLite 和 vendored OpenSSL；macOS 保留操作系统原生库依赖（如 libSystem），并非完全静态。Linux 包不携带宿主机 CA 信任库，私有 TLS 请在集群设置显式配置 CA PEM。网络/DNS、可写存储和操作系统执行许可仍需可用。

验证命令需在构建主机执行，使用 `curl`、`file`、`shasum`、`sqlite3`、`otool` 和 Docker。它校验完整文件清单、哈希、架构和静态链接边界，在临时发行副本中复制 `.env.example` 为 `.env`，通过 `start/status/stop` 管理 macOS 与 Linux/amd64 容器中的真实程序，验证版本、后台生命周期、SQLite 迁移、健康接口、页面/资源、SPA fallback 和正常停止；验证后清理临时进程和容器，不向原发行目录写入 `.env`、数据库、PID 或日志。

## 故障排查

| 现象 | 检查方法 |
| --- | --- |
| 启动提示配置数据库不兼容 | 停止服务，确认数据库路径，手动删除旧 DB 和 sidecar 后重新启动并配置集群与 Lag 规则；旧加密数据不自动转换 |
| 无法初始化数据库 | 检查 SQLite URL、父目录是否存在以及运行账户读写权限；移动当前版本发行包时保留 `data/` 并保护其明文凭据 |
| 连接超时或 DNS 错误 | 从后端所在机器检查 Bootstrap Servers 和所有 advertised listeners 是否可达，核对 DNS、防火墙与连接超时配置 |
| SASL/TLS 失败 | 核对四类协议、SASL mechanism、账户权限和证书信任链；双向 TLS 证书/私钥须配对；Linux 发行包不含宿主 CA 信任库 |
| 数据未知、陈旧或缺少消费组 | 查看页面安全错误及 Request ID，检查只读账户的元数据/消费组权限；未提交 Offset 不等于 Lag 为 0；部分失败不会覆盖旧值为 0 |
| 状态一直正常或计时重置 | 检查默认/自定义阈值、持续时间及页面可见性；仅连续有效超限采样累积计时 |
| 页面 404 或 `/api` 请求失败 | 源码运行先启动 Vite；生产检查 `KAFKA_DASHBOARD_WEB_DIR` 中的 `index.html`，开发检查代理目的地；未知 `/api` 返回 JSON 404 是预期行为 |
| E2E 无法启动 | 检查 Docker 是否运行、Compose v2 是否支持 `--wait`、Chromium 是否已安装，并允许测试进程监听本地临时端口 |
| 发行验证失败 | 在 Apple Silicon macOS 上运行，确认 Docker 可运行 Linux/amd64；哈希不匹配时重新构建或重新复制完整发行包 |

提交故障信息时只提供安全错误、Request ID、版本与复现步骤；不要附真实数据库、环境变量转储、PEM 或包含请求体的 trace。
