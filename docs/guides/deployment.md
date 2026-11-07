# 打包与部署

## 下载并启动发行包

从 [GitHub Releases](https://github.com/zhangblue/kafka_dashbod/releases) 下载与运行机器匹配的部署包和同名 `.sha256` 文件。Linux x86_64 选 `linux-amd64`，Linux aarch64 选 `linux-arm64`，Apple Silicon Mac 选 `darwin-arm64`。以 v0.4.0 的 Linux AMD64 包为例，在下载目录运行以下命令。

```bash
sha256sum -c kafka-dashboard-v0.4.0-linux-amd64.tar.gz.sha256
tar -xzf kafka-dashboard-v0.4.0-linux-amd64.tar.gz
cd kafka-dashboard-v0.4.0-linux-amd64
```

macOS 可使用 `shasum -a 256 -c` 校验对应归档。

首次安装时，在解压出的目录中校验文件、创建配置并限制其读取权限。

```bash
shasum -a 256 -c SHA256SUMS  # Linux 也可以使用 sha256sum -c SHA256SUMS
cp .env.example .env
chmod 600 .env
```

按需编辑 `.env` 后启动。

```bash
./start.sh                 # 后台启动，等同于 ./start.sh start
./start.sh status          # 输出“Kafka Lens 正在运行，PID: …”或“Kafka Lens 未运行”
./start.sh stop            # 发送 SIGTERM 并等待正常退出
./start.sh restart         # 正常停止后重新启动
./start.sh --version       # 前台显示版本，无需 .env
```

`status` 通过文本表达业务状态，运行中与未运行均以退出码 0 结束；PID 无效、身份不匹配等管理错误以非零退出码结束。重复启动会返回非零退出码并明确输出 `Kafka Lens 已在运行，PID: <pid>`。启动器固定使用 `data/kafka-dashboard.pid` 和 `data/kafka-dashboard.log`；日志追加写入，不自动轮转，启动失败时先查看此日志。`stop` 只发送 SIGTERM 并有界等待程序释放在途 Kafka 工作和会话，超时返回非零退出码、保留 PID 文件，不会自动发送 SIGKILL；`restart` 仅在 `stop` 成功后启动。生产部署应配置系统日志轮转，或使用服务管理器管理进程与日志。

模板默认访问地址为 `http://127.0.0.1:3000`，健康接口为 `/api/health`，用 `KAFKA_DASHBOARD_BIND` 修改监听地址。启动器切换到发行目录并创建私有权限的 `data/`；模板通过相对路径指定 `web/` 和 `data/kafka-dashboard.db`，可整体搬移。正常启动仍由 Rust 强制读取发行目录中可读且格式有效的 `.env`；启动器不解析、设置或导出配置，不透传运行参数。

启动器将工作目录切换到发行目录，模板使用固定的相对 SQLite URL，目录名含 `?`、`%20`、空格或中文时也不会改变数据库实际位置。父进程显式设置的同名环境变量优先于 `.env`，其值原样保留，其中相对路径以发行目录为基准。

运行机器不需要 Node.js、Rust、系统 librdkafka 或系统 OpenSSL。Linux 二进制使用 musl，静态链接 librdkafka、SQLite 和 vendored OpenSSL；macOS 保留操作系统原生库依赖（如 libSystem），并非完全静态。Linux 包不携带宿主机 CA 信任库，私有 TLS 请在集群设置显式配置 CA PEM。网络/DNS、可写存储和操作系统执行许可仍需可用。

## 部署边界

当前服务没有内置用户登录或权限系统。能够访问管理 API 的调用者可以使用所配置 Kafka 凭据的管理权限，页面确认框不是身份认证。生产环境必须置于受保护的内网或带身份验证的 HTTPS 反向代理之后，对全部 `/api` 配置访问控制，不得直接暴露到公网。发行包由后端直接提供静态资源和 SPA fallback。Vite preview 仅用于本地验收。后端默认只绑定回环地址；代理请求体限制应与 256 KiB 上限一致，超时需容纳 Kafka 采集及管理操作。限制数据库、sidecar 和备份的文件权限，受控保存备份；建议启用磁盘加密。仅监控时使用最小只读权限；需要管理时按 [Topic 管理权限表](../features/topics.md#最小-kafka-权限与兼容性)限定 Topic 范围和操作权限，优先选择 SASL_SSL / SSL。

## 升级

常规升级时先停止旧服务，备份其 `.env` 和 `data/`。解压到新目录，保留私有权限复制配置和数据，再启动新版本。

从旧加密凭据版本升级时，先停止服务并确认当前 `DATABASE_URL` 指向的数据库（发行包默认为 `data/kafka-dashboard.db`，源码运行默认为仓库根目录的 `kafka-dashboard.db`）。手动删除该旧数据库及同名的 `-wal`、`-shm`、`-journal` sidecar，再运行新版本并重新配置全部集群和 Lag 规则。此操作会丢弃旧配置；程序不会转换旧凭据或自动删除数据库。

## 从源码打包

每次必须显式选择一个平台，不提供默认平台或 `all`：

| 平台 | Rust target | 构建命令 |
| --- | --- | --- |
| `linux-amd64` | `x86_64-unknown-linux-musl` | `cargo zigbuild` |
| `linux-arm64` | `aarch64-unknown-linux-musl` | `cargo zigbuild` |
| `darwin-arm64` | `aarch64-apple-darwin` | `cargo build`（须 Apple Silicon macOS） |

构建需要[开发环境](local-development.md)中的 Rust / C / Node 工具、相应 Rust target；Linux 交叉编译另需 cargo-zigbuild、Zig 与 curl 开发头文件。macOS 自动从 Xcode SDK 提取 `curl/`；Linux 可安装 curl 开发包（如 `libcurl4-openssl-dev`），或设置 `KAFKA_DASHBOARD_CURL_INCLUDE_DIR` 为含 `curl/curl.h` 的 include 目录。脚本只复制 `curl/` 到 Cargo 缓存的 `release-curl-headers/`，不会引入宿主 libc 头文件，不修改 Cargo registry。此头文件兼容处理不启用 OIDC/curl 运行依赖。首次构建需要网络下载 npm / Cargo 依赖；半离线指目标机器无需下载编译依赖，运行仍需 Kafka、飞书业务网络。

唯一打包入口（示例版本应替换为 `backend/Cargo.toml` 当前版本）：

```bash
./tools/build-release.sh --platform linux-amd64
./tools/build-release.sh --platform linux-arm64
./tools/build-release.sh --platform darwin-arm64 --version v0.4.0
./tools/verify-release.sh dist/kafka-dashboard-v0.4.0-linux-amd64
./tools/verify-release.sh dist/kafka-dashboard-v0.4.0-linux-arm64
./tools/verify-release.sh dist/kafka-dashboard-v0.4.0-darwin-arm64
```

`--version` 可省略；显式版本必须为 `vX.Y.Z` 且与 Cargo 包版本一致。每次只编译并打包所选架构，保留 Cargo 锁文件检查。`CARGO_TARGET_DIR` 可指定共享构建缓存；默认使用仓库 `target/`。同一 worktree 的独占构建锁防止前端和 dist 竞争，不要并发打包。构建、归档或发布任一步失败均保留已有三件产物；若目标目录有 `data/` 或真实 `.env`，脚本拒绝替换，避免删除部署数据。不同架构可共存。`dist/` 不进入 Git。

每个平台输出 `dist/kafka-dashboard-v<版本>-<platform>/`、同名 `.tar.gz` 和 `.tar.gz.sha256`。归档 sidecar 校验真实压缩字节；例如在 dist 下执行 `shasum -a 256 -c kafka-dashboard-v0.4.0-linux-arm64.tar.gz.sha256`。目录结构如下（每包只含一个二进制）：

```text
kafka-dashboard-v0.4.0-linux-arm64/
├── bin/kafka-dashboard-linux-arm64
├── web/                 # 完整前端页面和 assets
├── start.sh
├── .env.example
├── README.txt
├── RELEASE.json         # version/platform/target/commit/dirty 来源记录
└── SHA256SUMS           # 除自身外全部文件的有序完整清单
```

`RELEASE.json` 的 `dirty` 仅统计 Git 未忽略的源码变更；本地验证允许 dirty 包，正式发布要求标签精确提交且源码干净。运行时创建的 data、PID、日志、数据库及真实 `.env` 不进入归档。

如果出现 `Failed to find zig`，检查 `zig version` 和 `CARGO_ZIGBUILD_ZIG_PATH`。设置此变量时，值必须指向可执行文件本身，例如 `/opt/zig/zig`。

## 验证单个平台

验证命令只验证传入的一个平台，使用 `curl`、`file`、`shasum`、`sqlite3`；macOS 包还需 Apple Silicon macOS 与 `otool`，Linux 包需要可运行对应 `linux/amd64` 或 `linux/arm64` 的 Docker。它校验完整文件清单、哈希、来源、版本、架构和静态链接边界，在临时发行副本中复制 `.env.example` 为 `.env`，通过 `start/status/stop` 管理本机 macOS 或对应 Linux 容器中的真实程序，验证版本、后台生命周期、SQLite 迁移、健康接口、页面/资源、SPA fallback 和正常停止；验证后清理临时进程和容器，不向原发行目录写入 `.env`、数据库、PID 或日志。

## 发布到 GitHub

`.github/workflows/release.yml` 接受 `v*` 标签推送，或手动指定已存在的 `vX.Y.Z` 标签。三个独立 runner 分别构建并验证 Linux AMD64、Linux ARM64、macOS ARM64；全部成功后，唯一发布 job 下载本次运行的三份 artifact。构建固定 Rust 1.99.1、Zig 0.15.2、cargo-zigbuild 0.23.0，Node 使用 24 系列。构建权限为 `contents:read`，仅发布 job 为 `contents:write`；相同标签的发布不会取消在途运行。

发布入口为 `node tools/publish-release.mjs vX.Y.Z <artifacts-dir>`，本机需要 Node.js、Python 3.9+、Git 和已鉴权且有仓库写权限的 GitHub CLI。目录必须只有三个 `.tar.gz` 与各自 `.tar.gz.sha256`，共六件普通文件。手工发布时须在 `dist/` 内另建只含这六件的子目录；构建输出的 `dist/` 同时有展开目录，不能直接作为发布输入。门禁核对 Cargo 版本、checkout 与本地及远端标签精确 commit、包内 version/platform/target/commit/dirty（必须为 false）及压缩字节 checksum，同时验证内部完整 checksum、必需文件、单对应架构二进制与安全文件清单，不执行归档内程序。Release 先保持草稿；上传后检查远端六件名称/大小，重新下载并逐字节比对全部六件，再次确认标签和草稿状态，最后取消草稿并核对正式状态。通过成功的鉴权分页列表查找草稿；已正式发布的版本一律拒绝改写，404、鉴权或网络失败均不会当作可创建新 Release 的证据。

发布 CLI 的静态检查验证完整 ELF64/Mach-O 可执行结构、段/加载命令及文件边界和入口，Linux 拒绝 `PT_INTERP` 与 `DT_NEEDED` 动态依赖。静态结构与 checksum 不能证明启动器或二进制的实际行为，不能替代三个平台各自的 `tools/verify-release.sh` 真实运行验收。推荐使用上述 Actions，在每个平台构建及运行验收成功后汇总发布；手工调用发布 CLI 前也必须完成三平台验收，CLI 不会自行执行包内程序。

失败可用**同一份六附件原始字节**重试，仅续传缺少的文件；已有草稿附件须与本地一致，不使用 `--clobber`。Actions 可重跑失败的发布 job，复用本次 run 的构建 artifacts。重跑构建可能改变归档字节；若已有附件不同、额外附件或 checksum 损坏，应先由维护者检查草稿，再人工处理草稿或使用新版本标签，脚本不会自动删除附件。发布前构建和上传仍需依赖下载与 GitHub 网络；半离线仅描述目标机器的运行依赖边界。

本地发布回归使用 `node --test tests/publish_release.test.mjs`，真实执行临时 Git 标签、tar 和 SHA-256，只替代外部 `gh`，不写入 GitHub。

[返回项目首页](../../README.md)
