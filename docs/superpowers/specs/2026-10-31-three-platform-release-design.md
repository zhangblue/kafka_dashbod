# 已确认设计：三平台独立半离线包

用户确认三平台各自打包：`--platform linux-amd64` 使用 `cargo zigbuild --release --target x86_64-unknown-linux-musl`，`--platform linux-arm64` 使用 `cargo zigbuild --release --target aarch64-unknown-linux-musl`，`--platform darwin-arm64` 在苹果芯片 macOS 使用 `cargo build --release --target aarch64-apple-darwin`。保留 Cargo --locked、backend 包与 kafka-dashboard bin 限定。无参数不构建，无 all 目标。

输出 `dist/kafka-dashboard-v<版本>-<platform>/`、同名 `.tar.gz`、`.tar.gz.sha256`。每包只含一个匹配架构的二进制、完整前端、启动器、配置模板、说明、完整 SHA256SUMS 和必要版本/平台/commit 来源记录。版本来自 Cargo.toml；可选显式版本必须与 Cargo 匹配。不同架构互不覆盖；失败恢复旧包，不能把运行 data、日志、真实 .env、数据库、密钥打入归档。

半离线：目标机器不需要下载编译依赖或安装 Rust/Node/librdkafka/OpenSSL；仍需要 Kafka/飞书的业务网络。Linux 静态 musl，macOS 允许系统原生库。启动器继续后台启动、固定 PID/log 路径、完整进程身份匹配、锁和有界 SIGTERM；新增 Linux ARM64，不弱化现有保护。

发布必须使用标签的精确提交，标签版本与 Cargo 一致。三个独立构建 job 完成构建和对应运行验证后，汇总三份归档与三个校验 sidecar，共六附件。Release 保持草稿直到六件全部上传、重新下载并与本地字节和 SHA-256 比对、确认标签和附件清单。失败不得宣称成功，已发布版本不得自动覆盖。发布脚本须可使用假的 gh 外部边界进行行为测试，测试不得触发真实 GitHub 写操作。

本次只实现及验证流程，不实际创建标签或发布，不自动提交或推送。原 main 保持不变，改动留在用户批准的独立 worktree。
