# 三平台独立发行与完整附件发布实现计划

> **面向 AI 代理的工作者：** 使用 superpowers:subagent-driven-development 逐任务实现；用户已确认设计和独立 worktree。不得自动提交、推送、创建标签或实际发布 Release；保留变更供用户验收。

**目标：** 每次仅构建所选平台，GitHub Release 必须从同一标签构建并核验三包三校验文件后才发布。

**架构：** Shell 构建与运行验证入口按平台参数化。Node 发布门禁处理附件和 GitHub CLI 边界；Actions 在独立 runner 上调用同一构建入口，汇总六个文件并通过门禁发布。原生依赖必须实测，不以 mock 构建冒充交叉编译成功。

**技术栈：** POSIX shell、Node.js 内置测试、Cargo / cargo-zigbuild / Zig、GitHub Actions / CLI。

## 已确认设计和全局约束

- `--platform linux-amd64` 使用 `cargo zigbuild --release --target x86_64-unknown-linux-musl`。
- `--platform linux-arm64` 使用 `cargo zigbuild --release --target aarch64-unknown-linux-musl`。
- `--platform darwin-arm64` 使用 `cargo build --release --target aarch64-apple-darwin`，在 Apple Silicon macOS 构建。
- 每次只生成一个平台；无参数或未知参数必须失败且不启动编译；无 `all` 目标。
- 三种产物名 `kafka-dashboard-<版本>-<platform>.tar.gz` 及对应 `.tar.gz.sha256`，每包内只含对应二进制、前端、启动器、配置模板、说明和完整文件校验清单。版本采用 `v` 前缀，来自 backend/Cargo.toml；发布标签必须匹配。
- 半离线指目标机器不需下载编译依赖、Rust、Node、librdkafka、OpenSSL；Kafka / 飞书网络按功能仍需可达。Linux musl 静态链接；macOS 仅系统库动态依赖。
- 构建失败不得损坏已有包；生成目录隔离按版本和平台命名，不覆盖其他架构、不触碰真实运行数据。保留清单校验、安全文件白名单和启动器的锁、完整进程身份、SIGTERM、有界等待语义。
- GitHub Release 首先是草稿，三个平台各自构建/运行验证后汇总六个附件；上传并重新下载比对本地字节、SHA-256 校验、确认附件列表完整后才取消草稿；失败保持草稿并非零退出，不得输出发布成功。
- 同一标签 checkout、版本检查、提交标识记录和构建产物之间的关联必须可验证；不把已发布 Release 强制改写，不自动覆盖未知附件，不自动创建或移动标签。
- 不改业务代码，不连接真实 Kafka；不实际执行 GitHub 写操作。文档说明外部工作流/手工发布绕过门禁无法由本仓库脚本禁止，但不算合规交付。

### 任务 1：单平台构建、启动与运行验证

**文件：** tools/build-release.sh、tools/verify-release.sh、packaging/start.sh、tests/release_scripts.test.mjs；必要时新增聚焦构建测试/辅助脚本。删除或替换不再使用的旧 Docker 构建入口。README.md、packaging/README.txt 与 AGENTS.md 的打包章节随实现更新。

- [ ] 先执行现有脚本测试建立基线。添加行为测试后确认 RED：缺少平台拒绝；三种平台只编译正确 target 且只含一份二进制；同版本不同架构共存；归档 checksum 与真实字节匹配；构建/归档/发布失败保留旧产物；Linux ARM64 启动身份检查沿用 Linux 安全路径；不支持平台依然拒绝。
- [ ] 构建解析 `--platform <值>` 和可选 `--version vX.Y.Z`（必须等于 Cargo 包版本），保留 `--locked -p backend --bin kafka-dashboard`。使用独立 target-dir 或明确 target-dir 保证复制的是本次构建产物。一个独占锁防止同 worktree npm/dist 竞争；错误早失败。编译器是昂贵外部边界可 fake，但 tar/hash/文件系统必须跑真实实现。
- [ ] 按选定平台改造 verify-release：只接受单二进制包，验证清单、静态边界、架构；macOS 本机执行、Linux Docker 指定平台执行。临时副本中创建 .env，通过 start/status/stop 验证版本、健康、前端、SPA、SQLite 与退出；拒绝错误 PID、错误状态、错误版本和额外敏感文件。验证不写原包、不遗留进程；保留相关旧回归覆盖，不为了通过删掉安全测试。
- [ ] GREEN：`node --test tests/release_scripts.test.mjs` 及新增聚焦测试；`sh -n packaging/start.sh tools/build-release.sh tools/verify-release.sh`；`git diff --check`。真实三平台构建由控制者在脚本稳定后串行执行，报告限制。
- [ ] 写报告包含 RED/GREEN、文件清单与实际命令；不要提交。明确导出的路径和给任务 2 的构建版本/来源接口。

### 任务 2：六附件发布门禁与 Actions

**文件：** 新增 tools/publish-release.mjs、tests/publish_release.test.mjs、.github/workflows/release.yml；更新 README.md、AGENTS.md 的发布约定。

- [ ] 先写可执行发布脚本测试；通过临时本地 Git tag、真实 tar/hash、外部 gh 假服务测试。RED 覆盖缺一附件、损坏 checksum、错误标签/来源/架构、下载内容被篡改、上传失败、已正式发布不能覆盖；每个失败都不能执行取消草稿。成功必须产生上传六件、下载六件、核验、最后取消草稿的顺序。无真实 GitHub 写操作。
- [ ] 实现 `node tools/publish-release.mjs <tag> <artifacts-dir>`：严格检查语义版本、tag 与当前 checkout/Cargo 版本一致，以及任务 1 包内来源记录；本地六附件验证成功后才调用 gh。安全调用 spawn 参数数组，拒绝路径穿越/奇异文件名；临时下载目录由 mkdtemp 创建并清理。允许仅重试同标签草稿且保守处理现有附件；不覆写已发布版本。取消草稿前重新核对标签仍对应提交、远端六件名称/大小和下载字节，取消草稿后核对 isDraft/tag/附件才报告成功。
- [ ] Actions 由 `v*` tag push 和可选 workflow_dispatch 的现有标签触发；checkout 精确 tag，三个独立 matrix jobs 调用单平台脚本。Linux 使用 cargo-zigbuild，macOS arm64 runner 原生构建；固定工具版本、安装所需 target 和原生构建依赖。各自执行包验证后只上传归档及 sidecar；汇总 job 依赖全部构建成功，精确下载本次 run 六个附件，调用发布脚本。最小权限（构建 contents:read，发布 contents:write）、同标签 concurrency 不取消在途发布。正式发布前不声称完成。
- [ ] GREEN：新发布测试、既有脚本测试、workflow YAML 语法/结构校验（工具可用则 actionlint）；文档写出三条构建命令、对应验证入口、六附件规则、重试方法和半离线网络边界。
- [ ] 写报告并保持未提交、不发布；控制者执行最终独立审查及实包验证。
