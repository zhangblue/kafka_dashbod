# Kafka Lens：代理开发约定

所有命令默认从仓库根目录执行。先阅读 `README.md` 和与任务相关的源码、测试；设计与计划位于 `docs/superpowers/`。历史设计和实现计划保留为决策记录，其中旧描述不作为当前操作指南。保留用户已有变更，不把无关改动纳入提交。

## 架构与职责

- `backend/src/domain/`：Offset、Lag、配置和快照的领域类型与纯计算；未知值必须显式表达，不能降为 0。
- `backend/src/api/`：Axum `/api` JSON 接口、校验与统一安全错误；`static_assets/` 负责生产静态资源。API 路径（包括未知 API）不得回退为 HTML，SPA fallback 只适用于允许的页面请求；保留静态文件路径、符号链接与目录能力的安全边界。
- `backend/src/db/`、`backend/migrations/`：SQLite 配置仓储与迁移。只持久化集群配置、明文 Kafka 凭据及 Lag 规则，不存储指标历史。旧加密数据库由用户停服后手动删除并重新配置，不迁移、转换或自动删除。
- `backend/src/kafka/`：librdkafka 有界采集及受控 Topic Admin 适配。仅允许创建/删除 Topic、读取配置和增量修改 8 个白名单参数；继续禁止生产消息、调用删除消息 API、提交/重置 Offset、分区扩容及副本重分配。删除保护 `__` 内部 Topic，必须严格匹配完整确认名称。
- `backend/src/sessions/`：按需客户端和 15 秒短租约。快照续租，同一会话采集互斥；5 秒清理过期会话。关闭、删除集群、取消任务及进程退出必须等待在途创建/采集/释放，不能把阻塞原生工作遗留到后台。
- `frontend/src/api/`：唯一 HTTP 客户端、共享类型；`frontend/src/features/`：设置、概览、Topics、Consumer Groups、Lag 规则、会话及内存历史；`frontend/src/components/`：公共展示组件。
- `backend/tests/`、前端 `*.test.ts(x)`：领域、仓储、API、进程、交互测试；`frontend/e2e/` 与 `scripts/`：一次性 Kafka + 浏览器验收；`tests/`：发行脚本回归。
- `packaging/`：启动器和发行模板；`tools/`：唯一构建/验证入口；`dist/`：生成的双平台发行包。

## 已确认的页面风格

用户于 2026-10-15 再次确认昨晚的原始 Demo，后续所有页面均以 `docs/ui/approved-dashboard-demo.html` 为统一视觉基准。该文件是原稿存档；本地来源为 `.superpowers/brainstorm/78111-1791991385/content/kafka-dashboard-demo.html`。概览、Topics、Consumer Groups、集群设置、Lag 规则、弹窗及新增页面都应遵循同一套布局、字体、间距、配色、边框和控件样式。

- 桌面布局保留 224px 左侧导航和 70px 顶部栏；集群选择、连接状态、更新时间、刷新频率及刷新按钮位于同一条顶部栏。
- 概览保留四张紧凑指标卡、左右排列的 Lag 趋势与 Lag 分布面板，以及下方需要关注的消费组表格；按原稿核对比例、间距和信息密度。
- 使用原稿的深色背景、青蓝强调色、绿/黄/红状态标签、紧凑表格、字段提示和配置弹窗样式；仅换配色不构成还原。
- 验收时在相同视口下对照原稿与正式页面；分别检查桌面与窄屏，不能把窄屏布局当作桌面设计。
- 原稿中的模拟数据、旧认证文案及早期交互只作历史展示，不覆盖当前行为约束。正式页面仍使用真实数据、明文配置存储、已确认的 Group/Topic 下拉选择和持续超限规则；不得恢复已移除的规则优先级、实时判定预览、表格显示说明及 Flink 建议板块。

## 行为约束

保存配置不保持 Kafka 连接。只有打开仪表盘或需要采样的规则页面才创建会话；页面隐藏停止轮询，离开页面、切换或删除集群主动释放，短租约兜底。历史仅在当前页面内存中保留一小时；刷新间隔仅 3/5/10 秒。

Topic 管理必须绑定当前有效 session，复用其 AdminClient 并与快照共享互斥。关闭/取消/删除集群/退出须等待所有已接受的管理任务及原生析构。配置仅实时读取，不持久化副本，不用示例值替代当前值。白名单为 `retention.ms`、`retention.bytes`、`cleanup.policy`、`segment.ms`、`segment.bytes`、`min.insync.replicas`、`max.message.bytes`、`compression.type`；增量 SET 修改指定值，DELETE 删除 Topic 覆盖以恢复 Broker 默认，禁止 legacy 整体替换。保留来源、未知/只读状态、保存前冲突检查及已应用但未确认的 202 语义；Kafka 不提供本流程的原子比较写入。

Low 是保留起点，High 是下一写入位置，非空分区最大消息 Offset 为 High - 1，范围估算为 High - Low，不能称为精确消息数。Lag 为 max(High - committed, 0)；未提交、未知、部分失败与陈旧数据必须保留区别。自定义规则按 Group + Topic 覆盖默认；持续超限计时仅累积连续可见有效采样，隐藏、断开、换会话或规则变化时重置。

## 安全与生成文件

SQLite 含明文 Kafka 凭据，须限制数据库、sidecar 和备份的访问权限。API 只返回凭据配置标志，API/日志仍不得回显秘密。不要把原生 Kafka 错误、请求体、连接密码、PEM 或数据库内容写入日志、截图、测试附件或错误响应。保留 256 KiB 请求上限、Request ID 和脱敏错误。

服务没有内置认证；默认只监听回环地址，部署需要受控内网或带认证的 HTTPS 反向代理，对全部 API 施加访问控制。Kafka 管理权限按 Topic 范围最小授予；删除不可恢复，降低保留值或恢复默认可能导致已有数据清理。只使用明确授权的一次性 broker 运行写入 fixture；fixture 精确要求 `127.0.0.1:<非零端口>` 和 `E2E_KAFKA_FIXTURE_PERMIT=disposable-compose`，管理验收仅用随机唯一 Topic，清理 guard 只能删除本次成功创建资源，禁止删除 seed 或已有集群资源。

禁止提交 `dist/`、`frontend/dist/`、`target/`、`node_modules/`、SQLite 文件及 sidecar、`.env`、私钥、真实证书、日志、PID 文件、启动器锁目录、Playwright 报告/trace。`packaging/.env.example` 只含占位符，可以提交。锁文件 `Cargo.lock`、`frontend/package-lock.json` 必须保留。生成产物从源码重建，不手改发布文件；`.superpowers/` 是本地工作记录，不属于交付源码。

## 开发与验证

后端使用 `cargo run -p backend --bin kafka-dashboard` 启动，正常启动必须读取当前工作目录中可读且格式有效的 `.env`；可先执行 `cp packaging/.env.example .env`，源码开发时按需调整或移除发行静态目录配置 `KAFKA_DASHBOARD_WEB_DIR=web`，并调整数据库路径或创建模板所需的可写 `data/` 目录。父进程已设置的同名环境变量优先于 `.env`；`--version` 查询例外，无需 `.env`。前端使用 `npm --prefix frontend ci`、`npm --prefix frontend run dev -- --host 127.0.0.1`。生产静态目录由 `KAFKA_DASHBOARD_WEB_DIR` 配置；开发代理地址用 `KAFKA_DASHBOARD_API_TARGET`。

`packaging/start.sh` 无参数或 `start` 必须后台启动，固定使用发行目录下的 `data/kafka-dashboard.pid` 和追加写入、不自动轮转的 `data/kafka-dashboard.log`。管理操作须持锁，并安全校验 PID 与完整进程身份，不得向无关进程发送信号。`status` 明确输出“Kafka Lens 正在运行，PID: …”或“Kafka Lens 未运行”，两种业务状态均退出 0，管理错误退出非零；重复启动须退出非零并输出 `Kafka Lens 已在运行，PID: <pid>`。`stop` 仅发送 SIGTERM 并有界等待，超时保留 PID 文件，不得自动 SIGKILL；`restart` 仅在停止成功后启动。正常启动仍由 Rust 强制读取发行目录 `.env`，启动器不解析、设置或导出配置，不透传运行参数；`--version` 前台执行且无需 `.env`。发行验证通过 `start/status/stop` 管理真实程序，运行生成的 `data/` 不进入发行归档；生产日志轮转由系统配置或服务管理器负责。

```bash
sh -n packaging/start.sh tools/verify-release.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
npm --prefix frontend run test -- --run
npm --prefix frontend run build
node --test scripts/e2e-preview.test.mjs
node --test tests/release_scripts.test.mjs
npm --prefix frontend run test:e2e
./tools/build-release.sh
./tools/verify-release.sh dist/kafka-dashboard
git diff --check
```

功能和缺陷修改遵循 TDD：先写有意义的失败测试，确认失败原因，再做最小实现并重跑相关回归。纯文档修改无需虚构测试。单元测试不依赖真实 Kafka；broker 测试默认 ignored，由明确的一次性环境执行。E2E 启动/清理由脚本统一管理，不复用未知健康服务。打包及双平台验证需要 Apple Silicon macOS 和可运行 Linux/amd64 的 Docker；不要并发打包。

提交前自审差异、运行与影响范围相称的验证和秘密扫描，记录实际结果与限制，禁止把未运行、被跳过或失败的检查报告为通过。提交应保持单一目的并包含所需源码、测试和文档；不要自动推送、强推或覆盖远端历史，遵循当前用户授权的交付流程。

## Context7 文档流程

涉及库、框架、SDK、API、CLI 工具或云服务的语法、配置、版本迁移、专属调试及安装问题时，必须查询 Context7 当前文档，即使熟悉该技术也不凭记忆回答。优先于网页搜索。纯重构、从头编写普通脚本、业务逻辑调试、代码审查及通用编程概念不触发此要求。

1. 首先调用 `resolve-library-id`，传入库名和要查的问题；仅当用户已给出准确 `/org/project` ID 时可以跳过解析。
2. 按精确名称、描述相关性、代码片段数量、来源信誉（优先官方及 High/Medium）和 benchmark score 选择匹配项。结果不合适时更换名称或问题表述；用户指定版本时使用对应版本 ID。
3. 使用所选 ID 调用 `query-docs`，每次围绕单一概念提出具体问题。路由、认证、缓存等独立概念分别查询；只有研究它们之间的交互时才组合查询。
4. 根据实际取回的文档实现或回答，必要时标明版本与来源。禁止向文档查询发送秘密、个人数据或专有源码。
