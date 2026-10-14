# Kafka Dashboard 设计规格

## 1. 目标

构建一个前后端分离的只读 Kafka 监控仪表盘。用户可以配置多个 Kafka 集群，并在每个集群的独立仪表盘中查看 Topic Offset、Consumer Group 已提交 Offset、Lag 当前值及最近一小时趋势。

第一版面向单用户或内网共享部署，不提供账号和权限系统。系统只在用户打开某个集群仪表盘时连接该 Kafka 集群；页面不再使用时及时断开，不在服务端持续采集或保存监控历史。

## 2. 范围

### 2.1 第一版包含

- 多 Kafka 集群配置、编辑、删除和连接测试。
- `PLAINTEXT`、TLS、`SASL/PLAIN`、`SASL/SCRAM` 连接方式。
- 集群概览、Topic 列表、Consumer Group 列表与详情。
- Topic 汇总及逐 Partition low/high watermark。
- 所有消费组，包括当前没有活跃成员但仍保留已提交 Offset 的消费组。
- Consumer Group 在 Topic/Partition 维度的 committed offset 和 Lag。
- 可配置的 Lag 状态默认区间，以及按 Consumer Group + Topic 精确匹配的自定义区间。
- 可配置的持续超限时间，用于过滤 Flink checkpoint 等场景造成的短时 Lag 波动。
- 可选择的 3 秒、5 秒、10 秒刷新频率。
- 浏览器内最近一小时 Lag 折线图。
- 短租约监控会话，仅在仪表盘使用期间维持 Kafka 连接。
- 集群认证信息加密持久化至 SQLite。
- 单目录跨平台发行包，同时包含 macOS ARM64 与 Linux x86_64 可执行文件、前端静态资源和自动选平台启动脚本。

### 2.2 第一版不包含

- 消息内容浏览、搜索或生产消息。
- Topic 创建、修改或删除。
- Consumer Group Offset 重置。
- 用户登录、角色和权限管理。
- 服务端历史指标存储与跨浏览器历史恢复。
- AWS MSK IAM、Confluent Cloud OAuth 等云厂商专用认证方式。

## 3. 技术架构

### 3.1 前端

- React + TypeScript + Vite。
- TanStack Query 管理服务端状态、动态轮询和页面可见性行为。
- Apache ECharts 展示 Lag 趋势。
- 浏览器内存环形缓冲保存当前页面运行期间最近一小时采样；刷新页面后历史清空。

### 3.2 后端

- Rust + Axum 提供 REST API。
- rust-rdkafka 连接 Kafka，并获取 metadata、partition watermarks、consumer groups 和 committed offsets。
- SQLx + SQLite 持久化集群配置。
- 监控会话管理器负责创建、复用、续租和释放 Kafka 客户端。

### 3.3 部署边界

开发环境分别启动前端和后端。生产发行包由一个 Rust 进程同时提供 `/api` 和已构建的前端静态资源；除 `/api` 外的未知 GET 路径回退到 `index.html`，支持前端路由直接访问。后端默认仅监听本机地址。内网暴露时由部署方通过反向代理提供 HTTPS 和访问控制。

发行包同时携带 macOS ARM64 与 Linux x86_64 二进制，目标机器不需要安装 Node.js、Rust 或系统版 librdkafka。Linux 二进制以 `x86_64-unknown-linux-musl` 为目标构建，并从源码静态链接 librdkafka 与 vendored OpenSSL。第一版只支持 SASL/PLAIN 和 SASL/SCRAM，不包含需要额外 Cyrus SASL/GSSAPI 运行库的 Kerberos 认证。

## 4. Kafka 连接生命周期

1. 用户进入某个集群的仪表盘，前端调用创建会话 API。
2. 后端解密目标集群认证配置，创建 rust-rdkafka 客户端，并返回随机会话 ID。
3. 前端按 3、5 或 10 秒间隔请求快照；每次成功到达的快照请求同时更新会话租约。
4. 同一浏览器页面同一时刻只监控当前选中的集群。切换集群前先删除旧会话。
5. 页面正常离开时发送关闭会话请求。页面异常关闭、断网或请求丢失时，后端在约 15 秒无续租后自动释放 Kafka 客户端。
6. 页面进入后台时暂停轮询；重新可见时立即采集一次。暂停超过租约后，恢复页面时透明地创建新会话。
7. 应用优雅退出时关闭全部活动会话。

会话只存在于后端内存，不在 SQLite 中持久化。每个会话绑定单一集群，并具有独立的并发控制，避免重叠采集。

## 5. 数据模型

### 5.1 ClusterConfig

- `id`
- `name`
- `bootstrap_servers`
- `security_protocol`
- `sasl_mechanism`
- `username`
- `encrypted_password`
- CA、客户端证书及其加密口令配置
- 连接超时
- 默认正常 Lag 上限，初始值为 `999`
- 默认高 Lag 起始值，初始值为 `5000`
- 默认持续超限时间，初始值为 `30` 秒
- 创建和更新时间

敏感值写入数据库前使用环境变量 `KAFKA_DASHBOARD_MASTER_KEY` 提供的主密钥，通过带随机 nonce 的 AEAD 算法加密。读取配置的 API 不返回密码或私钥明文；编辑时空密码表示保留原值。

### 5.2 LagStatusRule

- `id`
- `cluster_id`
- `consumer_group`
- `topic`
- 正常 Lag 上限
- 高 Lag 起始值
- 持续超限时间（秒）
- 创建和更新时间

同一集群内的 Consumer Group + Topic 组合必须唯一。规则精确匹配时覆盖集群的三个默认值；没有匹配规则时回退到集群默认值。正常上限必须大于或等于 `0`，高 Lag 起始值必须大于正常上限，持续超限时间必须大于或等于 `0`。

### 5.3 MonitorSession

- 随机会话 ID
- 集群 ID
- Kafka 客户端句柄
- 创建时间
- 最后续租时间
- 当前采集锁或等价的防重入状态

### 5.4 ClusterSnapshot

- 服务端采集时间
- 集群基本状态
- Topic 和 Partition watermarks
- Consumer Group 状态
- Group/Topic/Partition committed offsets 与 Lag
- 部分采集失败列表

## 6. Offset 与 Lag 语义

- 分区最小 Offset 使用 Kafka low watermark。
- Kafka high watermark 表示下一条消息的位置。页面中的“最大消息 Offset”显示 `high - 1`；空分区显示 `—`。
- Partition Lag 计算为 `max(high - committed_offset, 0)`。
- 从未提交 Offset 的分区显示“未提交”，Lag 显示 `—`，不当作 0。
- Topic 汇总行显示 Partition 数、所有分区 low watermark 的最小值、所有非空分区最大消息 Offset 的最大值，以及 `sum(high - low)` 得到的保留消息数估算。Offset 本质上只在 Partition 内有意义，因此汇总值用于概览，展开后的逐 Partition 值才是权威数据。
- Consumer Group + Topic 汇总 Lag 为具有有效 committed offset 的 Partition Lag 之和；同时显示未提交 Partition 数。展开后展示逐 Partition committed offset、high watermark、最大消息 Offset 和 Lag。

### 6.1 Lag 状态判定

对没有采集错误且所有 Partition 均存在 committed offset 的 Consumer Group + Topic，按下列区间判定：

- 正常：`0 <= total_lag <= normal_lag_max`，集群默认范围为 `0–999`。
- 积压：`normal_lag_max < total_lag < high_lag_min`，集群默认范围为 `1000–4999`。
- 高 Lag：`total_lag >= high_lag_min`，集群默认范围为 `>= 5000`。

状态优先级为：采集异常、未提交、高 Lag、积压、正常。存在采集错误时立即显示“采集异常”；至少一个 Partition 不存在 committed offset 时立即显示“未提交”，并将数值展示为“已知 Lag + 未知 Partition 数”。这两种状态不受持续超限时间影响。

Lag 首次超过正常上限时开始计时。在连续超限时间达到当前生效规则的设定值前，仍按正常波动显示；达到后，根据当前 Lag 落点显示“积压”或“高 Lag”。Lag 回到正常范围时立即清除计时。积压与高 Lag 共用一次连续超限计时，在二者区间内变化不会重新开始计时。

连续超限计时属于当前浏览器监控会话，只在页面可见且持续采样时推进。页面隐藏、刷新、切换集群或会话重建后计时重新开始，不把未采集的后台时间算入持续时间。

## 7. API 设计

### 7.1 集群配置

- `GET /api/clusters`：列出脱敏后的集群配置。
- `POST /api/clusters`：新增集群。
- `PUT /api/clusters/:id`：更新集群。
- `DELETE /api/clusters/:id`：删除集群；删除前终止该集群的活动会话。
- `POST /api/clusters/:id/test`：临时连接并测试认证，完成后立即释放连接。
- `GET /api/clusters/:id/lag-rules`：读取集群默认 Lag 状态设置和自定义规则。
- `PUT /api/clusters/:id/lag-rules`：原子更新集群默认 Lag 状态设置和全部自定义规则。

### 7.2 监控会话

- `POST /api/clusters/:id/sessions`：创建监控会话。
- `GET /api/sessions/:id/snapshot`：采集完整快照并续租。
- `DELETE /api/sessions/:id`：主动关闭会话。
- `GET /api/health`：服务存活检查。

错误响应使用统一结构，包含稳定错误码、用户可读消息、可选字段级详情和 request ID。API 区分配置错误、认证失败、DNS/网络失败、TLS 失败、Kafka 超时、会话过期和内部错误。

## 8. 采集策略

- 单轮快照获取 Topic metadata 和各 Partition watermarks，再读取全部 Consumer Group 及其 committed offsets。
- 对 Kafka 操作设置明确超时，并使用有上限的并发，避免大集群造成无界任务或请求堆积。
- 同一会话不允许两轮快照重叠；上一轮仍在执行时，后续请求返回明确的“采集中”结果或共享进行中的结果。
- 单个 Topic、Partition 或消费组失败不使整个快照失败。响应标记为部分成功，包含失败项及原因。
- 前端对部分失败项保留上一轮值并标记为陈旧；集群整体不可达时保留最后一次快照并显示断开状态。

## 9. 页面与交互

### 9.1 全局布局

采用“总览 + 分析工作台”布局。左侧导航包含“概览”“Topics”“Consumer Groups”“集群设置”。顶部栏包含当前集群选择器、连接状态、最后更新时间、3/5/10 秒刷新频率和手动刷新。

### 9.2 概览页

- Topic 数、消费组数、总 Lag、异常消费组数指标卡片。
- 最近一小时总 Lag 折线图。
- 按 Lag 降序展示异常消费组，支持跳转详情。

“异常消费组”是当前状态为“采集异常”“未提交”“积压”或“高 Lag”的消费组。“正常”状态允许处于配置范围内的非零 Lag。

### 9.3 Topics 页

- 按名称搜索 Topic。
- Topic 汇总行展示 Partition 数、汇总最小/最大 Offset、保留消息数估算。
- 展开后展示逐 Partition low watermark、high watermark 和最大消息 Offset。
- Topic 表格在相关字段旁提供说明提示，解释 low watermark、high watermark、最大消息 Offset 和保留消息数估算的定义、计算方式及边界情况。

### 9.4 Consumer Groups 页

- 按消费组名称和 Topic 搜索。
- 默认按总 Lag 降序。
- 汇总行展示消费组状态、Topic、已提交 Partition 数、未提交 Partition 数和总 Lag。
- 展开后展示每个 Partition 的 committed offset、high watermark、最大消息 Offset 和 Lag。
- 状态、已提交/未提交、总 Lag 和趋势字段均提供说明提示。趋势只表示相较上一采样点的方向，不参与状态判定。
- 命中 Consumer Group + Topic 自定义规则的行显示“自定义规则”标记；悬停标记可查看实际采用的正常上限、高 Lag 起始值和持续超限时间。使用集群默认规则的行不显示该标记。

### 9.5 消费组详情

- 选择一个或多个 Topic/Partition 查看 Lag 折线图。
- 每次快照按服务端采集时间写入环形缓冲，并持续淘汰一小时前的采样。
- 改变刷新频率不清空已有历史。

### 9.6 集群设置

- 新增、编辑、删除集群。
- 保存前可测试连接。
- 密码和私钥不回显；编辑时允许保持原敏感字段。
- “Lag 状态判断区间”提供集群默认正常上限、高 Lag 起始值和持续超限时间。
- “自定义 Lag 状态判定区间”允许新增和删除 Consumer Group + Topic 精确覆盖规则。
- Consumer Group 必须从当前集群已发现的消费组下拉列表选择。选定 Consumer Group 后，Topic 下拉列表只显示该消费组存在 committed offset 的 Topic，不允许自由输入。
- 打开规则配置时复用当前集群监控会话加载下拉选项；没有活动会话时创建短租约会话，离开配置页后释放。集群不可达时保留已有规则只读展示，并要求重新连接后才能新增或更换 Consumer Group/Topic。
- 已保存规则引用的 Consumer Group 或 Topic 暂时不存在时保留该规则，并将对应选项标记为“当前不可用”；用户可以删除规则，但不能用任意文本替换名称。

## 10. 状态与错误体验

- 首次采集显示骨架屏。
- 普通刷新保留已有内容，避免页面闪烁。
- 行级失败显示警告和错误摘要；悬停或展开查看详细原因。
- Lag 状态区间、持续超限计时和自定义规则标记遵循第 6.1 节定义。
- 数据超过两倍当前刷新周期仍未更新时标记为陈旧。
- 会话过期时前端自动创建新会话并重试一次；持续失败后停止自动重试并提供手动重连。
- 后端日志包含 request ID、集群 ID 和操作类型，但不得记录密码、私钥或完整连接参数。

## 11. 安全约束

- 使用 AEAD 加密敏感配置，每条密文使用独立随机 nonce。
- 主密钥只从环境变量读取，不写入数据库、配置文件或日志。
- 限制请求体大小、快照并发数和 Kafka 操作超时。
- 对展示文本进行正常的 React 转义，不渲染来自 Kafka 元数据的任意 HTML。
- 默认本机监听；远程访问需要部署方配置 HTTPS 和访问控制。

## 12. 测试策略

### 12.1 后端

- 单元测试：配置校验、加密与解密、Lag 计算、空分区、未提交 Offset、默认与自定义规则选择、租约续期与过期、错误映射。
- API 测试：集群 CRUD、Lag 规则唯一性与边界校验、敏感字段脱敏、会话创建/续租/删除、会话过期恢复、部分成功响应。
- Kafka 集成测试：使用测试容器验证 PLAINTEXT、Topic watermarks、消费组 committed offsets 和 Lag。TLS/SASL 配置转换通过单元测试覆盖，并在可用环境中保留可选集成测试。

### 12.2 前端

- 单元测试：最近一小时环形缓冲、Lag 汇总、状态区间匹配、持续超限计时与重置、刷新频率切换、陈旧状态判断。
- 组件测试：Topic 展开表格、字段说明提示、消费组筛选与排序、自定义规则标记、规则下拉联动与不可用选项、错误和空状态、图表序列选择。
- 行为测试：页面隐藏时暂停、重新可见时恢复、切换集群和卸载页面时释放会话。

### 12.3 端到端

覆盖新增集群、测试连接、进入仪表盘、查看 Topic 和消费组、切换刷新频率、查看趋势、切换集群以及退出后释放会话。

### 12.4 发行包

- 自动测试启动脚本的平台识别、环境变量检查、参数透传和不支持平台报错。
- 使用 `file` 检查两个后端二进制的目标架构。
- 在 macOS ARM64 主机构建并启动本机二进制，验证健康检查与 SPA 回退。
- 在 Docker 的 Linux/amd64 环境启动 Linux 二进制，验证健康检查、静态资源、SQLite 写入与优雅退出。
- 校验发行目录不存在源码、明文密钥、开发依赖和绝对构建路径，并验证 SHA-256 校验文件。

## 13. 发行与运行

### 13.1 发行目录

`tools/build-release.sh` 生成下列单一目录，并另外生成便于传输的压缩包：

```text
dist/kafka-dashboard/
├── bin/
│   ├── kafka-dashboard-darwin-arm64
│   └── kafka-dashboard-linux-amd64
├── web/
├── start.sh
├── .env.example
├── README.txt
└── SHA256SUMS
```

`web/` 只保存 Vite 生产构建产物。`start.sh` 根据 `uname -s` 与 `uname -m` 选择对应二进制，支持 Darwin/arm64 和 Linux/x86_64；其他组合以非零状态退出并显示支持列表。脚本从当前发行目录定位资源，因此整个目录可以移动。命令行参数原样传给后端。

### 13.2 构建流程

构建入口只有 `tools/build-release.sh`。脚本在 macOS ARM 主机上检查 Node.js/npm、Rust 和 Docker Buildx，使用锁文件安装依赖并构建前端；本机构建 macOS ARM64 后端；再通过 Docker Buildx 构建 Linux/amd64 musl 后端并用 local exporter 取回文件。脚本从空的临时暂存目录组装发行内容，最后生成校验和与压缩包，任一步失败都不发布不完整目录。

前后端依赖锁文件必须提交。`dist/`、运行期 SQLite 数据和本地 `.env` 不提交到 Git。

### 13.3 运行配置

启动时必须通过环境变量提供 `KAFKA_DASHBOARD_MASTER_KEY`；发行包只包含不带秘密的 `.env.example`。默认将 SQLite 文件写到发行目录下的 `data/`，也允许用环境变量覆盖监听地址、数据库 URL 和日志级别。启动脚本创建缺失的 `data/`，但不会生成、保存或打印主密钥。

## 14. 项目文档

根目录 `README.md` 面向使用者和开发者，包含功能、限制、快速启动、环境变量、Kafka 认证字段、Offset/Lag 语义、源码开发、测试、发行构建、安全部署和故障排查。发行目录的 `README.txt` 是离线精简运行指南。

根目录 `AGENTS.md` 面向后续编码代理，记录架构边界、常用命令、TDD 与验证要求、敏感信息规则、生成目录约定，以及涉及第三方库/API/CLI 时必须通过 Context7 查询当前文档的要求。

## 15. GitHub 交付

本地仓库将 `origin` 设置为 `https://github.com/zhangblue/kafka_dashbod.git`。在完整测试、发行包验证和秘密扫描通过后，把源码、锁文件、设计、计划、README 与 AGENTS 提交并推送到 `main`。构建产物、数据库和本地秘密不推送；本次交付不额外创建 Pull Request 或 GitHub Release。

## 16. 验收标准

- 可以保存和切换多个 Kafka 集群，并支持约定的 PLAINTEXT、TLS、SASL/PLAIN、SASL/SCRAM 配置。
- 页面只在仪表盘可见并使用时维持 Kafka 连接；关闭、切换或租约到期后释放连接。
- 3 秒、5 秒、10 秒刷新频率均可工作，且不会产生重叠采集。
- 所有 Topic 均可搜索、汇总和展开查看 Partition Offset。
- 所有仍可由 Kafka 查询到的消费组均可查看 committed offset 与 Lag，包括无活跃成员的组。
- Lag 与最大消息 Offset 符合本文定义，空分区和未提交 Offset 展示明确。
- 集群默认 Lag 区间可配置；Consumer Group + Topic 自定义规则可以覆盖默认区间与持续超限时间，并在 Consumer Groups 表格中明确标记。
- Lag 只有持续超过正常范围达到生效规则的设定时间后才显示“积压”或“高 Lag”，回到正常范围后计时重置。
- Lag 规则中的 Consumer Group 和 Topic 只能从当前集群发现的数据中选择，不能手工输入。
- 最近一小时趋势只保存在浏览器内存，连续运行超过一小时后不会保留超期样本。
- Kafka 局部错误不会清空健康数据，界面能区分部分失败、断开和陈旧状态。
- 敏感认证字段不会通过读取 API 或日志泄露，数据库中不保存明文密码。
- 一个构建脚本可以生成规定结构的双平台单目录发行包和校验和；macOS ARM64 与 Linux x86_64 的运行验证均通过。
- 发行目录复制到对应目标机后无需安装 Node.js、Rust、librdkafka 或 OpenSSL，只需配置主密钥即可启动前后端一体服务。
- 根目录包含可独立指导安装、开发、测试、打包和运维的 `README.md`，并包含约束后续代理工作的 `AGENTS.md`。
- GitHub `main` 分支包含已验证的完整项目，且不包含发行产物、数据库或秘密。
