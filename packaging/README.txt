Kafka Lens — 离线发行说明

一、支持平台

支持 macOS Apple Silicon（Darwin/arm64）和 Linux x86-64（Linux/x86_64）。
将整个 kafka-dashboard 目录复制或解压到可写位置，并进入该目录。
运行机器无需安装 Node.js、Rust、系统 librdkafka 或系统 OpenSSL。
Linux 使用 musl，静态链接 librdkafka、SQLite 和 vendored OpenSSL。
macOS 依赖 libSystem 等系统原生库，并非完全静态；执行许可遵循本机
Gatekeeper 策略。运行时仍需可用的网络、DNS 和可写存储。

二、校验发行文件

首次使用前在发行目录执行：
  shasum -a 256 -c SHA256SUMS
Linux 也可执行：
  sha256sum -c SHA256SUMS
所有条目应校验通过；失败时重新获取或复制完整发行包。

三、创建启动配置

在发行目录执行：
  cp .env.example .env
按需编辑 .env，模板中的四项含义如下：
  KAFKA_DASHBOARD_BIND=127.0.0.1:3000
    HTTP 监听地址，默认仅本机可访问。修改端口后同步调整访问地址。
  DATABASE_URL=sqlite://data/kafka-dashboard.db?mode=rwc
    SQLite 配置数据库 URL，mode=rwc 允许读写并在不存在时创建数据库。
    自定义路径的父目录必须已存在且可写。
  KAFKA_DASHBOARD_WEB_DIR=web
    前端静态资源目录，必须包含 index.html。
  RUST_LOG=info
    日志级别；排查时也不得记录敏感配置。
启动器会切换到发行目录，再创建私有权限的 data/；上述相对路径均以
发行目录为基准，整体移动目录后仍可使用。

四、启动与停止

在发行目录执行：
  ./start.sh                 后台启动（等同于 ./start.sh start）
  ./start.sh status          输出“Kafka Lens 正在运行，PID: …”或“Kafka Lens 未运行”
  ./start.sh stop            发送 SIGTERM 并等待正常退出
  ./start.sh restart         正常停止后重新启动
  ./start.sh --version       前台显示版本，不要求 .env

PID：data/kafka-dashboard.pid
日志：data/kafka-dashboard.log（追加写入，不自动轮转）
这些运行文件在首次启动后创建，不包含在发行归档中。

浏览器访问：http://127.0.0.1:3000
健康检查：http://127.0.0.1:3000/api/health
修改监听地址后请使用对应地址。status 通过上述文本表达业务状态，
运行中与未运行均以退出码 0 结束；PID 无效、身份不匹配等管理错误以
非零退出码结束。重复启动会被拒绝，返回非零退出码并明确输出
“Kafka Lens 已在运行，PID: <pid>”。启动失败时先查看上述日志。

stop 只发送 SIGTERM，有界等待程序完成在途 Kafka 工作及会话释放；
超时返回非零退出码并保留 PID 文件，不会自动发送 SIGKILL。
restart 仅在 stop 成功后重新启动。生产部署应配置系统日志轮转，
或使用服务管理器管理进程与日志。

Rust 程序正常启动时强制读取当前工作目录的 .env。文件缺少、不可读或
格式错误都会导致启动失败；即使已在外部设置全部配置，也必须有有效
的 .env。父进程已经设置的同名环境变量优先于 .env，启动器本身不解析、
设置或导出这些配置，也不透传运行参数；只接受上述管理命令与版本查询。

五、权限、安全与备份

SQLite 以明文保存 Kafka 密码、证书、私钥、私钥口令及飞书 Token。限制数据库、
-wal、-shm、-journal 等 sidecar、备份和 .env 的访问权限，仅允许运行
账户及授权管理员读取；例如在复制配置后执行 chmod 600 .env。
备份应存放在访问受控的位置，建议使用磁盘加密。不要将 data/、真实
.env、凭据或私钥放入对外分发的发行包。API 和日志不得回显这些凭据。
Kafka Lens 支持监控与受控 Topic 管理，不再是完全只读应用。仅监控时
仍只授予必要的元数据与消费组读取权限；管理权限按下一节最小配置。
服务没有内置认证；请部署在受保护内网或带身份验证的反向代理之后，
使用 HTTPS，并为全部 /api 配置访问控制，不得直接暴露公网。能访问
管理 API 的调用者可以使用已配置 Kafka 凭据的权限，确认框不能代替
身份认证。Linux 发行包不携带宿主机 CA
信任库，私有 TLS 请在集群设置中显式配置 CA PEM。

六、Topic 管理、权限与风险

Topics 页面成功连接后提供四类操作：新建 Topic、删除 Topic、读取
当前配置、修改或恢复配置。创建默认 1 分区，副本因子留空采用集群
默认值，可选参数留空不发送覆盖。删除须输入大小写完全一致的完整
名称，禁止删除 __ 开头的内部 Topic。删除不可恢复，会删除数据并
影响相关生产者和消费者。

仅允许八个配置参数：
  retention.ms          保留时间（毫秒），-1 或非负整数
  retention.bytes       每分区保留空间（字节），-1 或非负整数
  cleanup.policy        delete、compact、delete,compact
  segment.ms            日志段滚动时间（毫秒），正整数
  segment.bytes         日志段大小（字节），正整数
  min.insync.replicas    最小同步副本数，正整数
  max.message.bytes     最大消息批次大小（字节），正整数
  compression.type      producer、gzip、snappy、lz4、zstd、uncompressed
保留参数 -1 表示不按相应维度限制；Kafka 仍可能施加更严格的范围。
应用不生产消息、不调用删除消息 API，不提交/重置 Offset，不提供
分区扩容或副本重分配。

配置弹窗实时读取 Kafka 当前有效值。“Topic 覆盖值”表示 Topic 级
显式设置，“Broker 默认值”表示继承集群默认；未知、不可用及只读
状态明确标记。修改仅增量 SET 已编辑参数；“恢复默认”保存后通过
增量 DELETE 删除该键覆盖，再读取当时的 Broker 默认值，并非写入
某个固定数字。未修改参数保持原样，SQLite 不保存 Topic 配置副本。
缩短保留时间、降低保留空间或恢复到更小的默认值，可能导致已有数据
被后续清理删除；修改前核对业务要求。可靠性参数也可能影响写入。

保存前比较旧值，冲突时须重新读取；该流程不是 Kafka 原子比较写入，
仍可能与外部客户端竞争。保存后重读；显示“已提交但暂时无法确认
当前值”时应重新读取确认，不应当作失败再次盲目提交。创建/删除的
元数据传播可能延迟，请等待列表刷新确认。所有管理复用当前短租约
会话并与采集互斥；服务关闭会等待在途操作及资源释放。

在监控所需 Topic / Group Describe 等权限之外，只对允许管理的
Topic 名称或命名前缀按需授予：
  创建：Create（Topic 级；Cluster Create 也可授权但范围更宽）
  删除：Delete
  读取配置：DescribeConfigs
  修改/恢复默认：AlterConfigs，以及前后读取所需的 DescribeConfigs
无需集群级配置管理权限，也不要授予 All 或不必要的 Write 权限。
Kafka Topic Delete ACL 同时授权 DeleteRecords；本应用不提供后者，
仍须严格保护管理凭据。没有权限时返回安全错误，不能靠按钮隐藏隔离。
ACL 参考：https://kafka.apache.org/39/security/authorization-and-acls/

配置修改依赖 IncrementalAlterConfigs，不降级到整体替换。一次性
验收环境为 Apache Kafka 3.9.1；其他版本、兼容实现和托管服务可能
限制删除、参数范围/可写性、副本数或管理 API，使用前核对其策略。

七、飞书 Lag 增长告警

先按第三、四节创建 .env 并执行 ./start.sh，让后端持续运行；浏览器进入
「集群设置」，在对应集群卡片点击「配置飞书告警」。在飞书群添加自定义
机器人，从 Webhook 复制 access_token 对应的 Token 值，只填 Token，
不要粘贴完整 URL。每个集群独立配置 Token，每条规则绑定一个
Consumer Group + Topic，同一组合不能重复；从真实快照下拉选择，
Lag 阈值最小为 0，持续时间为整数且至少 10 秒。

阈值比较该 Group 在此 Topic 的全部 Partition Lag 总和，严格超过
才计时。连续完整有效采样须在持续时间内非递减，且至少发生一次实际
增长；全程持平不发送。Lag 下降时从当前样本重新计时；任一 Partition
未知、未提交、失败或陈旧都中断计时，不使用部分总和告警。

后台每 5 秒采样；配置 Token 且至少启用一条规则后，后台保持独立的
只读 Kafka 连接，关闭页面、隐藏或切换集群后仍监控。无 Token 或无
启用规则时不发通知、不保持后台连接。服务端必须能访问 Kafka 与飞书
HTTPS；停止后端进程即停止监控，浏览器刷新频率不影响后台采样。

每条规则每轮超限只成功通知一次。发送失败后至少等待 30 秒，只有采样
仍完整有效且满足增长条件时重试，包含首次共最多 3 次；第三次失败后
停止本轮尝试。完整有效的总 Lag 恢复到小于或等于阈值才允许下一轮；
下降但仍超限、未知/失败均不解除已通知或重试耗尽状态。修改规则或
Token、服务重启会重建内存状态，计时与指标历史不持久化。

眼睛图标只控制新输入 Token 的显隐；保存后输入清空，重开只显示掩码，
留空表示保留。点击「测试通知」使用新输入或已存 Token，不保存草稿；
成功后到目标群确认消息，失败时检查 Token、机器人设置及 HTTPS 网络。
取消「启用规则」或删除规则后保存可停用；勾选「清除已保存 Token」
并保存，摘要变为「未配置」，停止该集群全部通知。页面正常/高 Lag
展示规则与飞书告警相互独立。

Token 明文存入 SQLite，数据库、sidecar 和备份权限要求见第五节。
服务没有内置认证，必须限制全部 HTTP/API 访问。不要在日志、截图、
trace 或故障附件中保存 Token、通知请求体或响应正文。

八、从旧加密数据库升级

先停止服务，确认实际生效的 DATABASE_URL 所指向的旧数据库（模板为
data/kafka-dashboard.db；外部环境变量可能覆盖此值）。手动删除该旧
数据库及同名 -wal、-shm、-journal sidecar，再启动新版本并重新配置
全部集群和 Lag 规则。删除会丢失旧配置；程序不会自动转换旧凭据，
也不会自动删除数据库。备份旧数据库时同样须保护凭据及访问权限。

九、故障排查

无法加载配置：检查发行目录是否有可读、格式有效的 .env；检查外部
环境变量是否覆盖了文件中的值。不要输出环境变量转储。
数据库不可写：检查实际 SQLite URL、父目录是否存在、磁盘空间和运行
账户的读写权限。旧加密数据库不兼容时按上述升级步骤处理。
端口占用：检查监听地址和端口，停止占用进程或修改
KAFKA_DASHBOARD_BIND 后重试。
页面不可用：先检查 /api/health，再确认 KAFKA_DASHBOARD_WEB_DIR 指向
含 index.html 的 web/，并检查发行文件校验和及反向代理配置。
Kafka 连接失败：从服务所在机器检查 Bootstrap Servers 和全部
advertised listeners 的 DNS、网络、防火墙及对应监控/管理权限。
SASL/TLS 失败：核对协议、SASL mechanism 和证书信任链；双向 TLS 的
客户端证书与私钥须成对配置，私有 CA 须显式填写。
反馈问题时只提供安全错误、Request ID、版本和复现步骤。不要提交
数据库、.env、环境变量转储、密码、PEM、私钥或包含请求体的 trace。
