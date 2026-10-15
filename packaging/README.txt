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

执行：
  ./start.sh
浏览器访问：http://127.0.0.1:3000
健康检查：http://127.0.0.1:3000/api/health
修改监听地址后请使用对应地址。按 Ctrl-C 或向进程发送 SIGTERM 可正常
停止服务，程序会等待在途 Kafka 工作及会话释放完成。

Rust 程序正常启动时强制读取当前工作目录的 .env。文件缺少、不可读或
格式错误都会导致启动失败；即使已在外部设置全部配置，也必须有有效
的 .env。父进程已经设置的同名环境变量优先于 .env，启动器本身不设置
或导出这些配置。启动器原样转发参数，以下版本查询无需 .env：
  ./start.sh --version

五、权限、安全与备份

SQLite 以明文保存 Kafka 密码、证书、私钥及私钥口令。限制数据库、
-wal、-shm、-journal 等 sidecar、备份和 .env 的访问权限，仅允许运行
账户及授权管理员读取；例如在复制配置后执行 chmod 600 .env。
备份应存放在访问受控的位置，建议使用磁盘加密。不要将 data/、真实
.env、凭据或私钥放入对外分发的发行包。API 和日志不得回显这些凭据。
Kafka 账户只授予所需的元数据与消费组等只读权限。
服务没有内置认证；请部署在受保护内网或带身份验证的反向代理之后，
使用 HTTPS，并为 /api 配置访问控制。Linux 发行包不携带宿主机 CA
信任库，私有 TLS 请在集群设置中显式配置 CA PEM。

六、从旧加密数据库升级

先停止服务，确认实际生效的 DATABASE_URL 所指向的旧数据库（模板为
data/kafka-dashboard.db；外部环境变量可能覆盖此值）。手动删除该旧
数据库及同名 -wal、-shm、-journal sidecar，再启动新版本并重新配置
全部集群和 Lag 规则。删除会丢失旧配置；程序不会自动转换旧凭据，
也不会自动删除数据库。备份旧数据库时同样须保护凭据及访问权限。

七、故障排查

无法加载配置：检查发行目录是否有可读、格式有效的 .env；检查外部
环境变量是否覆盖了文件中的值。不要输出环境变量转储。
数据库不可写：检查实际 SQLite URL、父目录是否存在、磁盘空间和运行
账户的读写权限。旧加密数据库不兼容时按上述升级步骤处理。
端口占用：检查监听地址和端口，停止占用进程或修改
KAFKA_DASHBOARD_BIND 后重试。
页面不可用：先检查 /api/health，再确认 KAFKA_DASHBOARD_WEB_DIR 指向
含 index.html 的 web/，并检查发行文件校验和及反向代理配置。
Kafka 连接失败：从服务所在机器检查 Bootstrap Servers 和全部
advertised listeners 的 DNS、网络、防火墙及只读账户权限。
SASL/TLS 失败：核对协议、SASL mechanism 和证书信任链；双向 TLS 的
客户端证书与私钥须成对配置，私有 CA 须显式填写。
反馈问题时只提供安全错误、Request ID、版本和复现步骤。不要提交
数据库、.env、环境变量转储、密码、PEM、私钥或包含请求体的 trace。
