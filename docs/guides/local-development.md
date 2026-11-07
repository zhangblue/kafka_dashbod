# 本地运行与环境变量

## 源码开发环境

- 支持 Rust 2024 edition 的稳定 Rust 工具链、Cargo、C/C++ 编译器、CMake、Perl 和 make（构建 vendored librdkafka / OpenSSL）。macOS 可使用 Xcode Command Line Tools；Linux 需安装相应编译工具。
- Node.js 22.12+（或 24 LTS）、npm。
- 后端能访问要监控的 Kafka broker。
- 浏览器：现代 Chromium、Firefox 或 Safari；自动化验收使用 Playwright Chromium。

## 启动前后端

在项目根目录创建 `.env` 后启动后端。复制发行模板后，执行 Cargo 前先编辑 `.env`：源码开发通常由 Vite 提供页面，应移除 `KAFKA_DASHBOARD_WEB_DIR=web`，或调整为含 `index.html` 的实际构建目录；将 `DATABASE_URL` 改为 `sqlite://kafka-dashboard.db?mode=rwc`，或先创建模板所需的可写 `data/` 目录。

```bash
cp packaging/.env.example .env
# 按上文调整 .env 后再启动
cargo run -p backend --bin kafka-dashboard
```

Rust 正常启动时必须有可读且格式有效的 `.env`，即使全部配置已由外部环境变量提供。**警告：SQLite 明文保存 Kafka 凭据。** 限制 `.env`、数据库及备份的访问权限，不要提交这些文件，也不要在命令输出和日志中记录其中内容。

`DATABASE_URL` 默认 `sqlite://kafka-dashboard.db`；可指定绝对路径，例如 `sqlite:///var/lib/kafka-lens/config.db`，父目录需提前创建且可写。启动时初始化当前数据库结构；旧加密版本数据库不兼容，须按[部署指南](deployment.md#升级)处理。绑定地址默认 `127.0.0.1:3000`，设为 `127.0.0.1:0` 可由系统选择临时端口；启动日志会报告地址。健康检查为 `GET /api/health`。

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

浏览器端到端测试使用一次性 Kafka 容器。测试命令、Docker 要求及故障排查见[测试指南](testing.md)。

[返回项目首页](../../README.md)
