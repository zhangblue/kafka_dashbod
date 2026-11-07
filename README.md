# Kafka Lens

## 项目介绍

Kafka Lens 是面向多个 Kafka 集群的 Web 仪表盘。你可以切换集群，查看 Topic 和 Partition 的 Offset 水位、Consumer Group 的消费进度与 Lag，并在授权范围内管理 Topic。后端使用 Rust，前端使用 React。连接配置和规则保存在 SQLite 中，图表保留当前页面最近一小时的采样。

## 使用场景

- **排查消费积压**。从集群概览找到需要关注的消费组，再查看对应 Topic 和 Partition 的提交位置与 Lag 趋势。
- **判断周期性提交的影响**。为特定 Group 和 Topic 配置正常区间与持续超限时间，区分 Flink checkpoint 之间的短暂波动和持续积压。
- **值班告警**。为关键消费组启用飞书通知，浏览器关闭后，后端仍可监控符合规则的 Lag 增长。
- **维护 Topic**。在页面查看保留策略等当前配置，按需修改、恢复 Broker 默认值，或创建和删除 Topic。

## 开始使用

| 目标 | 指南 |
| --- | --- |
| 在服务器或 Mac 上运行 | [打包与部署](docs/guides/deployment.md) · [下载发行包](https://github.com/zhangblue/kafka_dashbod/releases) |
| 从源码启动前后端 | [本地运行与环境变量](docs/guides/local-development.md) |
| 排查连接、采集或启动问题 | [常见问题](docs/guides/troubleshooting.md) |
| 运行开发测试 | [开发验证与端到端测试](docs/guides/testing.md) |

## 功能指南

| 功能 | 可以查看或配置的内容 |
| --- | --- |
| [集群连接](docs/features/clusters.md) | 多集群配置、连接测试、SASL / TLS 与凭据 |
| [集群概览](docs/features/dashboard.md) | Lag 趋势与分布、刷新频率、采集会话 |
| [Topics](docs/features/topics.md) | 分区水位、保留量估算、创建和删除 Topic、配置与权限 |
| [Consumer Groups](docs/features/consumer-groups.md) | 提交位置、有效与未提交分区、Lag 历史 |
| [Lag 状态判定](docs/features/lag-rules.md) | 正常、积压和高 Lag 的阈值、持续时间与自定义规则 |
| [飞书告警](docs/features/feishu-alerts.md) | 机器人配置、后台采样、增长条件与通知重试 |
| [界面语言](docs/features/language.md) | 中文与英文切换、浏览器偏好 |
