# Topics 水位与管理

## 查看 Partition 水位

在 Topics 页面展开 Topic 可查看各 Partition。每个 Partition 都有自己的 Offset 序列。

| 表格字段 | 读法 |
| --- | --- |
| Low watermark | 当前最早保留的位置 |
| High watermark | 下一条消息的写入位置 |
| 最大消息 Offset | 非空分区为 High − 1，空分区显示 `—` |
| 保留消息估算 | High − Low 表示 Offset 范围。压缩、删除和事务记录都会影响实际消息数 |

## 创建、删除与修改配置

进入 Topics 页面并成功连接后，可执行四类操作：新建 Topic、删除 Topic、读取当前 Topic 配置、修改或恢复配置。创建时分区数默认 1，副本因子留空使用集群默认值，可选参数留空不会发送覆盖值。删除必须输入大小写完全一致的完整名称，`__` 开头的内部 Topic 禁止删除。**删除 Topic 不可恢复，会删除其中数据并影响相关生产者与消费者。**

仅开放以下 8 个参数，界面显示 Kafka key、中文含义、单位及风险；Kafka 的实际策略仍可能拒绝本地校验通过的值。

| 参数 | 含义与输入范围 |
| --- | --- |
| `retention.ms` | 消息保留时间，毫秒；`-1` 表示不按时间限制，或非负整数 |
| `retention.bytes` | 每分区保留空间，字节；`-1` 表示不按空间限制，或非负整数 |
| `cleanup.policy` | 清理策略：`delete`、`compact`、`delete,compact` |
| `segment.ms` | 日志段滚动时间，毫秒，正整数 |
| `segment.bytes` | 日志段大小，字节，正整数 |
| `min.insync.replicas` | 最小同步副本数，正整数；会影响生产者写入可用性 |
| `max.message.bytes` | 最大消息批次大小，字节，正整数；需与客户端限制配合 |
| `compression.type` | 压缩类型：`producer`、`gzip`、`snappy`、`lz4`、`zstd`、`uncompressed` |

打开配置弹窗即向 Kafka 读取有效当前值。来源“Topic 覆盖值”表示显式的 Topic 级设置，“Broker 默认值”表示继承集群提供的默认配置；来源未知、值不可用及只读参数会明确标记，不用示例值补齐。点击“恢复默认”并保存会删除该键的 Topic 级覆盖（增量 `DELETE`），随后重新读取当时的 Broker 默认值，不把某个固定数字写作默认。普通保存仅增量修改已编辑项（`SET`），未修改参数保持原样。[Kafka 配置覆盖说明](https://github.com/apache/kafka/blob/trunk/docs/configuration/topic-configs.md)

**缩短保留时间或降低保留空间，可能使已有数据在后续清理中被删除。** 恢复默认也可能降低当前有效保留值；修改前应核对业务保留要求、磁盘容量及集群默认策略。`compact` 不等同于按条数或时间精确保留。

保存前会重读已编辑键并比较预期旧值，发现冲突时要求重新读取；Kafka 不提供本流程的原子比较写入，因此外部客户端仍可能在检查后并发修改。保存成功后再次读取；若显示“已提交但暂时无法确认当前值”，应重新读取确认，不能据此断言写入失败。创建和删除后列表可能受元数据传播影响稍有延迟，等待刷新确认实际结果。

管理操作复用当前短租约会话，与快照采集互斥；会话失效或页面隐藏时管理入口禁用。关闭会话、删除集群或退出服务会等待已接受的管理工作及资源释放。

### 最小 Kafka 权限与兼容性

在已有监控所需 Topic / Group `Describe` 等权限基础上，按业务需要仅对允许管理的 Topic 名称或命名前缀增授下列 ACL。无需为 Topic 配置读取/修改授予集群级配置管理权限。

| 操作 | Topic 资源所需 ACL |
| --- | --- |
| 创建 | `Create`；集群级 `Create` 也能授权，但范围更宽 |
| 删除 | `Delete` |
| 读取配置 | `DescribeConfigs` |
| 修改/恢复默认 | `AlterConfigs`，以及本应用保存前后读取所需的 `DescribeConfigs` |

仅监控的账户可以继续不授予上述写权限；没有权限的管理请求会返回安全错误。不要授予 `All` 或不必要的 `Write` / Offset 修改能力。Kafka 的 Topic `Delete` ACL 同时授权其 DeleteRecords API，本应用仍不提供该 API；须保护管理凭据，不能把应用功能边界当作 Kafka 权限隔离。[Kafka ACL 对照表](https://kafka.apache.org/39/security/authorization-and-acls/)

配置更新依赖集群支持 `IncrementalAlterConfigs`，不会降级到整体替换配置。一次性验收环境使用 Apache Kafka 3.9.1；其他版本、兼容实现及托管服务可能限制删除、参数可写性、合法范围、副本数或管理 API。请依据目标服务的版本和策略核对权限，不能把单一版本验收视为全部服务兼容保证。

[返回项目首页](../../README.md)
