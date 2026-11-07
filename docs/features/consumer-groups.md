# Consumer Groups 与 Lag 趋势

在 Consumer Groups 页面选择 Group 和 Topic，展开行可查看各 Partition 的提交位置、High watermark 和 Lag。也可以在折线图中查看最近一小时的变化。

| 字段 | 读法 |
| --- | --- |
| Committed Offset | 消费组已提交的下一条待消费位置 |
| Lag | 对单个 Partition，等于 `max(high − committed, 0)` |
| 有效分区 | 同时取得 High watermark 和有效提交位置、可以计算 Lag 的分区数 |
| 未提交分区 | 没有有效提交位置的分区数。采集异常时，无法据此断定该组从未提交 |

部分分区采集失败时，界面会标记未知或陈旧状态。历史曲线遇到未提交、采集失败或缺失的序列会显示断点。行上的正常、积压和高 Lag 标签由[状态判定规则](lag-rules.md)决定。

[返回项目首页](../../README.md)
