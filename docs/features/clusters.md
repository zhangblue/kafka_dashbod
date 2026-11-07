# 多集群连接与凭据

在「集群设置」添加集群并填写 Bootstrap Servers。可以先测试连接，再保存配置。进入仪表盘后，用顶部集群选择器切换查看不同集群。

## 连接协议

Bootstrap Servers 填写逗号分隔的 `host:port`。后端必须能访问 Kafka 返回的 advertised listeners 地址。支持 `PLAINTEXT`、`SSL`、`SASL_PLAINTEXT` 和 `SASL_SSL`；SASL Mechanism 可填 `PLAIN`、`SCRAM-SHA-256` 或 `SCRAM-SHA-512`，同时填写用户名和密码。TLS 使用 CA PEM，双向 TLS 另需成对提供客户端证书 PEM 和客户端私钥 PEM；加密私钥可填写客户端私钥口令（`clientKeyPassphrase`）。

| Security Protocol | 连接配置 |
| --- | --- |
| `PLAINTEXT` | 无 SASL 或 TLS；仅适合受信任网络 |
| `SSL` | TLS；按 broker 信任链提供 CA，双向认证时同时提供客户端证书和私钥 |
| `SASL_PLAINTEXT` | SASL mechanism、用户名、密码；传输不加密 |
| `SASL_SSL` | SASL mechanism、用户名、密码，加 TLS 配置 |

不支持 OAuth/OIDC、Kerberos 或任意 librdkafka 参数透传。TLS 客户端证书和私钥必须成对填写；不要把 SASL 字段配置到非 SASL 协议。

## 凭据与连接测试

**警告：密码、CA、客户端证书、私钥和私钥口令以明文 JSON 存入 SQLite。** 任何能读取数据库、sidecar 或备份的人都能获得这些凭据；请限制文件权限并将备份保存在访问受控的位置；建议启用磁盘加密。API 只返回各字段的“已配置”标志，不回显内容，日志也不得输出凭据。编辑时敏感字段留空或省略表示保留；使用对应的清除选项可显式移除。API 字段为 `clearPassword`、`clearCaPem`、`clearClientCertificatePem`、`clearClientKeyPem` 和 `clearClientKeyPassphrase`，设为 `true` 时清除该字段；同一字段不能同时填写新值并要求清除。保存前会校验合并后的完整配置：切换协议时须清除不适用的凭据，证书与私钥仍须成对保留，私钥口令不能脱离私钥单独存在。

新增或编辑表单均可在保存前测试连接。新配置使用 `POST /api/clusters/test`，未保存的编辑使用 `POST /api/clusters/:id/test`，请求体均为完整表单字段；编辑草稿中省略或留空的敏感字段会复用已保存值，清除选项只影响此次测试。原有 `POST /api/clusters/:id/test` 空请求体继续测试已保存配置。测试不会写入配置，也不会创建监控会话；临时工作由会话管理器跟踪，删除集群或退出进程时会等待相关测试释放资源。请求体上限为 256 KiB，超限返回 413；连接错误及校验错误使用安全错误消息与 Request ID。

[返回项目首页](../../README.md)
