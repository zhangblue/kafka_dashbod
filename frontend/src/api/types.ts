export type SecurityProtocol = 'PLAINTEXT' | 'SSL' | 'SASL_PLAINTEXT' | 'SASL_SSL'

// 新增或编辑请求使用的配置输入，可携带凭据；禁止将此对象写入日志或错误附件。
export interface ClusterInput {
  name: string
  bootstrapServers: string
  securityProtocol: SecurityProtocol
  saslMechanism?: string | null
  username?: string | null
  password?: string | null
  caPem?: string | null
  clientCertificatePem?: string | null
  clientKeyPem?: string | null
  clientKeyPassphrase?: string | null
  clearPassword?: boolean
  clearCaPem?: boolean
  clearClientCertificatePem?: boolean
  clearClientKeyPem?: boolean
  clearClientKeyPassphrase?: boolean
  connectionTimeoutMs: number
  defaultNormalLagMax: number
  defaultHighLagMin: number
  defaultOverLimitSeconds: number
}

// 集群读取响应仅暴露凭据是否配置，页面不得从 DTO 推断或显示秘密原文。
export interface ClusterDto {
  id: string
  name: string
  bootstrapServers: string
  securityProtocol: SecurityProtocol
  saslMechanism: string | null
  username: string | null
  passwordConfigured?: boolean
  caPemConfigured?: boolean
  clientCertificatePemConfigured?: boolean
  clientKeyPemConfigured?: boolean
  clientKeyPassphraseConfigured?: boolean
  connectionTimeoutMs: number
  defaultNormalLagMax: number
  defaultHighLagMin: number
  defaultOverLimitSeconds: number
  createdAt: string
  updatedAt: string
}

export interface LagThresholds {
  normalMax: number
  highMin: number
  overLimitSeconds: number
}
export interface LagRule extends LagThresholds {
  id?: string
  clusterId?: string
  groupId: string
  topic: string
}
export interface LagRuleSet { defaults: LagThresholds; overrides: LagRule[] }
export interface FeishuAlertRule {
  id?: string
  groupId: string
  topic: string
  lagThreshold: number
  growthSeconds: number
  enabled: boolean
}
export interface FeishuAlertSettings {
  accessTokenConfigured: boolean
  maskedAccessToken: string | null
  rules: FeishuAlertRule[]
}
export interface SaveFeishuAlertSettings {
  accessToken?: string
  clearAccessToken: boolean
  rules: FeishuAlertRule[]
}
export interface ConnectionTestResult { brokerCount: number; topicCount: number; elapsedMs: number }
// 监控连接创建响应携带短租约信息，由页面按需采样续租。
export interface CreatedSession { sessionId: string; leaseSeconds: number }
export interface CreateTopicInput {
  name: string
  partitions: number
  replicationFactor?: number | null
  configs?: Record<string, string>
}
export type TopicConfigSource = 'topicOverride' | 'brokerDefault' | 'unknown'
export interface TopicConfigValue {
  key: string
  value: string | null
  source: TopicConfigSource
  readOnly: boolean
}
export interface TopicConfigsResponse { configs: TopicConfigValue[] }
// 保存配置时携带增量变更和冲突检查期望值；null 表示删除 Topic 覆盖以恢复默认。
export interface UpdateTopicConfigsInput {
  changes: Record<string, string | null>
  expectedValues: Record<string, string | null>
}
// 管理操作响应区分创建成功与已应用但未确认，后者不能简单当作失败重试。
export type TopicMutationResult = { status: 'created' } | { status: 'appliedButUnconfirmed' }
// Low 为保留起点，High 为下一写入位置；范围只是估算，null 必须保留为未知。
export interface PartitionWatermark {
  partition: number; low: number | null; high: number | null
  latestMessageOffset: number | null; retainedOffsetSpan: number | null
}
export interface TopicSnapshot {
  topic: string; low: number | null; latestMessageOffset: number | null
  retainedOffsetSpan: number; incomplete: boolean; partitions: PartitionWatermark[]
}
// 分区 Lag 与提交位置允许未知，未提交或采集失败不得转换成零。
export interface GroupPartitionSnapshot {
  partition: number; high: number | null; committedOffset: number | null; lag: number | null
}
export interface ConsumerGroupTopicSnapshot {
  groupId: string; topic: string; lag: number; unknownPartitionCount: number
  incomplete: boolean; partitions: GroupPartitionSnapshot[]
}
export interface PartialFailure { scope: string; resource: string; code: string; message: string }
// 原始采集快照用于统计、历史和规则计时，部分失败与未知值随响应保留。
export interface ClusterSnapshot {
  clusterId: string; collectedAt: string; topics: TopicSnapshot[]
  consumerGroupIds: string[]; consumerGroups: ConsumerGroupTopicSnapshot[]; partialFailures: PartialFailure[]
}
