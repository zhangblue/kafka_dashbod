export type SecurityProtocol = 'PLAINTEXT' | 'SSL' | 'SASL_PLAINTEXT' | 'SASL_SSL'

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
export interface ConnectionTestResult { brokerCount: number; topicCount: number; elapsedMs: number }
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
export interface UpdateTopicConfigsInput {
  changes: Record<string, string | null>
  expectedValues: Record<string, string | null>
}
export type TopicMutationResult = { status: 'created' } | { status: 'appliedButUnconfirmed' }
export interface PartitionWatermark {
  partition: number; low: number | null; high: number | null
  latestMessageOffset: number | null; retainedOffsetSpan: number | null
}
export interface TopicSnapshot {
  topic: string; low: number | null; latestMessageOffset: number | null
  retainedOffsetSpan: number; incomplete: boolean; partitions: PartitionWatermark[]
}
export interface GroupPartitionSnapshot {
  partition: number; high: number | null; committedOffset: number | null; lag: number | null
}
export interface ConsumerGroupTopicSnapshot {
  groupId: string; topic: string; lag: number; unknownPartitionCount: number
  incomplete: boolean; partitions: GroupPartitionSnapshot[]
}
export interface PartialFailure { scope: string; resource: string; code: string; message: string }
export interface ClusterSnapshot {
  clusterId: string; collectedAt: string; topics: TopicSnapshot[]
  consumerGroupIds: string[]; consumerGroups: ConsumerGroupTopicSnapshot[]; partialFailures: PartialFailure[]
}
