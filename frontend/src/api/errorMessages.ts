import type { TFunction } from 'i18next'
import { ApiError } from './client'
import type { zhCN } from '../i18n/resources/zh-CN'

const apiKeys = {
  VALIDATION_ERROR: 'errors.api.VALIDATION_ERROR',
  CLUSTER_NOT_FOUND: 'errors.api.CLUSTER_NOT_FOUND',
  CONFLICT: 'errors.api.CONFLICT',
  PAYLOAD_TOO_LARGE: 'errors.api.PAYLOAD_TOO_LARGE',
  INTERNAL_ERROR: 'errors.api.INTERNAL_ERROR',
  SESSION_EXPIRED: 'errors.api.SESSION_EXPIRED',
  SHUTTING_DOWN: 'errors.api.SHUTTING_DOWN',
  SNAPSHOT_IN_PROGRESS: 'errors.api.SNAPSHOT_IN_PROGRESS',
  KAFKA_AUTH_FAILED: 'errors.api.KAFKA_AUTH_FAILED',
  KAFKA_DNS_FAILED: 'errors.api.KAFKA_DNS_FAILED',
  KAFKA_TIMEOUT: 'errors.api.KAFKA_TIMEOUT',
  KAFKA_TLS_FAILED: 'errors.api.KAFKA_TLS_FAILED',
  KAFKA_QUERY_FAILED: 'errors.api.KAFKA_QUERY_FAILED',
  KAFKA_UNAVAILABLE: 'errors.api.KAFKA_UNAVAILABLE',
  FEISHU_SEND_FAILED: 'errors.api.FEISHU_SEND_FAILED',
  TOPIC_ALREADY_EXISTS: 'errors.api.TOPIC_ALREADY_EXISTS',
  TOPIC_NOT_FOUND: 'errors.api.TOPIC_NOT_FOUND',
  TOPIC_AUTHORIZATION_FAILED: 'errors.api.TOPIC_AUTHORIZATION_FAILED',
  TOPIC_CONFIG_CHANGED: 'errors.api.TOPIC_CONFIG_CHANGED',
  TOPIC_OPERATION_TIMEOUT: 'errors.api.TOPIC_OPERATION_TIMEOUT',
  INVALID_TOPIC_CONFIG: 'errors.api.INVALID_TOPIC_CONFIG',
  INTERNAL_TOPIC_PROTECTED: 'errors.api.INTERNAL_TOPIC_PROTECTED',
} as const

type FieldKey = `errors.fields.${keyof typeof zhCN.errors.fields}`
const fieldKeys = new Map<string, FieldKey>([
  ['Name is required', 'errors.fields.required'],
  ['Bootstrap servers are required', 'errors.fields.required'],
  ['Consumer group is required', 'errors.fields.required'],
  ['Topic is required', 'errors.fields.required'],
  ['An access token is required', 'errors.fields.required'],
  ['Expected current value is required', 'errors.fields.required'],
  ['At least one topic configuration change is required', 'errors.fields.required'],
  ['Must be zero or greater', 'errors.fields.nonNegativeInteger'],
  ['Must be a safe integer from 0 to 9007199254740991', 'errors.fields.nonNegativeInteger'],
  ['Must be greater than zero', 'errors.fields.positiveInteger'],
  ['Partitions must be greater than zero', 'errors.fields.positiveInteger'],
  ['Replication factor must be greater than zero', 'errors.fields.positiveInteger'],
  ['The group/topic pair must be unique', 'errors.fields.duplicatePair'],
  ['Invalid settings', 'errors.fields.invalidSettings'],
  ['Unsupported topic configuration', 'errors.fields.unsupportedTopicConfig'],
  ['Invalid topic configuration value', 'errors.fields.invalidTopicConfig'],
  ['Invalid topic name', 'errors.fields.invalidTopicName'],
  ['A valid JSON request with the required fields is expected', 'errors.fields.invalidBody'],
  ['Must be greater than the normal maximum', 'errors.fields.thresholdOrder'],
  ['Must be a safe integer from 10 to 9007199254740991 seconds', 'errors.fields.growthDuration'],
  ['Token must be at most 512 bytes and contain no control characters', 'errors.fields.invalidToken'],
  ['A new token and clearing the token cannot be combined', 'errors.fields.tokenConflict'],
  ['Partitions must not exceed 100000', 'errors.fields.partitionsLimit'],
  ['Replication factor must not exceed 10000', 'errors.fields.replicationLimit'],
  ['Confirmation name must exactly match the topic name', 'errors.fields.confirmationMismatch'],
])
const fieldNames = new Set([
  'body', 'name', 'bootstrapServers', 'securityProtocol', 'saslMechanism', 'username', 'password',
  'caPem', 'clientCertificatePem', 'clientKeyPem', 'clientKeyPassphrase', 'connectionTimeoutMs',
  'defaultNormalLagMax', 'defaultHighLagMin', 'defaultOverLimitSeconds',
  'accessToken', 'clearAccessToken', 'partitions', 'replicationFactor', 'confirmationName',
  'configs', 'changes', 'expectedValues',
])
const configKeys = new Set(['retention.ms', 'retention.bytes', 'cleanup.policy', 'segment.ms',
  'segment.bytes', 'min.insync.replicas', 'max.message.bytes', 'compression.type'])

// 字段错误进入表单前验证固定字段路径，防止未知后端键通过 DOM id 回显原文。
function isKnownField(field: string): boolean {
  if (fieldNames.has(field) || configKeys.has(field)) return true
  if (/^defaults\.(normalMax|highMin|overLimitSeconds)$/.test(field)) return true
  if (/^overrides\.\d+\.(groupId|topic|normalMax|highMin|overLimitSeconds)$/.test(field)) return true
  if (/^rules\.\d+\.(id|groupId|topic|lagThreshold|growthSeconds|enabled)$/.test(field)) return true
  return /^(configs|changes|expectedValues)\./.test(field) && configKeys.has(field.slice(field.indexOf('.') + 1))
}

// 渲染服务端字段错误时只翻译完整匹配的受控类别；未知文本降为固定提示以避免秘密泄露。
export function localizeFieldError(message: string, t: TFunction): string {
  return t(fieldKeys.get(message) ?? 'errors.fields.invalid')
}

// 公共错误提示或表单渲染时根据稳定 code 翻译，不使用后端 message 或任意 Error.message。
export function localizeApiError(error: unknown, t: TFunction): { message: string; fields: Record<string, string>; requestId?: string } {
  if (!(error instanceof ApiError)) return { message: t('common.errors.requestFailed'), fields: {} }
  // 显式检查自有键，防止 constructor 等未知码命中对象原型而越过通用降级。
  const key = Object.hasOwn(apiKeys, error.code) ? apiKeys[error.code as keyof typeof apiKeys] : 'common.errors.requestFailed'
  const fields: Record<string, string> = {}
  for (const [field, message] of Object.entries(error.fields)) {
    // 未知字段键不进入 DOM；整体错误提示仍保留，让新增后端字段安全失败。
    if (isKnownField(field)) fields[field] = localizeFieldError(message, t)
  }
  return { message: t(key), fields, requestId: error.requestId }
}
