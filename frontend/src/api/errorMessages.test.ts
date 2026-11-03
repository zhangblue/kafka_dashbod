import { expect, it } from 'vitest'
import i18n from '../i18n'
import { ApiError } from './client'
import { localizeApiError, localizeFieldError } from './errorMessages'

const tEn = i18n.getFixedT('en')
const tZh = i18n.getFixedT('zh-CN')
const codes = [
  ['VALIDATION_ERROR', 'Some fields are invalid.'],
  ['CLUSTER_NOT_FOUND', 'Cluster not found.'],
  ['CONFLICT', 'A configuration with the same unique fields already exists.'],
  ['PAYLOAD_TOO_LARGE', 'The request exceeds the 256 KiB limit.'],
  ['INTERNAL_ERROR', 'The request could not be completed. Please try again later.'],
  ['SESSION_EXPIRED', 'The session has expired. Please reconnect.'],
  ['SHUTTING_DOWN', 'The service is shutting down. Please try again later.'],
  ['SNAPSHOT_IN_PROGRESS', 'A snapshot is already being collected. Please try again later.'],
  ['KAFKA_AUTH_FAILED', 'Kafka authentication failed. Check the credentials and permissions.'],
  ['KAFKA_DNS_FAILED', 'Kafka host lookup failed. Check the server addresses and DNS.'],
  ['KAFKA_TIMEOUT', 'The Kafka request timed out. Please try again later.'],
  ['KAFKA_TLS_FAILED', 'The Kafka TLS connection failed. Check the certificates and protocol.'],
  ['KAFKA_QUERY_FAILED', 'The Kafka query failed. Please try again later.'],
  ['KAFKA_UNAVAILABLE', 'Kafka is unavailable. Check the connection and try again.'],
  ['FEISHU_SEND_FAILED', 'The Feishu notification could not be sent. Check the token and network.'],
  ['TOPIC_ALREADY_EXISTS', 'The Topic already exists.'],
  ['TOPIC_NOT_FOUND', 'The Topic was not found.'],
  ['TOPIC_AUTHORIZATION_FAILED', 'This operation is not authorized for the Topic.'],
  ['TOPIC_CONFIG_CHANGED', 'The Topic configuration has changed. Reload it before saving.'],
  ['TOPIC_OPERATION_TIMEOUT', 'The Topic operation timed out. Refresh to confirm the result.'],
  ['INVALID_TOPIC_CONFIG', 'The Topic configuration is invalid.'],
  ['INTERNAL_TOPIC_PROTECTED', 'Internal Topics cannot be deleted.'],
] as const

// 验证固定错误码使用本地化提示，后端 message 无论内容如何都不能覆盖它。
it.each(codes)('localizes the stable code %s', (code, expected) => {
  const error = new ApiError(422, { code, message: '不得显示', requestId: 'req-42' })
  expect(localizeApiError(error, tEn)).toEqual({ message: expected, fields: {}, requestId: 'req-42' })
  expect(localizeApiError(error, tZh).message).not.toMatch(/不得显示|^errors\./)
  expect(localizeApiError(error, tZh).message).not.toBe(expected)
})

// 验证非 API 错误、未知码与原型属性码安全降级，不输出任意错误文本。
it.each([new Error('不得显示'), new ApiError(500, { code: 'UNKNOWN', message: '不得显示' }),
  new ApiError(500, { code: 'constructor', message: '不得显示' }), null])('safely falls back for unknown failures %#', error => {
  expect(localizeApiError(error, tEn).message).toBe('Request failed. Please try again later.')
})

// 验证只按完整受控文本匹配字段类别，防止模糊匹配将任意服务端详情带入页面。
it.each([
  ['Name is required', 'This field is required.', '此字段为必填项。'],
  ['Must be zero or greater', 'Enter a non-negative integer.', '请输入非负整数。'],
  ['Must be greater than zero', 'Enter a positive integer.', '请输入正整数。'],
  ['The group/topic pair must be unique', 'The Consumer Group / Topic pair must be unique.', 'Consumer Group / Topic 组合不能重复。'],
  ['Invalid settings', 'The security or settings combination is invalid.', '安全协议或配置组合无效。'],
  ['Unsupported topic configuration', 'This Topic configuration option is not supported.', '不支持此 Topic 配置项。'],
  ['A valid JSON request with the required fields is expected', 'The request body is invalid.', '请求体格式或字段无效。'],
  ['unrecognized backend detail', 'This field is invalid.', '字段值无效。'],
  ['Name is required: private-value', 'This field is invalid.', '字段值无效。'],
])('localizes the controlled field category %s', (raw, english, chinese) => {
  expect(localizeFieldError(raw, tEn)).toBe(english)
  expect(localizeFieldError(raw, tZh)).toBe(chinese)
})

// 验证受控路径保留表单关联，未知字段键和值原文不出现在本地化结果。
it('localizes field values while bounding their DOM-visible identities', () => {
  const result = localizeApiError(new ApiError(422, { code: 'VALIDATION_ERROR', fields: {
    name: 'Name is required', 'overrides.0.topic': 'Topic is required',
    'changes.retention.ms': 'Invalid topic configuration value',
    'private backend field': 'private backend detail',
  }, requestId: 'req-fields' }), tZh)
  expect(result.fields.name).toBe('此字段为必填项。')
  expect(result.fields['overrides.0.topic']).toBe('此字段为必填项。')
  expect(result.fields['changes.retention.ms']).toBe('Topic 配置值无效。')
  expect(JSON.stringify(result)).not.toContain('private backend')
  expect(result.requestId).toBe('req-fields')
})
