import { describe, expect, it } from 'vitest'
import { topicConfigDefinitions, validateTopicConfigValue } from './topicConfig'

describe('topic configuration form contract', /* 在测试组注册阶段组织当前功能的交互与边界回归。 */ () => {
  it('defines eight supported settings with typed translation metadata and unchanged values', /* 在本用例执行时验证八项白名单顺序、翻译键元数据和控件类型符合约定。 */ () => {
    expect(topicConfigDefinitions.map(item => [item.key, item.labelKey, item.unitKey, item.inputType])).toEqual([
      ['retention.ms', 'topicAdmin.config.retentionMs.label', 'topicAdmin.units.milliseconds', 'number'],
      ['retention.bytes', 'topicAdmin.config.retentionBytes.label', 'topicAdmin.units.bytesPerPartition', 'number'],
      ['cleanup.policy', 'topicAdmin.config.cleanupPolicy.label', null, 'select'],
      ['segment.ms', 'topicAdmin.config.segmentMs.label', 'topicAdmin.units.milliseconds', 'number'],
      ['segment.bytes', 'topicAdmin.config.segmentBytes.label', 'topicAdmin.units.bytes', 'number'],
      ['min.insync.replicas', 'topicAdmin.config.minInsyncReplicas.label', 'topicAdmin.units.count', 'number'],
      ['max.message.bytes', 'topicAdmin.config.maxMessageBytes.label', 'topicAdmin.units.bytes', 'number'],
      ['compression.type', 'topicAdmin.config.compressionType.label', null, 'select'],
    ])
    for (const definition of topicConfigDefinitions) {
      expect(definition.descriptionKey.length).toBeGreaterThan(12)
      expect(definition.descriptionKey).toMatch(/^topicAdmin\.config\./)
      expect(definition.riskKey).toMatch(/^topicAdmin\.config\./)
    }
    expect(topicConfigDefinitions.find(item => item.key === 'cleanup.policy')?.options?.map(item => item.value))
      .toEqual(['delete', 'compact', 'delete,compact'])
    expect(topicConfigDefinitions.find(item => item.key === 'compression.type')?.options?.map(item => item.value))
      .toEqual(['producer', 'gzip', 'snappy', 'lz4', 'zstd', 'uncompressed'])
  })

  it.each([
    ['retention.ms', '-1'], ['retention.ms', '0'], ['retention.bytes', '-1'], ['retention.bytes', '0'],
    ['retention.ms', '9223372036854775807'], ['segment.ms', '1'], ['segment.bytes', '0001'],
    ['min.insync.replicas', '2'], ['max.message.bytes', '9223372036854775807'],
    ['cleanup.policy', 'delete'], ['cleanup.policy', 'compact'], ['cleanup.policy', 'delete,compact'],
    ['compression.type', 'producer'], ['compression.type', 'gzip'], ['compression.type', 'snappy'],
    ['compression.type', 'lz4'], ['compression.type', 'zstd'], ['compression.type', 'uncompressed'],
  ])('accepts the backend-supported value %s=%s', /* 在本用例执行时验证后端支持的参数值均通过前端校验。 */ (key, value) => {
    expect(validateTopicConfigValue(key, value)).toBeUndefined()
  })

  it.each([
    ['retention.ms', '-2'], ['retention.bytes', '-01'], ['retention.ms', '+1'], ['retention.ms', ' 1'],
    ['retention.ms', '1 '], ['retention.ms', ''], ['segment.ms', '0'], ['segment.bytes', '-1'],
    ['min.insync.replicas', '1.5'], ['max.message.bytes', '1e3'], ['segment.ms', '１２'],
    ['retention.ms', '9223372036854775808'], ['segment.bytes', '9223372036854775808'],
    ['cleanup.policy', 'compact,delete'], ['cleanup.policy', 'delete, compact'], ['compression.type', 'brotli'],
    ['unclean.leader.election.enable', 'true'], ['toString', '1'],
  ])('rejects invalid configuration %s without reflecting its value', /* 在本用例执行时验证非法参数返回固定错误而不回显输入。 */ (key, value) => {
    const error = validateTopicConfigValue(key, value)
    expect(error).toBe(key === 'toString' || key === 'unclean.leader.election.enable' ? 'topicAdmin.validation.unsupported' : key === 'cleanup.policy' || key === 'compression.type' ? 'topicAdmin.validation.invalidOption' : 'topicAdmin.validation.invalidInteger')
    if (value) expect(error).not.toContain(value)
  })
})
