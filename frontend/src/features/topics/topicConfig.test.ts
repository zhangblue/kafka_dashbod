import { describe, expect, it } from 'vitest'
import { topicConfigDefinitions, validateTopicConfigValue } from './topicConfig'

describe('topic configuration form contract', () => {
  it('defines the eight supported settings in display order with Chinese guidance and suitable controls', () => {
    expect(topicConfigDefinitions.map(item => [item.key, item.label, item.unit, item.inputType])).toEqual([
      ['retention.ms', '消息保留时间', '毫秒', 'number'],
      ['retention.bytes', '分区保留空间', '字节 / Partition', 'number'],
      ['cleanup.policy', '清理策略', null, 'select'],
      ['segment.ms', '日志段最大时间', '毫秒', 'number'],
      ['segment.bytes', '日志段最大大小', '字节', 'number'],
      ['min.insync.replicas', '最少同步副本数', '个', 'number'],
      ['max.message.bytes', '最大消息批次', '字节', 'number'],
      ['compression.type', '压缩方式', null, 'select'],
    ])
    for (const definition of topicConfigDefinitions) {
      expect(definition.description.length).toBeGreaterThan(12)
      expect(definition.description).toMatch(/[\u4e00-\u9fff]/)
      expect(definition.risk).toMatch(/[\u4e00-\u9fff]/)
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
  ])('accepts the backend-supported value %s=%s', (key, value) => {
    expect(validateTopicConfigValue(key, value)).toBeUndefined()
  })

  it.each([
    ['retention.ms', '-2'], ['retention.bytes', '-01'], ['retention.ms', '+1'], ['retention.ms', ' 1'],
    ['retention.ms', '1 '], ['retention.ms', ''], ['segment.ms', '0'], ['segment.bytes', '-1'],
    ['min.insync.replicas', '1.5'], ['max.message.bytes', '1e3'], ['segment.ms', '１２'],
    ['retention.ms', '9223372036854775808'], ['segment.bytes', '9223372036854775808'],
    ['cleanup.policy', 'compact,delete'], ['cleanup.policy', 'delete, compact'], ['compression.type', 'brotli'],
    ['unclean.leader.election.enable', 'true'], ['toString', '1'],
  ])('rejects invalid configuration %s without reflecting its value', (key, value) => {
    const error = validateTopicConfigValue(key, value)
    expect(error).toMatch(/[\u4e00-\u9fff]/)
    if (value) expect(error).not.toContain(value)
  })
})
