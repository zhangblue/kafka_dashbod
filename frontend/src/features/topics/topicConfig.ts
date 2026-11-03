import type { ParseKeys } from 'i18next'

type TopicConfigTranslationKey = Extract<ParseKeys, `topicAdmin.${string}`>
export type TopicConfigValidationKey = 'topicAdmin.validation.unsupported' | 'topicAdmin.validation.invalidOption' | 'topicAdmin.validation.invalidInteger'

export interface TopicConfigDefinition {
  key: string
  labelKey: TopicConfigTranslationKey
  descriptionKey: TopicConfigTranslationKey
  riskKey: TopicConfigTranslationKey
  unitKey: TopicConfigTranslationKey | null
  inputType: 'number' | 'select'
  options?: { value: string; labelKey: TopicConfigTranslationKey }[]
}

export const topicConfigDefinitions: TopicConfigDefinition[] = [
  {
    key: 'retention.ms', labelKey: 'topicAdmin.config.retentionMs.label', unitKey: 'topicAdmin.units.milliseconds', inputType: 'number',
    descriptionKey: 'topicAdmin.config.retentionMs.description',
    riskKey: 'topicAdmin.config.retentionMs.risk',
  },
  {
    key: 'retention.bytes', labelKey: 'topicAdmin.config.retentionBytes.label', unitKey: 'topicAdmin.units.bytesPerPartition', inputType: 'number',
    descriptionKey: 'topicAdmin.config.retentionBytes.description',
    riskKey: 'topicAdmin.config.retentionBytes.risk',
  },
  {
    key: 'cleanup.policy', labelKey: 'topicAdmin.config.cleanupPolicy.label', unitKey: null, inputType: 'select',
    descriptionKey: 'topicAdmin.config.cleanupPolicy.description',
    riskKey: 'topicAdmin.config.cleanupPolicy.risk',
    options: [
      { value: 'delete', labelKey: 'topicAdmin.options.delete' },
      { value: 'compact', labelKey: 'topicAdmin.options.compact' },
      { value: 'delete,compact', labelKey: 'topicAdmin.options.both' },
    ],
  },
  {
    key: 'segment.ms', labelKey: 'topicAdmin.config.segmentMs.label', unitKey: 'topicAdmin.units.milliseconds', inputType: 'number',
    descriptionKey: 'topicAdmin.config.segmentMs.description',
    riskKey: 'topicAdmin.config.segmentMs.risk',
  },
  {
    key: 'segment.bytes', labelKey: 'topicAdmin.config.segmentBytes.label', unitKey: 'topicAdmin.units.bytes', inputType: 'number',
    descriptionKey: 'topicAdmin.config.segmentBytes.description',
    riskKey: 'topicAdmin.config.segmentBytes.risk',
  },
  {
    key: 'min.insync.replicas', labelKey: 'topicAdmin.config.minInsyncReplicas.label', unitKey: 'topicAdmin.units.count', inputType: 'number',
    descriptionKey: 'topicAdmin.config.minInsyncReplicas.description',
    riskKey: 'topicAdmin.config.minInsyncReplicas.risk',
  },
  {
    key: 'max.message.bytes', labelKey: 'topicAdmin.config.maxMessageBytes.label', unitKey: 'topicAdmin.units.bytes', inputType: 'number',
    descriptionKey: 'topicAdmin.config.maxMessageBytes.description',
    riskKey: 'topicAdmin.config.maxMessageBytes.risk',
  },
  {
    key: 'compression.type', labelKey: 'topicAdmin.config.compressionType.label', unitKey: null, inputType: 'select',
    descriptionKey: 'topicAdmin.config.compressionType.description',
    riskKey: 'topicAdmin.config.compressionType.risk',
    options: [
      { value: 'producer', labelKey: 'topicAdmin.options.producer' },
      { value: 'gzip', labelKey: 'topicAdmin.options.gzip' },
      { value: 'snappy', labelKey: 'topicAdmin.options.snappy' },
      { value: 'lz4', labelKey: 'topicAdmin.options.lz4' },
      { value: 'zstd', labelKey: 'topicAdmin.options.zstd' },
      { value: 'uncompressed', labelKey: 'topicAdmin.options.uncompressed' },
    ],
  },
]

// 在创建或增量保存参数前按白名单校验值，返回固定翻译键且不读取全局语言或回显用户输入。
export function validateTopicConfigValue(key: string, value: string): TopicConfigValidationKey | undefined {
  const definition = topicConfigDefinitions.find(item => item.key === key)
  if (!definition) return 'topicAdmin.validation.unsupported'
  if (definition.inputType === 'select') {
    return definition.options?.some(option => option.value === value) ? undefined : 'topicAdmin.validation.invalidOption'
  }
  const retention = key === 'retention.ms' || key === 'retention.bytes'
  if (retention && value === '-1') return undefined
  if (/^[0-9]+$/.test(value)) {
    const number = BigInt(value)
    if (number <= 9223372036854775807n && (retention ? number >= 0n : number > 0n)) return undefined
  }
  return 'topicAdmin.validation.invalidInteger'
}
