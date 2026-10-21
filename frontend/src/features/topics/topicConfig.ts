export interface TopicConfigDefinition {
  key: string
  label: string
  description: string
  risk: string
  unit: string | null
  inputType: 'number' | 'select'
  options?: { value: string; label: string }[]
}

export const topicConfigDefinitions: TopicConfigDefinition[] = [
  {
    key: 'retention.ms', label: '消息保留时间', unit: '毫秒', inputType: 'number',
    description: '消息按保留策略允许保留的最长时间，-1 表示不限时，0 表示立即满足时间清理条件。',
    risk: '缩短保留时间可能使已有日志段更早被删除，删除后无法恢复。',
  },
  {
    key: 'retention.bytes', label: '分区保留空间', unit: '字节 / Partition', inputType: 'number',
    description: '每个 Partition 的日志保留空间上限，并非 Topic 总量；-1 表示不限空间。',
    risk: '降低空间上限可能触发旧日志段清理，删除后无法恢复。',
  },
  {
    key: 'cleanup.policy', label: '清理策略', unit: null, inputType: 'select',
    description: 'delete 按保留规则删除旧日志段，compact 按 Key 保留最新值，也可同时启用。',
    risk: '更换清理策略会改变历史数据保留方式；压缩可能移除同 Key 的旧值。',
    options: [
      { value: 'delete', label: '按保留规则删除（delete）' },
      { value: 'compact', label: '按 Key 压缩（compact）' },
      { value: 'delete,compact', label: '删除并压缩（delete,compact）' },
    ],
  },
  {
    key: 'segment.ms', label: '日志段最大时间', unit: '毫秒', inputType: 'number',
    description: '达到该时间后滚动日志段，使保留和压缩策略有机会处理已关闭的旧日志段。',
    risk: '设置过小会增加日志段和文件数量，带来额外磁盘管理开销。',
  },
  {
    key: 'segment.bytes', label: '日志段最大大小', unit: '字节', inputType: 'number',
    description: '日志段达到该大小后滚动为新段，保留与压缩策略随后可处理旧日志段。',
    risk: '设置过小会增加日志段滚动频率和文件数量。',
  },
  {
    key: 'min.insync.replicas', label: '最少同步副本数', unit: '个', inputType: 'number',
    description: '生产者使用 acks=all 时，一次写入所需的最少同步副本数量。',
    risk: '设置过高可能在副本故障时阻止写入；应与副本因子和可用性要求一起评估。',
  },
  {
    key: 'max.message.bytes', label: '最大消息批次', unit: '字节', inputType: 'number',
    description: 'Topic 接受的最大消息批次大小，生产者和消费者也需要配置兼容的上限。',
    risk: '调整上限可能导致写入或读取失败，应同时核对客户端与 Broker 限制。',
  },
  {
    key: 'compression.type', label: '压缩方式', unit: null, inputType: 'select',
    description: '指定 Topic 最终使用的压缩方式，producer 表示保留生产者选择的压缩方式。',
    risk: '更换压缩方式会影响 CPU、网络和存储开销，也需确认客户端兼容性。',
    options: [
      { value: 'producer', label: '沿用生产者（producer）' },
      { value: 'gzip', label: 'gzip' },
      { value: 'snappy', label: 'snappy' },
      { value: 'lz4', label: 'lz4' },
      { value: 'zstd', label: 'zstd' },
      { value: 'uncompressed', label: '不压缩（uncompressed）' },
    ],
  },
]

export function validateTopicConfigValue(key: string, value: string): string | undefined {
  const definition = topicConfigDefinitions.find(item => item.key === key)
  if (!definition) return '不支持此 Topic 配置。'
  if (definition.inputType === 'select') {
    return definition.options?.some(option => option.value === value) ? undefined : '请选择有效的配置选项。'
  }
  const retention = key === 'retention.ms' || key === 'retention.bytes'
  if (retention && value === '-1') return undefined
  if (/^[0-9]+$/.test(value)) {
    const number = BigInt(value)
    if (number <= 9223372036854775807n && (retention ? number >= 0n : number > 0n)) return undefined
  }
  return '请输入有效范围内的十进制整数。'
}
