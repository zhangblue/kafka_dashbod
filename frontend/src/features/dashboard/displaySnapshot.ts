import type { ClusterSnapshot } from '../../api/types'

// 生成 Topic 陈旧标记时编码资源身份，避免名称分隔符造成键冲突。
export const topicDisplayKey = (topic: string) => JSON.stringify(['topic', topic])
// 生成 Topic 分区陈旧标记时组合名称与分区编号。
export const topicPartitionDisplayKey = (topic: string, partition: number) => JSON.stringify(['topic', topic, partition])
// 生成消费组与 Topic 关联行的陈旧标记时组合两个身份。
export const groupDisplayKey = (group: string, topic: string) => JSON.stringify(['group', group, topic])
// 生成消费组分区陈旧标记时组合消费组、Topic 与分区编号。
export const groupPartitionDisplayKey = (group: string, topic: string, partition: number) => JSON.stringify(['group', group, topic, partition])
export interface DisplaySnapshot { snapshot: ClusterSnapshot; staleResources: Record<string, string> }
export interface MonitorDisplay {
  displaySnapshot?: ClusterSnapshot | null
  staleResources?: Record<string, string>
}

/** 新快照到达时仅为展示保留采集失败的旧资源；保留值不得进入历史或 Lag 判定。 */
export function retainDisplaySnapshot(current: ClusterSnapshot, previous?: DisplaySnapshot): DisplaySnapshot {
  if (!previous || previous.snapshot.clusterId !== current.clusterId) return { snapshot: current, staleResources: {} }
  const before = previous.snapshot
  const staleResources: Record<string, string> = {}
  // 合并快照时匹配采集失败范围与资源名，确定是否允许保留旧值。
  const fails = (scope: string, resources: string[]) => current.partialFailures.some(failure => failure.scope === scope && resources.includes(failure.resource))
  // 合并 Topic 时判断该 Topic 或整个发现阶段是否失败。
  const topicFailed = (topic: string) => fails('topics', [current.clusterId, 'cluster', topic])
  // 合并消费组时识别具体组、集群及未知组发现失败。
  const groupFailed = (group: string) => fails('groups', [current.clusterId, 'cluster', 'unknown', group])
  // 合并分区时汇总 Topic 发现及水位、元数据失败，判断旧水位是否可展示。
  const watermarkFailed = (topic: string, partition: number) => topicFailed(topic)
    || ['watermark', 'metadata', 'topics'].some(scope => fails(scope, [`${topic}:${partition}`]))
  // 合并消费分区时结合发现、水位与提交位置失败，判断旧 Lag 是否可展示。
  const committedFailed = (group: string, topic: string, partition: number) => groupFailed(group)
    || watermarkFailed(topic, partition) || fails('committed', [group, `${group}/${topic}:${partition}`])
  // 采用旧资源时延续首次陈旧时间，避免连续失败把保留值伪装成新采样。
  const retain = (key: string) => {
    const at = previous.staleResources[key] ?? before.collectedAt
    staleResources[key] = at
    return at
  }
  // 子分区合并后将最早陈旧时间传播到父行，提示汇总含有保留值。
  const parentTime = (key: string, childKeys: string[]) => {
    const times = childKeys.flatMap(child => staleResources[child] ? [staleResources[child]] : [])
    if (times.length) staleResources[key] = times.sort()[0]
  }
  // 合并当前与旧资源列表时，仅补回明确由失败遮蔽且当前缺失的资源。
  function merge<T>(now: T[], old: T[], key: (value: T) => string | number, retainMissing: (value: T) => boolean) {
    return [...now, ...old.filter(item => !now.some(value => key(value) === key(item)) && retainMissing(item))]
  }
  const topics = merge(current.topics, before.topics, topic => topic.topic, topic => topicFailed(topic.topic)).map(topic => {
    const old = before.topics.find(value => value.topic === topic.topic)
    if (!old) return topic
    const partitions = merge(topic.partitions, old.partitions, partition => partition.partition,
      partition => watermarkFailed(topic.topic, partition.partition)).map(partition => {
      const prior = old.partitions.find(value => value.partition === partition.partition)
      if (!prior || !watermarkFailed(topic.topic, partition.partition)) return partition
      retain(topicPartitionDisplayKey(topic.topic, partition.partition))
      return prior
    })
    parentTime(topicDisplayKey(topic.topic), partitions.map(partition => topicPartitionDisplayKey(topic.topic, partition.partition)))
    if (topicFailed(topic.topic) && !partitions.length) retain(topicDisplayKey(topic.topic))
    return { ...topic, partitions }
  })
  const currentGroups = new Set(current.consumerGroupIds)
  const currentTopics = new Set(current.topics.map(topic => topic.topic))
  const consumerGroups = merge(current.consumerGroups, before.consumerGroups,
    row => groupDisplayKey(row.groupId, row.topic), row => {
      const groupUnknown = groupFailed(row.groupId), topicUnknown = topicFailed(row.topic)
      const associationUnknown = fails('committed', [row.groupId,
        ...row.partitions.map(partition => `${row.groupId}/${row.topic}:${partition.partition}`)])
      // 单一发现维度失败不能复活另一维度已确认删除的资源；提交读取失败也可能遮蔽已知非活跃组关联。
      return (groupUnknown || topicUnknown || associationUnknown)
        && (currentGroups.has(row.groupId) || groupUnknown)
        && (currentTopics.has(row.topic) || topicUnknown)
    }).map(row => {
    const old = before.consumerGroups.find(value => value.groupId === row.groupId && value.topic === row.topic)
    if (!old) return row
    const rowMissing = !current.consumerGroups.some(value => value.groupId === row.groupId && value.topic === row.topic)
    const partitions = merge(row.partitions, old.partitions, partition => partition.partition,
      partition => committedFailed(row.groupId, row.topic, partition.partition)).map(partition => {
      const prior = old.partitions.find(value => value.partition === partition.partition)
      if (!prior || (!rowMissing && !committedFailed(row.groupId, row.topic, partition.partition))) return partition
      retain(groupPartitionDisplayKey(row.groupId, row.topic, partition.partition))
      return prior
    })
    parentTime(groupDisplayKey(row.groupId, row.topic), partitions.map(partition => groupPartitionDisplayKey(row.groupId, row.topic, partition.partition)))
    if ((rowMissing || groupFailed(row.groupId) || topicFailed(row.topic)) && !partitions.length) retain(groupDisplayKey(row.groupId, row.topic))
    return { ...row, partitions, lag: partitions.reduce((sum, partition) => sum + (partition.lag ?? 0), 0) }
  })
  const consumerGroupIds = [...new Set([...current.consumerGroupIds, ...before.consumerGroupIds.filter(group => groupFailed(group))])]
  return { snapshot: { ...current, topics, consumerGroups, consumerGroupIds }, staleResources }
}
