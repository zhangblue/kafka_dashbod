import type { ClusterSnapshot } from '../../api/types'

export const topicDisplayKey = (topic: string) => JSON.stringify(['topic', topic])
export const topicPartitionDisplayKey = (topic: string, partition: number) => JSON.stringify(['topic', topic, partition])
export const groupDisplayKey = (group: string, topic: string) => JSON.stringify(['group', group, topic])
export const groupPartitionDisplayKey = (group: string, topic: string, partition: number) => JSON.stringify(['group', group, topic, partition])
export interface DisplaySnapshot { snapshot: ClusterSnapshot; staleResources: Record<string, string> }
export interface MonitorDisplay {
  displaySnapshot?: ClusterSnapshot | null
  staleResources?: Record<string, string>
}

/** Presentation only. Never feed retained values into history or lag evaluation. */
export function retainDisplaySnapshot(current: ClusterSnapshot, previous?: DisplaySnapshot): DisplaySnapshot {
  if (!previous || previous.snapshot.clusterId !== current.clusterId) return { snapshot: current, staleResources: {} }
  const before = previous.snapshot
  const staleResources: Record<string, string> = {}
  const fails = (scope: string, resources: string[]) => current.partialFailures.some(failure => failure.scope === scope && resources.includes(failure.resource))
  const topicFailed = (topic: string) => fails('topics', [current.clusterId, 'cluster', topic])
  const groupFailed = (group: string) => fails('groups', [current.clusterId, 'cluster', 'unknown', group])
  const watermarkFailed = (topic: string, partition: number) => topicFailed(topic)
    || ['watermark', 'metadata', 'topics'].some(scope => fails(scope, [`${topic}:${partition}`]))
  const committedFailed = (group: string, topic: string, partition: number) => groupFailed(group)
    || watermarkFailed(topic, partition) || fails('committed', [group, `${group}/${topic}:${partition}`])
  const retain = (key: string) => {
    const at = previous.staleResources[key] ?? before.collectedAt
    staleResources[key] = at
    return at
  }
  const parentTime = (key: string, childKeys: string[]) => {
    const times = childKeys.flatMap(child => staleResources[child] ? [staleResources[child]] : [])
    if (times.length) staleResources[key] = times.sort()[0]
  }
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
  const consumerGroups = merge(current.consumerGroups, before.consumerGroups,
    row => groupDisplayKey(row.groupId, row.topic), row => groupFailed(row.groupId) || topicFailed(row.topic)).map(row => {
    const old = before.consumerGroups.find(value => value.groupId === row.groupId && value.topic === row.topic)
    if (!old) return row
    const partitions = merge(row.partitions, old.partitions, partition => partition.partition,
      partition => committedFailed(row.groupId, row.topic, partition.partition)).map(partition => {
      const prior = old.partitions.find(value => value.partition === partition.partition)
      if (!prior || !committedFailed(row.groupId, row.topic, partition.partition)) return partition
      retain(groupPartitionDisplayKey(row.groupId, row.topic, partition.partition))
      return prior
    })
    parentTime(groupDisplayKey(row.groupId, row.topic), partitions.map(partition => groupPartitionDisplayKey(row.groupId, row.topic, partition.partition)))
    if ((groupFailed(row.groupId) || topicFailed(row.topic)) && !partitions.length) retain(groupDisplayKey(row.groupId, row.topic))
    return { ...row, partitions, lag: partitions.reduce((sum, partition) => sum + (partition.lag ?? 0), 0) }
  })
  const consumerGroupIds = [...new Set([...current.consumerGroupIds, ...before.consumerGroupIds.filter(group => groupFailed(group))])]
  return { snapshot: { ...current, topics, consumerGroups, consumerGroupIds }, staleResources }
}
