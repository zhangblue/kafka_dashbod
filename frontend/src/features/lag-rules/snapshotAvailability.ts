import type { ClusterSnapshot } from '../../api/types'

// 在复用快照或建立配置会话后识别 Topics 与 Groups 同时发现失败的不可用状态。
export function snapshotUnavailable(snapshot: ClusterSnapshot) {
  // 在判断快照不可用时检查当前集群的指定发现范围是否失败。
  const discoveryFailed = (scope: string) => snapshot.partialFailures
    .some(failure => failure.scope === scope && failure.resource === snapshot.clusterId)
  return snapshot.topics.length === 0 && snapshot.consumerGroupIds.length === 0
    && snapshot.consumerGroups.length === 0 && discoveryFailed('topics') && discoveryFailed('groups')
}
