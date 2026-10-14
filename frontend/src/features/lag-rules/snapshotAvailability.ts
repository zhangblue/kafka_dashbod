import type { ClusterSnapshot } from '../../api/types'

export function snapshotUnavailable(snapshot: ClusterSnapshot) {
  const discoveryFailed = (scope: string) => snapshot.partialFailures
    .some(failure => failure.scope === scope && failure.resource === snapshot.clusterId)
  return snapshot.topics.length === 0 && snapshot.consumerGroupIds.length === 0
    && snapshot.consumerGroups.length === 0 && discoveryFailed('topics') && discoveryFailed('groups')
}
