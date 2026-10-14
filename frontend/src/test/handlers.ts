import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'

export const clusterFixture = {
  id: 'cluster-a', name: '生产 Kafka', bootstrapServers: 'broker:9092',
  securityProtocol: 'SASL_SSL' as const, saslMechanism: 'PLAIN', username: 'operator',
  connectionTimeoutMs: 10000, defaultNormalLagMax: 999, defaultHighLagMin: 5000,
  defaultOverLimitSeconds: 30, createdAt: '2026-10-15T00:00:00Z', updatedAt: '2026-10-15T00:00:00Z',
}

export const rulesFixture = {
  defaults: { normalMax: 999, highMin: 5000, overLimitSeconds: 30 }, overrides: [],
}

export const snapshotFixture = {
  clusterId: 'cluster-a', collectedAt: '2026-10-15T00:00:00Z', topics: [],
  consumerGroupIds: ['flink-order-enrich', 'audit-worker', 'no-offsets'],
  consumerGroups: [
    { groupId: 'flink-order-enrich', topic: 'raw-orders', lag: 5, unknownPartitionCount: 0, incomplete: false,
      partitions: [{ partition: 0, high: 10, committedOffset: 5, lag: 5 }] },
    { groupId: 'flink-order-enrich', topic: 'uncommitted', lag: 0, unknownPartitionCount: 1, incomplete: true,
      partitions: [{ partition: 0, high: 10, committedOffset: null, lag: null }] },
    { groupId: 'audit-worker', topic: 'audit-log', lag: 1, unknownPartitionCount: 0, incomplete: false,
      partitions: [{ partition: 0, high: 10, committedOffset: 9, lag: 1 }] },
  ], partialFailures: [],
}

export const handlers = [
  http.get('/api/clusters', () => HttpResponse.json([clusterFixture])),
  http.get('/api/clusters/:id/lag-rules', () => HttpResponse.json(rulesFixture)),
]

export const server = setupServer(...handlers)
