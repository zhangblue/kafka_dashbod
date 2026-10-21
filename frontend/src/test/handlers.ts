import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'
import type { TopicConfigsResponse } from '../api/types'

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

export const topicConfigsFixture: TopicConfigsResponse = { configs: [
  { key: 'retention.ms', value: '604800000', source: 'topicOverride', readOnly: false },
  { key: 'retention.bytes', value: '-1', source: 'brokerDefault', readOnly: false },
  { key: 'cleanup.policy', value: 'delete', source: 'topicOverride', readOnly: false },
  { key: 'segment.ms', value: '3600000', source: 'brokerDefault', readOnly: false },
  { key: 'segment.bytes', value: '1073741824', source: 'brokerDefault', readOnly: false },
  { key: 'min.insync.replicas', value: '2', source: 'topicOverride', readOnly: true },
  { key: 'max.message.bytes', value: '1048588', source: 'brokerDefault', readOnly: false },
  { key: 'compression.type', value: 'producer', source: 'brokerDefault', readOnly: false },
] }

export const handlers = [
  http.get('/api/clusters', () => HttpResponse.json([clusterFixture])),
  http.get('/api/clusters/:id/lag-rules', () => HttpResponse.json(rulesFixture)),
  http.post('/api/sessions/:id/topics', () => HttpResponse.json({ status: 'created' }, { status: 201 })),
  http.get('/api/sessions/:id/topics/:topic/configs', () => HttpResponse.json(topicConfigsFixture)),
]

export const server = setupServer(...handlers)
