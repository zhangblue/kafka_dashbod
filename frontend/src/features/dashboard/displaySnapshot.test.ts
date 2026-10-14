import { expect, it } from 'vitest'
import type { ClusterSnapshot } from '../../api/types'
import { retainDisplaySnapshot, groupDisplayKey, topicDisplayKey, groupPartitionDisplayKey, topicPartitionDisplayKey } from './displaySnapshot'

const complete: ClusterSnapshot = {
  clusterId: 'a', collectedAt: '2026-10-15T00:00:00Z', consumerGroupIds: ['worker'], partialFailures: [],
  topics: [{ topic: 'orders', low: 0, latestMessageOffset: 99, retainedOffsetSpan: 200, incomplete: false, partitions: [
    { partition: 0, low: 0, high: 100, latestMessageOffset: 99, retainedOffsetSpan: 100 },
    { partition: 1, low: 0, high: 100, latestMessageOffset: 99, retainedOffsetSpan: 100 },
  ] }],
  consumerGroups: [{ groupId: 'worker', topic: 'orders', lag: 100, unknownPartitionCount: 0, incomplete: false, partitions: [
    { partition: 0, high: 100, committedOffset: 50, lag: 50 }, { partition: 1, high: 100, committedOffset: 50, lag: 50 },
  ] }],
}
const failure = (scope: string, resource: string) => ({ scope, resource, code: 'KAFKA_TIMEOUT', message: 'timeout' })

it.each(['topics', 'groups'])('retains rows lost to %s discovery and preserves their original successful time across repeated failures', scope => {
  const prior = retainDisplaySnapshot(complete)
  const raw = { ...complete, collectedAt: '2026-10-15T00:00:05Z', consumerGroups: [],
    topics: scope === 'topics' ? [] : complete.topics, consumerGroupIds: scope === 'groups' ? [] : complete.consumerGroupIds,
    partialFailures: [failure(scope, 'a')] }
  const display = retainDisplaySnapshot(raw, prior)
  expect(display.snapshot.consumerGroups[0].lag).toBe(100)
  expect(display.staleResources[groupDisplayKey('worker', 'orders')]).toBe(complete.collectedAt)
  if (scope === 'topics') expect(display.staleResources[topicDisplayKey('orders')]).toBe(complete.collectedAt)
  const again = retainDisplaySnapshot({ ...raw, collectedAt: '2026-10-15T00:00:10Z' }, display)
  expect(again.staleResources[groupDisplayKey('worker', 'orders')]).toBe(complete.collectedAt)
  expect(raw.consumerGroups).toEqual([])
  expect(retainDisplaySnapshot(complete, display).staleResources).toEqual({})
})

it('retains failed committed positions while preserving healthy current partitions and raw unknowns', () => {
  const raw: ClusterSnapshot = { ...complete, collectedAt: '2026-10-15T00:00:05Z', partialFailures: [failure('committed', 'worker/orders:0')],
    consumerGroups: [{ ...complete.consumerGroups[0], lag: 20, incomplete: true, unknownPartitionCount: 1, partitions: [
      { partition: 0, high: 100, committedOffset: null, lag: null }, { partition: 1, high: 100, committedOffset: 80, lag: 20 },
    ] }] }
  const display = retainDisplaySnapshot(raw, retainDisplaySnapshot(complete))
  expect(display.snapshot.consumerGroups[0].partitions.map(partition => partition.lag)).toEqual([50, 20])
  expect(display.snapshot.consumerGroups[0].lag).toBe(70)
  expect(display.staleResources[groupPartitionDisplayKey('worker', 'orders', 0)]).toBe(complete.collectedAt)
  expect(display.staleResources[groupPartitionDisplayKey('worker', 'orders', 1)]).toBeUndefined()
  expect(raw.consumerGroups[0].lag).toBe(20)
  expect(raw.consumerGroups[0].partitions[0].committedOffset).toBeNull()
})

it('retains a failed partition watermark in topic and consumer displays without replacing current known totals', () => {
  const raw: ClusterSnapshot = { ...complete, collectedAt: '2026-10-15T00:00:05Z', partialFailures: [failure('watermark', 'orders:0')],
    topics: [{ ...complete.topics[0], low: 10, latestMessageOffset: 109, retainedOffsetSpan: 100, incomplete: true, partitions: [
      { partition: 0, low: null, high: null, latestMessageOffset: null, retainedOffsetSpan: null },
      { partition: 1, low: 10, high: 110, latestMessageOffset: 109, retainedOffsetSpan: 100 },
    ] }],
    consumerGroups: [{ ...complete.consumerGroups[0], lag: 20, incomplete: true, unknownPartitionCount: 1, partitions: [
      { partition: 0, high: null, committedOffset: 60, lag: null }, { partition: 1, high: 110, committedOffset: 90, lag: 20 },
    ] }] }
  const display = retainDisplaySnapshot(raw, retainDisplaySnapshot(complete))
  expect(display.snapshot.topics[0].partitions.map(partition => partition.high)).toEqual([100, 110])
  expect(display.snapshot.consumerGroups[0].partitions.map(partition => partition.lag)).toEqual([50, 20])
  expect(display.staleResources[topicPartitionDisplayKey('orders', 0)]).toBe(complete.collectedAt)
  expect(display.staleResources[groupDisplayKey('worker', 'orders')]).toBe(complete.collectedAt)
  expect(raw.consumerGroups[0].lag).toBe(20)
})

it('drops previous rows after successful discovery confirms deletion and isolates clusters', () => {
  const empty = { ...complete, topics: [], consumerGroupIds: [], consumerGroups: [] }
  expect(retainDisplaySnapshot(empty, retainDisplaySnapshot(complete)).snapshot.consumerGroups).toEqual([])
  expect(retainDisplaySnapshot({ ...empty, clusterId: 'b', partialFailures: [failure('topics', 'b')] }, retainDisplaySnapshot(complete)).snapshot.topics).toEqual([])
})

it('retains whole-group committed failures using the collector group resource form', () => {
  const raw: ClusterSnapshot = { ...complete, collectedAt: '2026-10-15T00:00:05Z', partialFailures: [failure('committed', 'worker')],
    consumerGroups: [{ ...complete.consumerGroups[0], lag: 0, incomplete: true, unknownPartitionCount: 2,
      partitions: complete.consumerGroups[0].partitions.map(partition => ({ ...partition, committedOffset: null, lag: null })) }] }
  const display = retainDisplaySnapshot(raw, retainDisplaySnapshot(complete))
  expect(display.snapshot.consumerGroups[0].lag).toBe(100)
  expect(Object.keys(display.staleResources)).toHaveLength(3)
  expect(raw.consumerGroups[0].lag).toBe(0)
})
