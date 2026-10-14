import type { ClusterSnapshot, ConsumerGroupTopicSnapshot, LagRuleSet, LagThresholds } from '../../api/types'

export type LagStatus = 'normal' | 'backlog' | 'high' | 'uncommitted' | 'error'
export const statusLabels: Record<LagStatus, string> = {
  normal: '正常', backlog: '积压', high: '高 Lag', uncommitted: '未提交', error: '采集异常',
}
export interface LagState { status: LagStatus; overLimitMs: number; overLimit: boolean }
export const initialLagState = (): LagState => ({ status: 'normal', overLimitMs: 0, overLimit: false })

export function evaluateLag(input: { lag: number; elapsedVisibleMs: number; rule: LagThresholds;
  collectionError?: boolean; uncommitted?: boolean }, previous: LagState) {
  const { lag, rule, collectionError, uncommitted } = input
  if (collectionError || uncommitted) return { state: { ...initialLagState(), status: collectionError ? 'error' : 'uncommitted' } as LagState }
  if (lag <= rule.normalMax) return { state: initialLagState() }
  const overLimitMs = previous.overLimitMs + Math.max(0, input.elapsedVisibleMs)
  const status: LagStatus = overLimitMs < rule.overLimitSeconds * 1000 ? 'normal' : lag >= rule.highMin ? 'high' : 'backlog'
  return { state: { status, overLimitMs, overLimit: true } }
}

export function resolveLagRule(rules: LagRuleSet, groupId: string, topic: string) {
  const override = rules.overrides.find(rule => rule.groupId === groupId && rule.topic === topic)
  return { rule: override ?? rules.defaults, custom: Boolean(override) }
}

export const lagRowKey = (clusterId: string, groupId: string, topic: string) => JSON.stringify([clusterId, groupId, topic])

export function hasCollectionFailure(row: ConsumerGroupTopicSnapshot, snapshot: ClusterSnapshot) {
  return snapshot.partialFailures.some(failure => {
    const resource = failure.resource
    if (failure.scope === 'committed') return resource === row.groupId
      || row.partitions.some(p => resource === `${row.groupId}/${row.topic}:${p.partition}`)
    if (failure.scope === 'watermark' || failure.scope === 'metadata') return row.partitions.some(p => resource === `${row.topic}:${p.partition}`)
    if (failure.scope === 'topics') return resource === snapshot.clusterId || resource === 'cluster' || resource === row.topic
      || row.partitions.some(p => resource === `${row.topic}:${p.partition}`)
    if (failure.scope === 'groups') return resource === snapshot.clusterId || resource === 'cluster' || resource === 'unknown' || resource === row.groupId
    return false
  })
}

export function rowHealth(row: ConsumerGroupTopicSnapshot, snapshot: ClusterSnapshot) {
  const uncommittedCount = row.partitions.filter(p => p.committedOffset === null || p.committedOffset < 0).length
  const unknownCount = row.partitions.filter(p => p.lag === null).length
  // Backend incomplete also covers uncommitted partitions. Only that fully explained case is not a collection error.
  const collectionError = hasCollectionFailure(row, snapshot) || snapshot.topics.some(topic => topic.topic === row.topic && topic.incomplete)
    || row.partitions.some(p => p.high === null || (p.lag === null && p.committedOffset !== null && p.committedOffset >= 0))
    || row.unknownPartitionCount !== unknownCount
    || (row.incomplete && (!uncommittedCount || unknownCount !== uncommittedCount))
  return { collectionError, uncommitted: uncommittedCount > 0, uncommittedCount,
    validCount: row.partitions.filter(p => p.lag !== null).length }
}

export interface TrackedLag { state: LagState; lag: number; trend: 'up' | 'down' | 'flat' | null }
export interface LagStates { continuityKey: string | null; clusterId: string | null; at: number | null;
  seenAt: number | null; rulesKey: string | null; rows: Record<string, TrackedLag> }
export const resetAllLagStates = (): LagStates => ({ continuityKey: null, clusterId: null, at: null, seenAt: null, rulesKey: null, rows: {} })

export function advanceLagStates(input: { snapshot: ClusterSnapshot | null; status: string; continuityKey: string;
  rules: LagRuleSet }, previous: LagStates): LagStates {
  const { snapshot, status, continuityKey, rules } = input
  if (!snapshot) return resetAllLagStates()
  const rulesKey = JSON.stringify(rules)
  const at = Date.parse(snapshot.collectedAt)
  if (!Number.isFinite(at)) return resetAllLagStates()
  const seenAt = previous.clusterId === snapshot.clusterId ? previous.seenAt : null
  const reset = { ...resetAllLagStates(), clusterId: snapshot.clusterId, continuityKey, rulesKey, seenAt }
  // Keep a deduplication watermark even after clearing timers. Resuming can expose the old cached snapshot.
  if (status !== 'connected') return { ...reset, seenAt: Math.max(seenAt ?? at, at) }
  const continuous = previous.continuityKey === continuityKey && previous.clusterId === snapshot.clusterId && previous.rulesKey === rulesKey
  const before = continuous ? previous : reset
  if (seenAt !== null && at <= seenAt) return before
  const rows: Record<string, TrackedLag> = {}
  for (const row of snapshot.consumerGroups) {
    const key = lagRowKey(snapshot.clusterId, row.groupId, row.topic)
    const prior = before.rows[key]
    const health = rowHealth(row, snapshot)
    const elapsedVisibleMs = prior?.state.overLimit && before.at !== null ? at - before.at : 0
    const { state } = evaluateLag({ lag: row.lag, elapsedVisibleMs, rule: resolveLagRule(rules, row.groupId, row.topic).rule, ...health }, prior?.state ?? initialLagState())
    const comparable = prior && !health.collectionError && !health.uncommitted && prior.state.status !== 'error' && prior.state.status !== 'uncommitted'
    rows[key] = { state, lag: row.lag, trend: comparable ? row.lag > prior.lag ? 'up' : row.lag < prior.lag ? 'down' : 'flat' : null }
  }
  return { continuityKey, clusterId: snapshot.clusterId, at, seenAt: at, rulesKey, rows }
}
