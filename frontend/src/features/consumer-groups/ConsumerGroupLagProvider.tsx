import { useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { lagRulesApi } from '../../api/client'
import type { MonitorSession } from '../dashboard/useMonitorSession'
import { advanceLagStates, evaluateLag, initialLagState, lagRowKey, resetAllLagStates, resolveLagRule, rowHealth } from './status'
import { ConsumerGroupLagContext } from './useConsumerGroupLag'
import { groupDisplayKey } from '../dashboard/displaySnapshot'

// Lives beside the monitor, above route outlets, so navigation does not break sampling continuity.
export function ConsumerGroupLagProvider({ clusterId, monitor, children }: {
  clusterId: string | null; monitor: MonitorSession; children: ReactNode
}) {
  const query = useQuery({ queryKey: ['lag-rules', clusterId], enabled: Boolean(clusterId),
    queryFn: ({ signal }) => lagRulesApi.get(clusterId!, signal) })
  const rules = query.data
  const { snapshot, status, continuityKey } = monitor
  const [tracking, setTracking] = useState(() => ({ snapshot, status, continuityKey, rules,
    book: rules ? advanceLagStates({ snapshot, status, continuityKey, rules }, resetAllLagStates()) : resetAllLagStates() }))
  let book = tracking.book
  // Only changed sampling inputs are processed; route and UI renders do not replay a sample.
  if (tracking.snapshot !== snapshot || tracking.status !== status || tracking.continuityKey !== continuityKey || tracking.rules !== rules) {
    book = rules ? advanceLagStates({ snapshot, status, continuityKey, rules }, tracking.book) : resetAllLagStates()
    setTracking({ snapshot, status, continuityKey, rules, book })
  }
  const display = monitor.displaySnapshot ?? snapshot
  const rows = rules && snapshot && display ? display.consumerGroups.map(displayRow => {
    const current = snapshot.consumerGroups.find(row => row.groupId === displayRow.groupId && row.topic === displayRow.topic)
    const row = current ?? displayRow
    const key = lagRowKey(snapshot.clusterId, row.groupId, row.topic)
    const staleAt = monitor.staleResources?.[groupDisplayKey(row.groupId, row.topic)]
    const health = { ...rowHealth(row, snapshot) }
    health.collectionError ||= !current || Boolean(staleAt)
    const resolved = resolveLagRule(rules, row.groupId, row.topic)
    const tracked = book.rows[key]
    const state = health.collectionError || status === 'error' ? { ...initialLagState(), status: 'error' as const }
      : tracked?.state ?? evaluateLag({ lag: row.lag, elapsedVisibleMs: 0, rule: resolved.rule,
      ...health }, initialLagState()).state
    return { row, displayRow, key, health, resolved, state, staleAt, trend: health.collectionError ? undefined : tracked?.trend }
  }) : []
  return <ConsumerGroupLagContext.Provider value={{ rows, isPending: query.isPending, error: query.error, ready: Boolean(rules) }}>
    {children}
  </ConsumerGroupLagContext.Provider>
}
