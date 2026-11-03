import { useState, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { lagRulesApi } from '../../api/client'
import type { MonitorSession } from '../dashboard/useMonitorSession'
import { advanceLagStates, evaluateLag, initialLagState, lagRowKey, resetAllLagStates, resolveLagRule, rowHealth } from './status'
import { ConsumerGroupLagContext } from './useConsumerGroupLag'
import { groupDisplayKey } from '../dashboard/displaySnapshot'

// Lives beside the monitor, above route outlets, so navigation does not break sampling continuity.
// 在仪表盘路由上层维护共享规则与连续采样状态，避免页面导航重复累计超限时间。
export function ConsumerGroupLagProvider({ clusterId, monitor, children }: {
  clusterId: string | null; monitor: MonitorSession; children: ReactNode
}) {
  const query = useQuery({ queryKey: ['lag-rules', clusterId], enabled: Boolean(clusterId),
    queryFn: /* 在当前资源查询启动时传递取消信号读取绑定集群的数据，使查询取消能传递到 HTTP 请求。 */ ({ signal }) => lagRulesApi.get(clusterId!, signal) })
  const rules = query.data
  const { snapshot, status, continuityKey } = monitor
  // 首次挂载时从空跟踪簿处理已有采样；规则尚未就绪时保持空状态，避免提前累计超限时间。
  const [tracking, setTracking] = useState(() => ({ snapshot, status, continuityKey, rules,
    book: rules ? advanceLagStates({ snapshot, status, continuityKey, rules }, resetAllLagStates()) : resetAllLagStates() }))
  let book = tracking.book
  // Only changed sampling inputs are processed; route and UI renders do not replay a sample.
  // 仅快照、连接状态、连续性标识或规则引用变化时推进，路由和界面重渲染不重复处理同一输入。
  if (tracking.snapshot !== snapshot || tracking.status !== status || tracking.continuityKey !== continuityKey || tracking.rules !== rules) {
    // 推进函数按采样时间去重，并在断连、会话或规则内容变化时重置连续性；规则缺失时直接清空跟踪簿。
    book = rules ? advanceLagStates({ snapshot, status, continuityKey, rules }, tracking.book) : resetAllLagStates()
    setTracking({ snapshot, status, continuityKey, rules, book })
  }
  const display = monitor.displaySnapshot ?? snapshot
  // 按展示行组装视图，但优先用当前原始采样判定；保留的旧展示值不能充当新的有效样本。
  const rows = rules && snapshot && display ? display.consumerGroups.map(displayRow => {
    const current = snapshot.consumerGroups.find(row => row.groupId === displayRow.groupId && row.topic === displayRow.topic)
    const row = current ?? displayRow
    const key = lagRowKey(snapshot.clusterId, row.groupId, row.topic)
    const staleAt = monitor.staleResources?.[groupDisplayKey(row.groupId, row.topic)]
    const health = { ...rowHealth(row, snapshot) }
    // 原始采样缺行或资源已陈旧时强制标记采集异常，防止旧值维持正常判定或展示有效趋势。
    health.collectionError ||= !current || Boolean(staleAt)
    const resolved = resolveLagRule(rules, row.groupId, row.topic)
    const tracked = book.rows[key]
    // 采集或连接错误优先返回零累计的 error；其余复用已跟踪状态，缺少状态时从零累计判定，避免展示过程续计时。
    const state = health.collectionError || status === 'error' ? { ...initialLagState(), status: 'error' as const }
      : tracked?.state ?? evaluateLag({ lag: row.lag, elapsedVisibleMs: 0, rule: resolved.rule,
      ...health }, initialLagState()).state
    return { row, displayRow, key, health, resolved, state, staleAt, trend: health.collectionError ? undefined : tracked?.trend }
  }) : []
  return <ConsumerGroupLagContext.Provider value={{ rows, isPending: query.isPending, error: query.error, ready: Boolean(rules) }}>
    {children}
  </ConsumerGroupLagContext.Provider>
}
