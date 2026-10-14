import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { Link, Outlet } from 'react-router'
import { clustersApi } from '../../api/client'
import type { ClusterDto } from '../../api/types'
import { ApiErrorNotice } from '../clusters/ClusterForm'
import { ConsumerGroupLagProvider } from '../consumer-groups/ConsumerGroupLagProvider'
import { useMonitorSession, type RefreshSeconds } from './useMonitorSession'
import type { DashboardContext } from './useDashboard'
import { useSnapshotStale } from './useSnapshotStale'

export type { DashboardContext } from './useDashboard'

function Monitor({ cluster, refreshSeconds }: { cluster: ClusterDto | null; refreshSeconds: RefreshSeconds }) {
  const monitor = useMonitorSession(cluster?.id ?? null, refreshSeconds)
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const labels = { idle: '未连接', connecting: '正在连接…', connected: '已连接', paused: '已暂停（页面不可见）', error: '连接已断开，显示最后一次快照' }
  return <>
    <p role="status">{labels[monitor.status]}</p>
    {stale && <p role="status" aria-label="监控数据已陈旧">数据已陈旧</p>}
    <p>最后更新时间：{monitor.snapshot
      ? <time aria-label="最后更新时间" dateTime={monitor.snapshot.collectedAt}>{new Date(monitor.snapshot.collectedAt).toLocaleString()}</time>
      : '尚未采集'}</p>
    <button type="button" disabled={monitor.status !== 'connected' || monitor.isFetching} onClick={() => void monitor.refresh()}>手动刷新</button>
    {monitor.status === 'error' && <button type="button" onClick={monitor.reconnect}>重新连接</button>}
    <ApiErrorNotice error={monitor.error} />
    {Boolean(monitor.snapshot?.partialFailures.length) && <p role="alert">部分数据采集失败；未知值和不完整数据已保留。</p>}
    <ConsumerGroupLagProvider clusterId={cluster?.id ?? null} monitor={monitor}>
      <Outlet context={{ cluster, monitor, refreshSeconds } satisfies DashboardContext} />
    </ConsumerGroupLagProvider>
  </>
}

export function DashboardLayout() {
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => clustersApi.list(signal) })
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [refreshSeconds, setRefreshSeconds] = useState<RefreshSeconds>(5)
  const cluster = clusters.data?.find(item => item.id === selectedId) ?? clusters.data?.[0] ?? null
  return <section aria-label="监控面板">
    <header>
      <label>集群<select aria-label="集群" value={cluster?.id ?? ''} onChange={event => setSelectedId(event.target.value)}>
        {!cluster && <option value="">请选择集群</option>}
        {clusters.data?.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}
      </select></label>
      <label>刷新频率<select aria-label="刷新频率" value={refreshSeconds} onChange={event => setRefreshSeconds(Number(event.target.value) as RefreshSeconds)}>
        <option value={3}>3 秒</option><option value={5}>5 秒</option><option value={10}>10 秒</option>
      </select></label>
    </header>
    {clusters.isPending && <p role="status">正在加载集群…</p>}
    <ApiErrorNotice error={clusters.error} />
    {clusters.data?.length === 0 && <p>尚未配置集群。<Link to="/settings">配置集群</Link></p>}
    <Monitor key={cluster?.id ?? ''} cluster={cluster} refreshSeconds={refreshSeconds} />
  </section>
}
