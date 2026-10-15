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

function Monitor({ cluster, clusters, refreshSeconds, onCluster, onInterval }: {
  cluster: ClusterDto | null; clusters: ClusterDto[]; refreshSeconds: RefreshSeconds
  onCluster: (id: string) => void; onInterval: (seconds: RefreshSeconds) => void
}) {
  const monitor = useMonitorSession(cluster?.id ?? null, refreshSeconds)
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const labels = { idle: '未连接', connecting: '正在连接…', connected: '已连接', paused: '已暂停（页面不可见）', error: '连接已断开，显示最后一次快照' }
  return <><header className="topbar">
    <div className="cluster-picker">
      <select aria-label="集群" value={cluster?.id ?? ''} onChange={event => onCluster(event.target.value)}>
        {!cluster && <option value="">请选择集群</option>}
        {clusters.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}
      </select>
    <p role="status" data-connection={monitor.status}>{labels[monitor.status]}</p>
    </div>
    <div className="topbar-actions">
    {stale && <p role="status" aria-label="监控数据已陈旧">数据已陈旧</p>}
    <span className="updated">更新于 {monitor.snapshot
      ? <time aria-label="最后更新时间" dateTime={monitor.snapshot.collectedAt} title={new Date(monitor.snapshot.collectedAt).toLocaleString()}>{new Date(monitor.snapshot.collectedAt).toLocaleTimeString()}</time>
      : '尚未采集'}</span>
    <label>刷新<select aria-label="刷新频率" value={refreshSeconds} onChange={event => onInterval(Number(event.target.value) as RefreshSeconds)}>
      <option value={3}>3 秒</option><option value={5}>5 秒</option><option value={10}>10 秒</option>
    </select></label>
    <button className="icon-button" type="button" aria-label="手动刷新" title="立即刷新" disabled={monitor.status !== 'connected' || monitor.isFetching} onClick={() => void monitor.refresh()}>↻</button>
    {monitor.status === 'error' && <button type="button" onClick={monitor.reconnect}>重新连接</button>}
    </div></header>
    <ApiErrorNotice error={monitor.error} />
    {Boolean(monitor.snapshot?.partialFailures.length) && <p role="alert">部分数据采集失败；未知值和不完整数据已保留。</p>}
    <ConsumerGroupLagProvider clusterId={cluster?.id ?? null} monitor={monitor}>
      <div className="page-content"><Outlet context={{ cluster, monitor, refreshSeconds } satisfies DashboardContext} /></div>
    </ConsumerGroupLagProvider>
  </>
}

export function DashboardLayout() {
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => clustersApi.list(signal) })
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [refreshSeconds, setRefreshSeconds] = useState<RefreshSeconds>(5)
  const cluster = clusters.data?.find(item => item.id === selectedId) ?? clusters.data?.[0] ?? null
  return <section className="dashboard-shell" aria-label="监控面板">
    {clusters.isPending && <p role="status">正在加载集群…</p>}
    <ApiErrorNotice error={clusters.error} />
    {clusters.data?.length === 0 && <p>尚未配置集群。<Link to="/settings">配置集群</Link></p>}
    <Monitor key={cluster?.id ?? ''} cluster={cluster} clusters={clusters.data ?? []} refreshSeconds={refreshSeconds} onCluster={setSelectedId} onInterval={setRefreshSeconds} />
  </section>
}
