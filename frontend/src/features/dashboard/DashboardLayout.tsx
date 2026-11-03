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

// 仪表盘布局渲染当前集群时管理监控生命周期，向顶部控制栏和子页面提供同一快照。
function Monitor({ cluster, clusters, refreshSeconds, onCluster, onInterval }: {
  cluster: ClusterDto | null; clusters: ClusterDto[]; refreshSeconds: RefreshSeconds
  onCluster: (id: string) => void; onInterval: (seconds: RefreshSeconds) => void
}) {
  // 按当前集群挂载监控 Hook，由布局共享连接，子路由切换仍复用采样与规则上下文。
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

// 进入监控路由时加载集群和刷新设置，选中集群变化后为新集群重建监控组件。
export function DashboardLayout() {
  // 布局加载或集群列表重新查询时执行 queryFn，透传取消信号，避免离开后继续等待列表。
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => clustersApi.list(signal) })
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [refreshSeconds, setRefreshSeconds] = useState<RefreshSeconds>(5)
  // 列表更新或选择变化时优先使用有效选择，否则回退到首个配置，避免引用已删除集群。
  const cluster = clusters.data?.find(item => item.id === selectedId) ?? clusters.data?.[0] ?? null
  // Monitor 以集群标识为 key：切换时卸载旧实例并释放会话，隔离新集群快照与历史。
  return <section className="dashboard-shell" aria-label="监控面板">
    {clusters.isPending && <p role="status">正在加载集群…</p>}
    <ApiErrorNotice error={clusters.error} />
    {clusters.data?.length === 0 && <p>尚未配置集群。<Link to="/settings">配置集群</Link></p>}
    <Monitor key={cluster?.id ?? ''} cluster={cluster} clusters={clusters.data ?? []} refreshSeconds={refreshSeconds} onCluster={setSelectedId} onInterval={setRefreshSeconds} />
  </section>
}
