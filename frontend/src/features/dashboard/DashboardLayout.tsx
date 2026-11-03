import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { LanguageSwitch } from '../../components/LanguageSwitch'
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
  const { t, i18n } = useTranslation()
  // 按当前集群挂载监控 Hook，由布局共享连接，子路由切换仍复用采样与规则上下文。
  const monitor = useMonitorSession(cluster?.id ?? null, refreshSeconds)
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const labels = { idle: t('dashboard.connection.idle'), connecting: t('dashboard.connection.connecting'), connected: t('dashboard.connection.connected'), paused: t('dashboard.connection.paused'), error: t('dashboard.connection.error') }
  return <><header className="topbar">
    <div className="cluster-picker">
      <select aria-label={t('dashboard.cluster')} value={cluster?.id ?? ''} onChange={event => onCluster(event.target.value)}>
        {!cluster && <option value="">{t('dashboard.selectCluster')}</option>}
        {clusters.map(item => <option key={item.id} value={item.id}>{item.name}</option>)}
      </select>
    <p role="status" data-connection={monitor.status}>{labels[monitor.status]}</p>
    </div>
    <div className="topbar-actions">
    {stale && <p role="status" aria-label={t('dashboard.staleLabel')}>{t('dashboard.stale')}</p>}
    <span className="updated">{t('dashboard.updatedAt')} {monitor.snapshot
      ? <time aria-label={t('dashboard.lastUpdated')} dateTime={monitor.snapshot.collectedAt} title={new Date(monitor.snapshot.collectedAt).toLocaleString(i18n.resolvedLanguage)}>{new Date(monitor.snapshot.collectedAt).toLocaleTimeString(i18n.resolvedLanguage)}</time>
      : t('dashboard.notCollected')}</span>
    <label>{t('dashboard.refresh')}<select aria-label={t('dashboard.refreshInterval')} value={refreshSeconds} onChange={event => onInterval(Number(event.target.value) as RefreshSeconds)}>
      <option value={3}>{t('dashboard.seconds', { count: 3 })}</option><option value={5}>{t('dashboard.seconds', { count: 5 })}</option><option value={10}>{t('dashboard.seconds', { count: 10 })}</option>
    </select></label>
    <button className="icon-button" type="button" aria-label={t('dashboard.manualRefresh')} title={t('dashboard.refreshNow')} disabled={monitor.status !== 'connected' || monitor.isFetching} onClick={() => void monitor.refresh()}>↻</button>
    {monitor.status === 'error' && <button type="button" onClick={monitor.reconnect}>{t('dashboard.reconnect')}</button>}
    <LanguageSwitch />
    </div></header>
    <ApiErrorNotice error={monitor.error} />
    {Boolean(monitor.snapshot?.partialFailures.length) && <p role="alert">{t('dashboard.partialFailure')}</p>}
    <ConsumerGroupLagProvider clusterId={cluster?.id ?? null} monitor={monitor}>
      <div className="page-content"><Outlet context={{ cluster, monitor, refreshSeconds } satisfies DashboardContext} /></div>
    </ConsumerGroupLagProvider>
  </>
}

// 进入监控路由时加载集群和刷新设置，选中集群变化后为新集群重建监控组件。
export function DashboardLayout() {
  const { t } = useTranslation()
  // 布局加载或集群列表重新查询时执行 queryFn，透传取消信号，避免离开后继续等待列表。
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => clustersApi.list(signal) })
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [refreshSeconds, setRefreshSeconds] = useState<RefreshSeconds>(5)
  // 列表更新或选择变化时优先使用有效选择，否则回退到首个配置，避免引用已删除集群。
  const cluster = clusters.data?.find(item => item.id === selectedId) ?? clusters.data?.[0] ?? null
  // Monitor 仅以集群标识为 key：切换集群才释放会话；语言重渲染必须复用实例以保留连接与历史。
  return <section className="dashboard-shell" aria-label={t('dashboard.label')}>
    {clusters.isPending && <p role="status">{t('dashboard.loadingClusters')}</p>}
    <ApiErrorNotice error={clusters.error} />
    {clusters.data?.length === 0 && <p>{t('dashboard.noClusters')}<Link to="/settings">{t('dashboard.configureCluster')}</Link></p>}
    <Monitor key={cluster?.id ?? ''} cluster={cluster} clusters={clusters.data ?? []} refreshSeconds={refreshSeconds} onCluster={setSelectedId} onInterval={setRefreshSeconds} />
  </section>
}
