import { useMemo } from 'react'
import { Link } from 'react-router'
import { useTranslation } from 'react-i18next'
import { formatNumber } from '../../i18n/format'
import { resolveInitialLanguage } from '../../i18n/language'
import type { ClusterSnapshot } from '../../api/types'
import { StatusBadge } from '../../components/StatusBadge'
import { StaleNotice } from '../../components/StaleNotice'
import { ApiErrorNotice } from '../clusters/ClusterForm'
import { LagLineChart } from '../charts/LagLineChart'
import { useConsumerGroupLag } from '../consumer-groups/useConsumerGroupLag'
import { rowHealth } from '../consumer-groups/status'
import { useDashboard } from '../dashboard/useDashboard'
import { useSnapshotStale } from '../dashboard/useSnapshotStale'

// 在概览卡片和历史曲线生成时汇总已知 Lag，同时保留不完整和完全未知的区别。
function totalLag(snapshot: ClusterSnapshot) {
  const known = snapshot.consumerGroups.reduce((sum, row) => sum + row.lag, 0)
  const collectionError = snapshot.consumerGroups.some(row => rowHealth(row, snapshot).collectionError)
    || snapshot.topics.some(topic => topic.incomplete)
    || snapshot.partialFailures.some(failure => ['groups', 'topics', 'committed', 'watermark'].includes(failure.scope))
  const incomplete = collectionError || snapshot.consumerGroups.some(row => row.incomplete || row.unknownPartitionCount > 0)
  return { known, incomplete, collectionError, available: !incomplete || snapshot.consumerGroups.some(row => row.partitions.some(partition => partition.lag !== null)) }
}

// 在概览快照更新时跨 Topic 汇总消费组 Lag，并给未知分区标记不完整。
function LagDistribution({ snapshot }: { snapshot: ClusterSnapshot }) {
  const { t, i18n } = useTranslation()
  const language = resolveInitialLanguage({ stored: i18n.resolvedLanguage ?? null, browserLanguages: [] })
  const groups = new Map<string, { name: string; known: number; incomplete: boolean }>()
  for (const row of snapshot.consumerGroups) {
    const group = groups.get(row.groupId) ?? { name: row.groupId, known: 0, incomplete: false }
    group.known += row.lag
    group.incomplete ||= row.incomplete || row.unknownPartitionCount > 0 || rowHealth(row, snapshot).collectionError
    groups.set(row.groupId, group)
  }
  const sorted = [...groups.values()].sort((a, b) => b.known - a.known || a.name.localeCompare(b.name))
  const items = sorted.length > 4 ? [...sorted.slice(0, 3), {
    name: t('overview.others'), known: sorted.slice(3).reduce((sum, group) => sum + group.known, 0),
    incomplete: sorted.slice(3).some(group => group.incomplete),
  }] : sorted
  const total = sorted.reduce((sum, group) => sum + group.known, 0)
  return <section className="panel distribution" aria-label={t('overview.distribution')}>
    <div className="panel-head"><h3>{t('overview.distribution')}</h3><span>{t('overview.byGroup')}</span></div>
    <div className="distribution-body">
      {items.map((group, index) => <div className="distribution-item" key={group.name}>
        <div><span title={group.name}>{group.name}</span><b>{formatNumber(group.known, language)}</b></div>
        {group.incomplete && <small>{t('overview.knownUnknownPartitions')}</small>}
        <div className="distribution-track"><span style={{ width: `${total ? group.known / total * 100 : 0}%`, background: ['var(--red)', 'var(--amber)', '#a78bfa', '#65748b'][index] }} /></div>
      </div>)}
      {!items.length && <p className="empty-state">{snapshot.partialFailures.some(item => item.scope === 'groups') ? t('overview.distributionFailed') : t('overview.noGroupLag')}</p>}
    </div>
  </section>
}

// 在概览路由渲染时组合紧凑指标、历史趋势、消费组分布和待关注表格。
export function OverviewPage() {
  const { t, i18n } = useTranslation()
  const language = resolveInitialLanguage({ stored: i18n.resolvedLanguage ?? null, browserLanguages: [] })
  const { cluster, monitor, refreshSeconds = 5 } = useDashboard()
  const lag = useConsumerGroupLag()
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const samples = useMemo(() => monitor.history.map(sample => {
    const value = totalLag(sample.snapshot)
    return { at: sample.at, lag: value.incomplete ? null : value.known }
  }), [monitor.history])
  const snapshot = monitor.snapshot
  const display = monitor.displaySnapshot ?? snapshot
  if (!cluster) return <section className="page-panel" aria-label={t('overview.content')}><h2>{t('overview.title')}</h2><p>{t('overview.selectCluster')}</p></section>
  if (!snapshot) return <section className="page-panel" aria-label={t('overview.content')}><h2>{t('overview.title')}</h2>
    {monitor.status === 'connecting' || monitor.isFetching || lag.isPending
      ? <div role="status" aria-label={t('overview.loadingLabel')} className="overview-skeleton"><p>{t('overview.loading')}</p><div /><div /><div /><div /></div>
      : <p>{t('overview.noData')}</p>}
  </section>
  const total = totalLag(snapshot)
  const abnormal = lag.rows.filter(row => row.state.status !== 'normal')
    .sort((a, b) => (b.displayRow ?? b.row).lag - (a.displayRow ?? a.row).lag || a.key.localeCompare(b.key))
  const partitions = display!.topics.reduce((sum, topic) => sum + topic.partitions.length, 0)
  return <section className="page-panel" aria-label={t('overview.content')}>
    <div className="page-head"><div><h2>{t('overview.title')}</h2><p>{t('overview.description', { cluster: cluster.name })}</p></div>
      {stale ? <span role="status">{t('shared.stale')}</span> : <span className={`status-badge ${total.collectionError ? 'status-error' : 'status-normal'}`}>● {total.collectionError ? t('overview.partialCollection') : t('overview.collectionNormal')}</span>}
    </div>
    <div className="overview-metrics">
      <section aria-label={t('overview.topicCount')}><h3>{t('overview.topics')}</h3><p>{formatNumber(display!.topics.length, language)}</p><small>{t('overview.partitions', { count: partitions, value: formatNumber(partitions, language) })}{snapshot.partialFailures.some(failure => failure.scope === 'topics') ? t('overview.incompleteRetained') : ''}</small></section>
      <section aria-label={t('overview.groupCount')}><h3>{t('overview.groups')}</h3><p>{formatNumber(new Set(display!.consumerGroupIds).size, language)}</p><small>{t('overview.discoveredGroups')}{snapshot.partialFailures.some(failure => failure.scope === 'groups') ? t('overview.incompleteRetained') : ''}</small></section>
      <section aria-label={t('overview.totalLagLabel')}><h3>{t('overview.totalLag')}</h3><p>{total.available ? formatNumber(total.known, language) : '—'}</p> <small>{total.incomplete ? t('overview.knownIncomplete') : t('overview.knownTotal')}</small></section>
      <section aria-label={t('overview.abnormalCount')}><h3>{t('overview.abnormalGroups')}</h3><p className={abnormal.length ? 'text-amber' : ''}>{lag.ready ? formatNumber(new Set(abnormal.map(item => item.row.groupId)).size, language) : '—'}</p><small>{t('overview.currentRules')}{snapshot.partialFailures.some(failure => failure.scope === 'groups') ? t('overview.incomplete') : ''}</small></section>
    </div>
    <div className="overview-grid"><LagLineChart samples={samples} stale={stale || total.collectionError} /><LagDistribution snapshot={snapshot} /></div>
    <section className="panel attention-panel"><div className="panel-head"><h3>{t('overview.attention')}</h3><Link className="ghost-link" to="/consumer-groups">{t('overview.viewAll')}</Link></div>
    {lag.isPending && <p role="status">{t('overview.loadingRules')}</p>}
    <ApiErrorNotice error={lag.error} />
    {lag.ready && (abnormal.length ? <div className="table-scroll"><table aria-label={t('overview.abnormalTable')}><thead><tr>
      <th scope="col">{t('overview.group')}</th><th scope="col">{t('overview.topic')}</th><th scope="col">{t('overview.status')}</th><th scope="col">{t('overview.partitionsHeader')}</th><th scope="col">{t('overview.totalLagLabel')}</th><th scope="col">{t('overview.change')}</th>
    </tr></thead><tbody>{abnormal.map(({ row, displayRow = row, staleAt, key, health, state, trend }) => <tr key={key}>
      <td><Link className="name" aria-label={`${row.groupId} / ${row.topic}`} to={`/consumer-groups?${new URLSearchParams({ groupId: row.groupId, topic: row.topic })}`}>{row.groupId}{' '}<span className="sr-only">/ {row.topic}</span></Link></td><td>{row.topic}</td>
      <td><StatusBadge status={state.status} />{(stale || health.collectionError) && <StaleNotice at={staleAt} />}</td><td>{formatNumber(displayRow.partitions.length, language)}</td><td>{formatNumber(displayRow.lag, language)}{row.unknownPartitionCount ? t('overview.unknownCount', { value: formatNumber(row.unknownPartitionCount, language) }) : ''}{health.collectionError ? t('overview.incomplete') : ''}</td>
      <td className={trend === 'up' ? 'text-red' : trend === 'down' ? 'text-green' : ''}>{trend === 'up' ? '↗' : trend === 'down' ? '↘' : trend === 'flat' ? '→' : '—'}</td>
    </tr>)}</tbody></table></div> : <p className="empty-state">{t('overview.noAttention')}</p>)}
    </section>
  </section>
}
