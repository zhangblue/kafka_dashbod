import { useTranslation } from 'react-i18next'
import { Fragment, useId, useState } from 'react'
import { useSearchParams } from 'react-router'
import type { ClusterSnapshot, ConsumerGroupTopicSnapshot } from '../../api/types'
import { MetricHelp } from '../../components/MetricHelp'
import { StatusBadge } from '../../components/StatusBadge'
import { StaleNotice } from '../../components/StaleNotice'
import { groupPartitionDisplayKey, topicPartitionDisplayKey } from '../dashboard/displaySnapshot'
import { ApiErrorNotice } from '../clusters/ClusterForm'
import { useDashboard } from '../dashboard/useDashboard'
import type { MonitorSession } from '../dashboard/useMonitorSession'
import { lagRowKey, statusTranslationKeys, type LagStatus } from './status'
import { useConsumerGroupLag, type ConsumerGroupLagRow } from './useConsumerGroupLag'
import { useSnapshotStale } from '../dashboard/useSnapshotStale'
import { ConsumerGroupHistory } from './ConsumerGroupHistory'

// 在消费进度表渲染时格式化已知整数，保留未知值标记。
const format = (value: number | null) => value === null ? '—' : value.toLocaleString('en-US')

// 在展开 Group 与 Topic 明细时关联分区水位、提交位置和局部陈旧状态。
function PartitionTable({ row, snapshot, staleResources }: { row: ConsumerGroupTopicSnapshot; snapshot: ClusterSnapshot; staleResources: Record<string, string> | undefined }) {
  const { t } = useTranslation()
  return <table aria-label={t('consumerGroups.partitionTable', { group: row.groupId, topic: row.topic })}><thead><tr>
    <th scope="col">{t('consumerGroups.partition')}</th>
    <th scope="col">{t('consumerGroups.high')} <MetricHelp label={t('consumerGroups.highLabel', { group: row.groupId, topic: row.topic })}>{t('consumerGroups.highHelp')}</MetricHelp></th>
    <th scope="col">{t('consumerGroups.maximum')} <MetricHelp label={t('consumerGroups.maximumLabel', { group: row.groupId, topic: row.topic })}>{t('consumerGroups.maximumHelp')}</MetricHelp></th>
    <th scope="col">{t('consumerGroups.committed')} <MetricHelp label={t('consumerGroups.committedLabel', { group: row.groupId, topic: row.topic })}>{t('consumerGroups.committedHelp')}</MetricHelp></th>
    <th scope="col">{t('consumerGroups.lag')} <MetricHelp label={t('consumerGroups.partitionLagLabel', { group: row.groupId, topic: row.topic })}>{t('consumerGroups.partitionLagHelp')}</MetricHelp></th>
  </tr></thead><tbody>{row.partitions.map(partition => {
    const watermark = snapshot.topics.find(topic => topic.topic === row.topic)?.partitions.find(item => item.partition === partition.partition)
    const max = watermark?.latestMessageOffset ?? null
    const watermarkStaleAt = staleResources?.[topicPartitionDisplayKey(row.topic, partition.partition)]
    return <tr key={partition.partition}>
    <td>{partition.partition}</td><td>{format(partition.high)}</td><td>{format(max)}{watermarkStaleAt && <StaleNotice at={watermarkStaleAt} />}</td><td>{format(partition.committedOffset === null || partition.committedOffset < 0 ? null : partition.committedOffset)}</td><td>{format(partition.lag)}{staleResources?.[groupPartitionDisplayKey(row.groupId, row.topic, partition.partition)] && <StaleNotice at={staleResources[groupPartitionDisplayKey(row.groupId, row.topic, partition.partition)]} />}</td>
  </tr>})}</tbody></table>
}

// 在规则加载完成后展示和筛选消费组状态，按链接展开精确的 Group 与 Topic 分区。
function ConsumerGroupsTable({ clusterId, monitor, evaluatedRows }: { clusterId: string; monitor: MonitorSession; evaluatedRows: ConsumerGroupLagRow[] }) {
  const { t } = useTranslation()
  const [params] = useSearchParams()
  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState<LagStatus | 'all' | 'abnormal'>('all')
  const { cluster } = useDashboard()
  const [expanded, setExpanded] = useState<Set<string>>(() => {
    const groupId = params.get('groupId'), topic = params.get('topic')
    return new Set(groupId && topic ? [lagRowKey(clusterId, groupId, topic)] : [])
  })
  const id = useId()
  const { snapshot, status } = monitor
  const rows = evaluatedRows.filter(({ row, state }) => (filter === 'all' || (filter === 'abnormal' ? state.status !== 'normal' : state.status === filter))
    && `${row.groupId}\n${row.topic}`.toLocaleLowerCase().includes(search.trim().toLocaleLowerCase()))
    .sort((a, b) => (b.displayRow ?? b.row).lag - (a.displayRow ?? a.row).lag || a.key.localeCompare(b.key))
  return <>
    <div className="page-head"><div><h2>{t('consumerGroups.title')}</h2><p>{t('consumerGroups.subtitle')}</p></div>
    <div className="toolbar"><label className="search-label"><span className="sr-only">{t('consumerGroups.search')}</span><input type="search" placeholder={t('consumerGroups.searchPlaceholder')} value={search} onChange={/* 在搜索内容变化时更新当前过滤词，仅筛选已取得的快照。 */ event => setSearch(event.target.value)} /></label>
    <div className="filter-tabs" aria-label={t('consumerGroups.quickFilter')}>{([['all', 'consumerGroups.all'], ['abnormal', 'consumerGroups.abnormal'], ['uncommitted', 'status.uncommitted']] as const).map(([value, label]) => <button type="button" key={value} aria-pressed={filter === value} onClick={/* 在状态筛选变化时更新筛选条件，保留共享的 Lag 判定与采样连续性。 */ () => setFilter(value)}>{t(label)}</button>)}</div>
    </div></div>
    <div className="offset-note rules-note"><span>{cluster && t('consumerGroups.defaultRule', { normal: cluster.defaultNormalLagMax.toLocaleString('en-US'), high: cluster.defaultHighLagMin.toLocaleString('en-US'), seconds: cluster.defaultOverLimitSeconds })}</span>
    <label><span className="sr-only">{t('consumerGroups.statusFilter')}</span><select value={filter} onChange={/* 在状态筛选变化时更新筛选条件，保留共享的 Lag 判定与采样连续性。 */ event => setFilter(event.target.value as typeof filter)}>
      <option value="all">{t('consumerGroups.allStatuses')}</option><option value="abnormal">{t('consumerGroups.allAbnormal')}</option>{Object.entries(statusTranslationKeys).map(([value, label]) => <option key={value} value={value}>{t(label)}</option>)}
    </select></label></div>
    {status !== 'connected' && <p role="status">{t('consumerGroups.paused')}</p>}
    {!!snapshot?.partialFailures.length && <p role="status">{t('consumerGroups.partial')}</p>}
    {!snapshot ? <p>{t('consumerGroups.notCollected')}</p> : <>
      <div className="panel table-scroll"><table aria-label={t('consumerGroups.summary')}><thead><tr>
        <th scope="col">{t('consumerGroups.group')}</th><th scope="col">{t('consumerGroups.topic')}</th>
        <th scope="col">{t('consumerGroups.status')} <MetricHelp label={t('consumerGroups.status')}>{t('consumerGroups.statusHelp')}</MetricHelp></th>
        <th scope="col">{t('consumerGroups.validPartitions')} <MetricHelp label={t('consumerGroups.validPartitions')}>{t('consumerGroups.validHelp')}</MetricHelp></th>
        <th scope="col">{t('consumerGroups.uncommittedPartitions')} <MetricHelp label={t('consumerGroups.uncommittedPartitions')}>{t('consumerGroups.uncommittedHelp')}</MetricHelp></th>
        <th scope="col">{t('consumerGroups.totalLag')} <MetricHelp label={t('consumerGroups.totalLag')}>{t('consumerGroups.totalHelp')}</MetricHelp></th>
        <th scope="col">{t('consumerGroups.trend')} <MetricHelp label={t('consumerGroups.trend')}>{t('consumerGroups.trendHelp')}</MetricHelp></th>
      </tr></thead><tbody>{rows.map(({ row, displayRow = row, staleAt, key, health, resolved, state, trend }, index) => {
        const open = expanded.has(key)
        const detailsId = `${id}-partitions-${index}`
        return <Fragment key={key}><tr>
          <td><button className="expand-button" type="button" aria-label={t(open ? 'consumerGroups.collapse' : 'consumerGroups.expand', { group: row.groupId, topic: row.topic })} aria-expanded={open} aria-controls={open ? detailsId : undefined}
            onClick={/* 在展开按钮点击时切换当前资源明细，不改变后台采样会话。 */ () => setExpanded(previous => { const next = new Set(previous); if (open) next.delete(key); else next.add(key); return next })}>{open ? '⌄' : '›'}</button> <span className="name">{row.groupId}</span></td>
          <td>{row.topic}</td><td><StatusBadge status={state.status} />{staleAt && <StaleNotice at={staleAt} />}{resolved.custom && <span> <MetricHelp trigger={t('consumerGroups.customRule')} label={t('consumerGroups.customRuleLabel', { group: row.groupId, topic: row.topic })}>
            {t('consumerGroups.customRuleDescription', { normal: format(resolved.rule.normalMax), high: format(resolved.rule.highMin), seconds: resolved.rule.overLimitSeconds })}
          </MetricHelp></span>}</td>
          <td>{health.validCount}</td><td>{health.uncommittedCount}</td>
          <td>{format(displayRow.lag)}{row.unknownPartitionCount > 0 ? t('consumerGroups.unknownCount', { value: row.unknownPartitionCount }) : ''}{health.collectionError ? t('consumerGroups.incomplete') : ''}</td>
          <td>{trend ? <span aria-label={t(`consumerGroups.trends.${trend}`)}>{trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→'}</span> : '—'}</td>
        </tr>{open && <tr className="detail-row"><td colSpan={7} id={detailsId}><PartitionTable row={displayRow} snapshot={monitor.displaySnapshot ?? snapshot} staleResources={monitor.staleResources} /></td></tr>}</Fragment>
      })}</tbody></table></div>
      {!rows.length && <p>{t(snapshot.consumerGroups.length ? 'consumerGroups.noMatch' : 'consumerGroups.empty')}</p>}
    </>}
  </>
}

// 在 Consumer Groups 路由渲染时组合规则状态、分区明细和页面内历史趋势。
export function ConsumerGroupsPage() {
  const { t } = useTranslation()
  const { cluster, monitor, refreshSeconds } = useDashboard()
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const lag = useConsumerGroupLag()
  return <section className="page-panel" aria-label={t('consumerGroups.content')}>
    {!lag.ready && <div className="page-head"><div><h2>{t('consumerGroups.title')}</h2><p>{t('consumerGroups.subtitle')}</p></div></div>}
    {stale && <p role="status" aria-label={t('consumerGroups.staleLabel')}>{t('consumerGroups.stale')}</p>}
    {!cluster ? <p>{t('consumerGroups.selectCluster')}</p> : <>
      {lag.isPending && <p role="status">{t('consumerGroups.loadingRules')}</p>}
      <ApiErrorNotice error={lag.error} />
      {lag.ready && <ConsumerGroupsTable key={cluster.id} clusterId={cluster.id} monitor={monitor} evaluatedRows={lag.rows} />}
      <ConsumerGroupHistory key={`history-${cluster.id}`} snapshot={monitor.snapshot} history={monitor.history} stale={stale} />
    </>}
  </section>
}
