import { useMemo } from 'react'
import { Link } from 'react-router'
import type { ClusterSnapshot } from '../../api/types'
import { StatusBadge } from '../../components/StatusBadge'
import { StaleNotice } from '../../components/StaleNotice'
import { ApiErrorNotice } from '../clusters/ClusterForm'
import { LagLineChart } from '../charts/LagLineChart'
import { useConsumerGroupLag } from '../consumer-groups/useConsumerGroupLag'
import { rowHealth } from '../consumer-groups/status'
import { useDashboard } from '../dashboard/useDashboard'
import { useSnapshotStale } from '../dashboard/useSnapshotStale'

function totalLag(snapshot: ClusterSnapshot) {
  const known = snapshot.consumerGroups.reduce((sum, row) => sum + row.lag, 0)
  const collectionError = snapshot.consumerGroups.some(row => rowHealth(row, snapshot).collectionError)
    || snapshot.topics.some(topic => topic.incomplete)
    || snapshot.partialFailures.some(failure => ['groups', 'topics', 'committed', 'watermark'].includes(failure.scope))
  const incomplete = collectionError || snapshot.consumerGroups.some(row => row.incomplete || row.unknownPartitionCount > 0)
  return { known, incomplete, collectionError, available: !incomplete || snapshot.consumerGroups.some(row => row.partitions.some(partition => partition.lag !== null)) }
}

function LagDistribution({ snapshot }: { snapshot: ClusterSnapshot }) {
  const groups = new Map<string, { name: string; known: number; incomplete: boolean }>()
  for (const row of snapshot.consumerGroups) {
    const group = groups.get(row.groupId) ?? { name: row.groupId, known: 0, incomplete: false }
    group.known += row.lag
    group.incomplete ||= row.incomplete || row.unknownPartitionCount > 0 || rowHealth(row, snapshot).collectionError
    groups.set(row.groupId, group)
  }
  const sorted = [...groups.values()].sort((a, b) => b.known - a.known || a.name.localeCompare(b.name))
  const items = sorted.length > 4 ? [...sorted.slice(0, 3), {
    name: '其他', known: sorted.slice(3).reduce((sum, group) => sum + group.known, 0),
    incomplete: sorted.slice(3).some(group => group.incomplete),
  }] : sorted
  const total = sorted.reduce((sum, group) => sum + group.known, 0)
  return <section className="panel distribution" aria-label="Lag 分布">
    <div className="panel-head"><h3>Lag 分布</h3><span>按消费组</span></div>
    <div className="distribution-body">
      {items.map((group, index) => <div className="distribution-item" key={group.name}>
        <div><span title={group.name}>{group.name}</span><b>{group.known.toLocaleString('en-US')}</b></div>
        {group.incomplete && <small>已知 Lag · 含未知分区</small>}
        <div className="distribution-track"><span style={{ width: `${total ? group.known / total * 100 : 0}%`, background: ['var(--red)', 'var(--amber)', '#a78bfa', '#65748b'][index] }} /></div>
      </div>)}
      {!items.length && <p className="empty-state">{snapshot.partialFailures.some(item => item.scope === 'groups') ? '消费组采集失败，分布未知。' : '暂无消费组 Lag 数据。'}</p>}
    </div>
  </section>
}

export function OverviewPage() {
  const { cluster, monitor, refreshSeconds = 5 } = useDashboard()
  const lag = useConsumerGroupLag()
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const samples = useMemo(() => monitor.history.map(sample => {
    const value = totalLag(sample.snapshot)
    return { at: sample.at, lag: value.incomplete ? null : value.known }
  }), [monitor.history])
  const snapshot = monitor.snapshot
  const display = monitor.displaySnapshot ?? snapshot
  if (!cluster) return <section className="page-panel" aria-label="概览内容"><h2>集群概览</h2><p>请选择集群。</p></section>
  if (!snapshot) return <section className="page-panel" aria-label="概览内容"><h2>集群概览</h2>
    {monitor.status === 'connecting' || monitor.isFetching || lag.isPending
      ? <div role="status" aria-label="正在加载概览" className="overview-skeleton"><p>正在加载概览…</p><div /><div /><div /><div /></div>
      : <p>尚未采集概览数据。</p>}
  </section>
  const total = totalLag(snapshot)
  const abnormal = lag.rows.filter(row => row.state.status !== 'normal')
    .sort((a, b) => (b.displayRow ?? b.row).lag - (a.displayRow ?? a.row).lag || a.key.localeCompare(b.key))
  const partitions = display!.topics.reduce((sum, topic) => sum + topic.partitions.length, 0)
  return <section className="page-panel" aria-label="概览内容">
    <div className="page-head"><div><h2>集群概览</h2><p>{cluster.name} 的实时消费状态</p></div>
      {stale ? <span role="status">数据已陈旧</span> : <span className={`status-badge ${total.collectionError ? 'status-error' : 'status-normal'}`}>● {total.collectionError ? '部分采集异常' : '采集正常'}</span>}
    </div>
    <div className="overview-metrics">
      <section aria-label="Topic 数"><h3>Topics</h3><p>{display!.topics.length.toLocaleString('en-US')}</p><small>共 {partitions.toLocaleString('en-US')} 个 Partitions{snapshot.partialFailures.some(failure => failure.scope === 'topics') ? '（不完整，含陈旧展示值）' : ''}</small></section>
      <section aria-label="Consumer Group 数"><h3>Consumer Groups</h3><p>{new Set(display!.consumerGroupIds).size.toLocaleString('en-US')}</p><small>当前已发现的消费组{snapshot.partialFailures.some(failure => failure.scope === 'groups') ? '（不完整，含陈旧展示值）' : ''}</small></section>
      <section aria-label="总 Lag"><h3>Total Lag</h3><p>{total.available ? total.known.toLocaleString('en-US') : '—'}</p> <small>{total.incomplete ? '已知 Lag（不完整，未知值未计入）' : '各消费组已知 Lag 汇总'}</small></section>
      <section aria-label="异常 Consumer Group 数"><h3>异常消费组</h3><p className={abnormal.length ? 'text-amber' : ''}>{lag.ready ? new Set(abnormal.map(item => item.row.groupId)).size : '—'}</p><small>按当前生效规则判定{snapshot.partialFailures.some(failure => failure.scope === 'groups') ? '（不完整）' : ''}</small></section>
    </div>
    <div className="overview-grid"><LagLineChart samples={samples} stale={stale || total.collectionError} /><LagDistribution snapshot={snapshot} /></div>
    <section className="panel attention-panel"><div className="panel-head"><h3>需要关注的消费组</h3><Link className="ghost-link" to="/consumer-groups">查看全部 →</Link></div>
    {lag.isPending && <p role="status">正在加载 Lag 规则…</p>}
    <ApiErrorNotice error={lag.error} />
    {lag.ready && (abnormal.length ? <div className="table-scroll"><table aria-label="异常 Consumer Groups"><thead><tr>
      <th scope="col">Consumer Group</th><th scope="col">Topic</th><th scope="col">状态</th><th scope="col">Partitions</th><th scope="col">总 Lag</th><th scope="col">变化</th>
    </tr></thead><tbody>{abnormal.map(({ row, displayRow = row, staleAt, key, health, state, trend }) => <tr key={key}>
      <td><Link className="name" aria-label={`${row.groupId} / ${row.topic}`} to={`/consumer-groups?${new URLSearchParams({ groupId: row.groupId, topic: row.topic })}`}>{row.groupId}{' '}<span className="sr-only">/ {row.topic}</span></Link></td><td>{row.topic}</td>
      <td><StatusBadge status={state.status} />{(stale || health.collectionError) && <StaleNotice at={staleAt} />}</td><td>{displayRow.partitions.length}</td><td>{displayRow.lag.toLocaleString('en-US')}{row.unknownPartitionCount ? ` + ${row.unknownPartitionCount} 未知` : ''}{health.collectionError ? '（不完整）' : ''}</td>
      <td className={trend === 'up' ? 'text-red' : trend === 'down' ? 'text-green' : ''}>{trend === 'up' ? '↗' : trend === 'down' ? '↘' : trend === 'flat' ? '→' : '—'}</td>
    </tr>)}</tbody></table></div> : <p className="empty-state">暂无异常 Consumer Groups。</p>)}
    </section>
  </section>
}
