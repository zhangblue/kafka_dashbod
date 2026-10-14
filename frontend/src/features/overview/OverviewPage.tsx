import { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router'
import type { ClusterSnapshot } from '../../api/types'
import { StatusBadge } from '../../components/StatusBadge'
import { StaleNotice } from '../../components/StaleNotice'
import { ApiErrorNotice } from '../clusters/ClusterForm'
import { LagLineChart } from '../charts/LagLineChart'
import { useConsumerGroupLag } from '../consumer-groups/useConsumerGroupLag'
import { rowHealth } from '../consumer-groups/status'
import { useDashboard } from '../dashboard/useDashboard'

function totalLag(snapshot: ClusterSnapshot) {
  const known = snapshot.consumerGroups.reduce((sum, row) => sum + row.lag, 0)
  const collectionError = snapshot.consumerGroups.some(row => rowHealth(row, snapshot).collectionError)
    || snapshot.topics.some(topic => topic.incomplete)
    || snapshot.partialFailures.some(failure => ['groups', 'topics', 'committed', 'watermark'].includes(failure.scope))
  const incomplete = collectionError || snapshot.consumerGroups.some(row => row.incomplete || row.unknownPartitionCount > 0)
  return { known, incomplete, collectionError, available: !incomplete || snapshot.consumerGroups.some(row => row.partitions.some(partition => partition.lag !== null)) }
}

export function OverviewPage() {
  const { cluster, monitor, refreshSeconds = 5 } = useDashboard()
  const lag = useConsumerGroupLag()
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])
  const samples = useMemo(() => monitor.history.map(sample => {
    const value = totalLag(sample.snapshot)
    return { at: sample.at, lag: value.incomplete ? null : value.known }
  }), [monitor.history])
  const snapshot = monitor.snapshot
  const display = monitor.displaySnapshot ?? snapshot
  if (!cluster) return <section aria-label="概览内容"><h2>概览</h2><p>请选择集群。</p></section>
  if (!snapshot) return <section aria-label="概览内容"><h2>概览</h2>
    {monitor.status === 'connecting' || monitor.isFetching || lag.isPending
      ? <div role="status" aria-label="正在加载概览" className="overview-skeleton"><p>正在加载概览…</p><div /><div /><div /><div /></div>
      : <p>尚未采集概览数据。</p>}
  </section>
  const stale = monitor.status === 'error' || now - Date.parse(snapshot.collectedAt) > refreshSeconds * 2000
  const total = totalLag(snapshot)
  const abnormal = lag.rows.filter(row => row.state.status !== 'normal')
    .sort((a, b) => (b.displayRow ?? b.row).lag - (a.displayRow ?? a.row).lag || a.key.localeCompare(b.key))
  return <section aria-label="概览内容"><h2>概览</h2>
    {stale && <p role="status">数据已陈旧</p>}
    <div className="overview-metrics">
      <section aria-label="Topic 数"><h3>Topic 数</h3><p>{display!.topics.length.toLocaleString('en-US')}{snapshot.partialFailures.some(failure => failure.scope === 'topics') ? '（不完整，含陈旧展示值）' : ''}</p></section>
      <section aria-label="Consumer Group 数"><h3>Consumer Group 数</h3><p>{new Set(display!.consumerGroupIds).size.toLocaleString('en-US')}{snapshot.partialFailures.some(failure => failure.scope === 'groups') ? '（不完整，含陈旧展示值）' : ''}</p></section>
      <section aria-label="总 Lag"><h3>总 Lag</h3><p>{total.available ? total.known.toLocaleString('en-US') : '—'}{total.incomplete ? ' 已知 Lag（不完整，未知值未计入）' : ''}</p></section>
      <section aria-label="异常 Consumer Group 数"><h3>异常 Consumer Group 数</h3><p>{lag.ready ? new Set(abnormal.map(item => item.row.groupId)).size : '—'}{snapshot.partialFailures.some(failure => failure.scope === 'groups') ? '（不完整）' : ''}</p></section>
    </div>
    <LagLineChart samples={samples} stale={stale || total.collectionError} />
    <h3>异常 Consumer Groups</h3>
    {lag.isPending && <p role="status">正在加载 Lag 规则…</p>}
    <ApiErrorNotice error={lag.error} />
    {lag.ready && (abnormal.length ? <table aria-label="异常 Consumer Groups"><thead><tr>
      <th scope="col">Consumer Group / Topic</th><th scope="col">状态</th><th scope="col">Lag</th><th scope="col">数据状态</th>
    </tr></thead><tbody>{abnormal.map(({ row, displayRow = row, staleAt, key, health, state }) => <tr key={key}>
      <td><Link to={`/consumer-groups?${new URLSearchParams({ groupId: row.groupId, topic: row.topic })}`}>{row.groupId} / {row.topic}</Link></td>
      <td><StatusBadge status={state.status} /></td><td>{displayRow.lag.toLocaleString('en-US')}{row.unknownPartitionCount ? ` + ${row.unknownPartitionCount} 未知` : ''}{health.collectionError ? '（不完整）' : ''}</td>
      <td>{stale || health.collectionError ? <StaleNotice at={staleAt} /> : '已更新'}</td>
    </tr>)}</tbody></table> : <p>暂无异常 Consumer Groups。</p>)}
  </section>
}
