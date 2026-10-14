import { Fragment, useId, useState } from 'react'
import { useSearchParams } from 'react-router'
import type { ConsumerGroupTopicSnapshot } from '../../api/types'
import { MetricHelp } from '../../components/MetricHelp'
import { StatusBadge } from '../../components/StatusBadge'
import { ApiErrorNotice } from '../clusters/ClusterForm'
import { useDashboard } from '../dashboard/useDashboard'
import type { MonitorSession } from '../dashboard/useMonitorSession'
import { lagRowKey, statusLabels, type LagStatus } from './status'
import { useConsumerGroupLag, type ConsumerGroupLagRow } from './useConsumerGroupLag'

const format = (value: number | null) => value === null ? '—' : value.toLocaleString('en-US')

function PartitionTable({ row }: { row: ConsumerGroupTopicSnapshot }) {
  return <table aria-label={`${row.groupId} / ${row.topic} Partitions`}><thead><tr>
    <th scope="col">Partition</th>
    <th scope="col">High watermark <MetricHelp label={`${row.groupId} / ${row.topic} High watermark`}>下一条消息将写入的位置，不是最后一条消息的 Offset。采集失败时显示 —。</MetricHelp></th>
    <th scope="col">Committed Offset <MetricHelp label={`${row.groupId} / ${row.topic} Committed Offset`}>消费组提交的下一条待消费位置；未提交或采集失败时显示 —，不能当作 0。</MetricHelp></th>
    <th scope="col">Lag <MetricHelp label={`${row.groupId} / ${row.topic} Partition Lag`}>max(high - committedOffset, 0)。任一位置未知时显示 —，并不计入已知 Lag。</MetricHelp></th>
  </tr></thead><tbody>{row.partitions.map(partition => <tr key={partition.partition}>
    <td>{partition.partition}</td><td>{format(partition.high)}</td><td>{format(partition.committedOffset === null || partition.committedOffset < 0 ? null : partition.committedOffset)}</td><td>{format(partition.lag)}</td>
  </tr>)}</tbody></table>
}

function ConsumerGroupsTable({ clusterId, monitor, evaluatedRows }: { clusterId: string; monitor: MonitorSession; evaluatedRows: ConsumerGroupLagRow[] }) {
  const [params] = useSearchParams()
  const [search, setSearch] = useState('')
  const [filter, setFilter] = useState<LagStatus | 'all'>('all')
  const [expanded, setExpanded] = useState<Set<string>>(() => {
    const groupId = params.get('groupId'), topic = params.get('topic')
    return new Set(groupId && topic ? [lagRowKey(clusterId, groupId, topic)] : [])
  })
  const id = useId()
  const { snapshot, status } = monitor
  const rows = evaluatedRows.filter(({ row, state }) => (filter === 'all' || state.status === filter)
    && `${row.groupId}\n${row.topic}`.toLocaleLowerCase().includes(search.trim().toLocaleLowerCase()))
    .sort((a, b) => b.row.lag - a.row.lag || a.key.localeCompare(b.key))
  return <>
    <label>搜索 Consumer Group 或 Topic <input type="search" value={search} onChange={event => setSearch(event.target.value)} /></label>
    <label>状态筛选 <select value={filter} onChange={event => setFilter(event.target.value as LagStatus | 'all')}>
      <option value="all">全部状态</option>{Object.entries(statusLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}
    </select></label>
    {status !== 'connected' && <p role="status">当前未持续采样，暂停计时；显示最后一次快照，恢复后重新累计。</p>}
    {!!snapshot?.partialFailures.length && <p role="status">部分数据采集失败；仅展示已发现的 Consumer Group / Topic，未知值不代表 0。</p>}
    {!snapshot ? <p>尚未采集 Consumer Groups 数据。</p> : <>
      <table aria-label="Consumer Groups 汇总"><thead><tr>
        <th scope="col">Consumer Group</th><th scope="col">Topic</th>
        <th scope="col">状态 <MetricHelp label="状态">优先级：采集异常 &gt; 未提交 &gt; 高 Lag &gt; 积压 &gt; 正常。正常为 Lag ≤ 正常上限，积压为超过正常上限且低于高 Lag 起始值，高 Lag 为达到起始值。连续超限达到规则时长前仍为正常；回到正常区间立即重置。只累计页面可见且连续成功采样的时间，隐藏、刷新页面、切换集群或会话重建会重置。</MetricHelp></th>
        <th scope="col">有效分区 <MetricHelp label="有效分区">同时取得 High watermark 和有效提交位置、可计算 Lag 的分区数量；不是消费者成员数。</MetricHelp></th>
        <th scope="col">未提交分区 <MetricHelp label="未提交分区">没有有效提交位置的分区数量。采集失败也可能使提交位置未知，此时以采集异常状态为准，不能据此断定从未提交。</MetricHelp></th>
        <th scope="col">总 Lag <MetricHelp label="总 Lag">已知分区 max(high - committedOffset, 0) 的总和；未知分区以“已知 Lag + N 未知”显示，元数据缺失另标不完整。Lag 是 Offset 差值估算，未知不能当作 0。</MetricHelp></th>
        <th scope="col">趋势 <MetricHelp label="趋势">仅比较最近两个连续成功且数据完整的样本：↑ 上升、↓ 下降、→ 持平；样本不足或中断显示 —。趋势不参与状态判定。</MetricHelp></th>
      </tr></thead><tbody>{rows.map(({ row, key, health, resolved, state, trend }, index) => {
        const open = expanded.has(key)
        const detailsId = `${id}-partitions-${index}`
        return <Fragment key={key}><tr>
          <td><button type="button" aria-label={`${open ? '收起' : '展开'} ${row.groupId} / ${row.topic}`} aria-expanded={open} aria-controls={open ? detailsId : undefined}
            onClick={() => setExpanded(previous => { const next = new Set(previous); if (open) next.delete(key); else next.add(key); return next })}>{open ? '收起' : '展开'}</button> <span>{row.groupId}</span></td>
          <td>{row.topic}</td><td><StatusBadge status={state.status} />{resolved.custom && <span> <MetricHelp trigger="自定义规则" label={`自定义规则 ${row.groupId} / ${row.topic}`}>
            正常上限：{format(resolved.rule.normalMax)}；高 Lag 起始值：{format(resolved.rule.highMin)}；持续超限时间：{resolved.rule.overLimitSeconds} 秒。
          </MetricHelp></span>}</td>
          <td>{health.validCount}</td><td>{health.uncommittedCount}</td>
          <td>{format(row.lag)}{row.unknownPartitionCount > 0 ? ` + ${row.unknownPartitionCount} 未知` : ''}{health.collectionError ? '（不完整）' : ''}</td>
          <td>{trend ? <span aria-label={`Lag ${trend === 'up' ? '上升' : trend === 'down' ? '下降' : '持平'}`}>{trend === 'up' ? '↑' : trend === 'down' ? '↓' : '→'}</span> : '—'}</td>
        </tr>{open && <tr><td colSpan={7} id={detailsId}><PartitionTable row={row} /></td></tr>}</Fragment>
      })}</tbody></table>
      {!rows.length && <p>{snapshot.consumerGroups.length ? '没有匹配的 Consumer Group / Topic。' : '暂无 Consumer Group / Topic 数据。'}</p>}
    </>}
  </>
}

export function ConsumerGroupsPage() {
  const { cluster, monitor } = useDashboard()
  const lag = useConsumerGroupLag()
  return <section aria-label="Consumer Groups 内容"><h2>Consumer Groups</h2>
    {!cluster ? <p>请选择集群。</p> : <>
      {lag.isPending && <p role="status">正在加载 Lag 规则…</p>}
      <ApiErrorNotice error={lag.error} />
      {lag.ready && <ConsumerGroupsTable key={cluster.id} clusterId={cluster.id} monitor={monitor} evaluatedRows={lag.rows} />}
    </>}
  </section>
}
