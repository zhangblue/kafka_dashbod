import { Fragment, useId, useState } from 'react'
import type { PartitionWatermark, TopicSnapshot } from '../../api/types'
import { MetricHelp } from '../../components/MetricHelp'
import { useDashboard } from '../dashboard/useDashboard'
import { StaleNotice } from '../../components/StaleNotice'
import { topicDisplayKey, topicPartitionDisplayKey } from '../dashboard/displaySnapshot'
import { useSnapshotStale } from '../dashboard/useSnapshotStale'

const format = (value: number | null) => value === null ? '—' : value.toLocaleString('en-US')
const latest = (partition: PartitionWatermark) => partition.low === null || partition.high === null
  || partition.high <= partition.low ? null : partition.high - 1

function summary(topic: TopicSnapshot) {
  const incomplete = topic.incomplete || topic.partitions.some(partition => partition.low === null || partition.high === null)
  const lows = topic.partitions.flatMap(partition => partition.low === null ? [] : [partition.low])
  const maxima = topic.partitions.flatMap(partition => { const value = latest(partition); return value === null ? [] : [value] })
  return { incomplete,
    low: incomplete || !lows.length ? null : Math.min(...lows),
    max: incomplete || !maxima.length ? null : Math.max(...maxima),
    span: incomplete ? null : topic.partitions.reduce((sum, partition) => sum + partition.high! - partition.low!, 0),
  }
}

function PartitionTable({ topic, staleResources }: { topic: TopicSnapshot; staleResources: Record<string, string> | undefined }) {
  return <table aria-label={`${topic.topic} Partitions`}>
    <thead><tr>
      <th scope="col">Partition</th>
      <th scope="col">Low watermark <MetricHelp label="Low watermark">当前分区最早保留的 Offset（low），消息保留或删除后可能增长；未知值显示 —。</MetricHelp></th>
      <th scope="col">High watermark <MetricHelp label="High watermark">下一条消息将写入的位置（high），不是最后一条消息的 Offset；空分区 high = low，未知值显示 —。</MetricHelp></th>
      <th scope="col">最大消息 Offset <MetricHelp label={`${topic.topic} Partition 最大消息 Offset`}>非空分区为 high - 1；空分区（high = low）或水位未知时显示 —。Offset 不保证连续存在。</MetricHelp></th>
      <th scope="col">保留消息估算 <MetricHelp label={`${topic.topic} Partition 保留消息估算`}>High − Low 的 Offset 范围估算，不保证等于实际消息条数。</MetricHelp></th>
    </tr></thead>
    <tbody>{topic.partitions.map(partition => <tr key={partition.partition}>
      <td>{partition.partition}</td><td>{format(partition.low)}</td><td>{format(partition.high)}</td><td>{format(latest(partition))}{staleResources?.[topicPartitionDisplayKey(topic.topic, partition.partition)] && <StaleNotice at={staleResources[topicPartitionDisplayKey(topic.topic, partition.partition)]} />}</td><td>{format(partition.high === null || partition.low === null ? null : partition.high - partition.low)}</td>
    </tr>)}</tbody>
  </table>
}

export function TopicsPage() {
  const { monitor, refreshSeconds } = useDashboard()
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const [search, setSearch] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const id = useId()
  const snapshot = monitor.displaySnapshot ?? monitor.snapshot
  const topics = snapshot?.topics.filter(topic => topic.topic.toLocaleLowerCase().includes(search.trim().toLocaleLowerCase())) ?? []
  return <section className="page-panel" aria-label="Topics 内容">
    <div className="page-head"><div><h2>Topics</h2><p>查看 Topic 汇总与逐 Partition Offset</p></div>
    <div className="toolbar"><label className="search-label"><span className="sr-only">搜索 Topic</span><input type="search" placeholder="搜索 Topic…" value={search} onChange={event => setSearch(event.target.value)} /></label><span className="status-badge status-normal">{topics.length} Topics</span></div></div>
    {stale && <p role="status" aria-label="Topics 数据已陈旧">数据已陈旧</p>}
    <p className="offset-note"><b>Offset 按 Partition 独立计算。</b> Topic 汇总仅供概览，不同 Partition 的 Offset 不能视为统一序列；以展开后的 Partition 水位为准。</p>
    {!!snapshot?.partialFailures.length && <p role="status">部分数据采集失败；保留的上次采集值标为陈旧，其余未知值显示 —。</p>}
    {!snapshot ? <p>尚未采集 Topic 数据。</p> : <>
      <div className="panel table-scroll"><table aria-label="Topics 汇总"><thead><tr>
        <th scope="col">Topic</th>
        <th scope="col">Partitions <MetricHelp label="Partitions">当前快照中发现的分区数量；采集失败时可能不完整，不代表集群全部分区。</MetricHelp></th>
        <th scope="col">最小 Offset <MetricHelp label="最小 Offset">所有分区 low 的最小值，仅用于概览；各分区 Offset 独立，数据不完整时显示 —。</MetricHelp></th>
        <th scope="col">最大消息 Offset <MetricHelp label="最大消息 Offset">非空分区 high - 1 的最大值。空分区（high = low）不参与；全部为空或数据不完整时显示 —。不能跨 Partition 比较消息先后，Offset 不保证连续存在。</MetricHelp></th>
        <th scope="col">保留消息估算 <MetricHelp label="保留消息估算">各分区 high - low 的总和，仅为 Offset 范围估算；压缩、删除和事务记录会造成与实际可读消息数的差异。空分区贡献 0，数据不完整时显示 —。</MetricHelp></th>
        <th scope="col">数据状态</th>
      </tr></thead><tbody>{topics.map((topic, index) => {
        const staleAt = monitor.staleResources?.[topicDisplayKey(topic.topic)]
        const values = summary(staleAt ? { ...topic, incomplete: false } : topic)
        const open = expanded.has(topic.topic)
        const detailsId = `${id}-partitions-${index}`
        return <Fragment key={topic.topic}>
          <tr><td><button className="expand-button" type="button" aria-label={`${open ? '收起' : '展开'} ${topic.topic}`} aria-expanded={open} aria-controls={open ? detailsId : undefined}
            onClick={() => setExpanded(previous => { const next = new Set(previous); if (open) next.delete(topic.topic); else next.add(topic.topic); return next })}>
            {open ? '⌄' : '›'}</button> <span className="name">{topic.topic}</span></td>
          <td>{topic.partitions.length}</td><td>{format(values.low)}</td><td>{format(values.max)}</td><td>{format(values.span)}</td>
          <td>{staleAt ? <StaleNotice at={staleAt} /> : values.incomplete ? '不完整：部分分区采集失败，展开查看已知值' : '完整'}</td></tr>
          {open && <tr className="detail-row"><td colSpan={6} id={detailsId}><PartitionTable topic={topic} staleResources={monitor.staleResources} /></td></tr>}
        </Fragment>
      })}</tbody></table></div>
      {!topics.length && <p>{snapshot.topics.length ? '没有匹配的 Topic。' : '暂无 Topic。'}</p>}
    </>}
  </section>
}
