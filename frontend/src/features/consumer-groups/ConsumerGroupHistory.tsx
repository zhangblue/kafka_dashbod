import { useMemo, useState } from 'react'
import { useSearchParams } from 'react-router'
import type { ClusterSnapshot } from '../../api/types'
import type { SnapshotSample } from '../dashboard/history'
import { LagLineChart } from '../charts/LagLineChart'
import { hasCollectionFailure, rowHealth } from './status'

interface Selection { key: string; topic: string; partition?: number; name: string }

function GroupHistory({ groupId, snapshots, history, stale }: {
  groupId: string; snapshots: ClusterSnapshot[]; history: SnapshotSample[]; stale: boolean
}) {
  const options = useMemo(() => {
    const choices = new Map<string, Selection>()
    for (const snapshot of snapshots) for (const row of snapshot.consumerGroups) {
      if (row.groupId !== groupId) continue
      const key = JSON.stringify([row.topic])
      choices.set(key, { key, topic: row.topic, name: `${row.topic} / 总 Lag` })
      for (const partition of row.partitions) {
        const key = JSON.stringify([row.topic, partition.partition])
        choices.set(key, { key, topic: row.topic, partition: partition.partition, name: `${row.topic} / Partition ${partition.partition}` })
      }
    }
    return [...choices.values()]
  }, [snapshots, groupId])
  const [selected, setSelected] = useState<Set<string> | null>(null)
  const active = selected ?? new Set(options.slice(0, 1).map(option => option.key))
  const series = options.filter(option => active.has(option.key)).map(option => ({ name: option.name,
    samples: history.map(sample => {
      const row = sample.snapshot.consumerGroups.find(row => row.groupId === groupId && row.topic === option.topic)
      let lag: number | null = null
      if (row && option.partition === undefined) {
        const health = rowHealth(row, sample.snapshot)
        if (!health.collectionError && !health.uncommitted && !row.incomplete && row.unknownPartitionCount === 0) lag = row.lag
      } else if (row) {
        const partition = row.partitions.find(partition => partition.partition === option.partition)
        if (partition && partition.high !== null && partition.committedOffset !== null && partition.committedOffset >= 0
          && !hasCollectionFailure({ ...row, partitions: [partition] }, sample.snapshot)) lag = partition.lag
      }
      return { at: sample.at, lag }
    }),
  }))
  return <>
    <fieldset><legend>选择 Topic / Partition 趋势（可多选）</legend>{options.map(option => <label key={option.key}>
      <input type="checkbox" checked={active.has(option.key)} onChange={() => setSelected(previous => {
        const next = new Set(previous ?? active)
        if (next.has(option.key)) next.delete(option.key); else next.add(option.key)
        return next
      })} />{option.name}
    </label>)}</fieldset>
    {!series.length ? <p>请选择至少一个 Topic 或 Partition。</p>
      : <LagLineChart label={`${groupId} Lag 历史`} series={series} stale={stale} />}
  </>
}

export function ConsumerGroupHistory({ snapshot, history, stale }: { snapshot: ClusterSnapshot | null; history: SnapshotSample[]; stale: boolean }) {
  const [params] = useSearchParams()
  const [chosenGroup, setChosenGroup] = useState(params.get('groupId') ?? '')
  const snapshots = useMemo(() => [...history.map(sample => sample.snapshot), ...snapshot ? [snapshot] : []], [history, snapshot])
  const groups = [...new Set(snapshots.flatMap(snapshot => snapshot.consumerGroups.map(row => row.groupId)))]
  const groupId = groups.includes(chosenGroup) ? chosenGroup : groups[0]
  if (!groupId) return null
  return <section aria-label="消费组 Lag 历史"><h3>消费组 Lag 历史</h3>
    <p>历史仅保留当前页面最近一小时的原始采样；未提交、采集失败或未发现的序列显示断点。</p>
    <label>趋势 Consumer Group <select value={groupId} onChange={event => setChosenGroup(event.target.value)}>
      {groups.map(group => <option key={group}>{group}</option>)}
    </select></label>
    <GroupHistory key={groupId} groupId={groupId} snapshots={snapshots} history={history} stale={stale} />
  </section>
}
