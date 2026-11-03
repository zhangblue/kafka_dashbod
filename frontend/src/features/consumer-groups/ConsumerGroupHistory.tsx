import { useTranslation } from 'react-i18next'
import { useMemo, useState } from 'react'
import { useSearchParams } from 'react-router'
import type { ClusterSnapshot } from '../../api/types'
import type { SnapshotSample } from '../dashboard/history'
import { LagLineChart } from '../charts/LagLineChart'
import { hasCollectionFailure, rowHealth } from './status'

interface Selection { key: string; topic: string; partition?: number }

// 在选定消费组后生成 Topic 与分区曲线选项，保留局部采集失败的历史断点。
function GroupHistory({ groupId, snapshots, history, stale }: {
  groupId: string; snapshots: ClusterSnapshot[]; history: SnapshotSample[]; stale: boolean
}) {
  const { t } = useTranslation()
  const choices = useMemo(() => {
    const choices = new Map<string, Selection>()
    for (const snapshot of snapshots) for (const row of snapshot.consumerGroups) {
      if (row.groupId !== groupId) continue
      const key = JSON.stringify([row.topic])
      choices.set(key, { key, topic: row.topic })
      for (const partition of row.partitions) {
        const key = JSON.stringify([row.topic, partition.partition])
        choices.set(key, { key, topic: row.topic, partition: partition.partition })
      }
    }
    return [...choices.values()]
  }, [snapshots, groupId])
  // 选择及缓存仅保存 Topic 与 Partition 标识；名称在渲染时翻译，语言切换不重建选择或历史。
  const options = choices.map(option => ({ ...option, name: option.partition === undefined
    ? t('consumerGroups.topicSeries', { topic: option.topic })
    : t('consumerGroups.partitionSeries', { topic: option.topic, partition: option.partition }) }))
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
    <fieldset><legend>{t('consumerGroups.historySelection')}</legend>{options.map(option => <label key={option.key}>
      <input type="checkbox" checked={active.has(option.key)} onChange={/* 在趋势复选框变化时更新选中序列，保留其他 Topic 与分区选择。 */ () => setSelected(previous => {
        const next = new Set(previous ?? active)
        if (next.has(option.key)) next.delete(option.key); else next.add(option.key)
        return next
      })} />{option.name}
    </label>)}</fieldset>
    {!series.length ? <p>{t('consumerGroups.selectSeries')}</p>
      : <LagLineChart label={t('consumerGroups.historyLabel', { group: groupId })} series={series} stale={stale} />}
  </>
}

// 在消费组页面渲染时从当前及历史快照建立消费组选项，并隔离各组的趋势选择。
export function ConsumerGroupHistory({ snapshot, history, stale }: { snapshot: ClusterSnapshot | null; history: SnapshotSample[]; stale: boolean }) {
  const { t } = useTranslation()
  const [params] = useSearchParams()
  const [chosenGroup, setChosenGroup] = useState(params.get('groupId') ?? '')
  const snapshots = useMemo(() => [...history.map(sample => sample.snapshot), ...snapshot ? [snapshot] : []], [history, snapshot])
  const groups = [...new Set(snapshots.flatMap(snapshot => snapshot.consumerGroups.map(row => row.groupId)))]
  const groupId = groups.includes(chosenGroup) ? chosenGroup : groups[0]
  if (!groupId) return null
  return <section aria-label={t('consumerGroups.historyTitle')}><h3>{t('consumerGroups.historyTitle')}</h3>
    <p>{t('consumerGroups.historyDescription')}</p>
    <label>{t('consumerGroups.historyGroup')} <select value={groupId} onChange={/* 在趋势消费组选择变化时切换对应历史视图。 */ event => setChosenGroup(event.target.value)}>
      {groups.map(group => <option key={group}>{group}</option>)}
    </select></label>
    <GroupHistory key={groupId} groupId={groupId} snapshots={snapshots} history={history} stale={stale} />
  </section>
}
