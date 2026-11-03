import type { ClusterSnapshot } from '../../api/types'

export interface SnapshotSample { at: number; snapshot: ClusterSnapshot }

/** 每次接收原始快照时追加去重样本，以最新服务端采集时间裁剪一小时内存历史并排序。 */
export function appendSample<T extends { at: number }>(history: readonly T[], sample: T): T[] {
  if (!Number.isFinite(sample.at)) return [...history]
  // 乱序响应不能把窗口起点倒退，因此以已有和新增样本中的最新采集时间为准。
  const newest = Math.max(sample.at, ...history.map(item => item.at))
  return [...history.filter(item => item.at !== sample.at), sample]
    .filter(item => item.at >= newest - 3_600_000)
    .sort((a, b) => a.at - b.at)
}
