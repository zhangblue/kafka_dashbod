import type { ClusterSnapshot } from '../../api/types'

export interface SnapshotSample { at: number; snapshot: ClusterSnapshot }

/** The server's newest collection time anchors the one-hour browser-memory window. */
export function appendSample<T extends { at: number }>(history: readonly T[], sample: T): T[] {
  if (!Number.isFinite(sample.at)) return [...history]
  const newest = Math.max(sample.at, ...history.map(item => item.at))
  return [...history.filter(item => item.at !== sample.at), sample]
    .filter(item => item.at >= newest - 3_600_000)
    .sort((a, b) => a.at - b.at)
}
