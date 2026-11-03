import { statusLabels, type LagStatus } from '../features/consumer-groups/status'

// 消费组状态展示时统一渲染对应标签文字与状态色。
export function StatusBadge({ status }: { status: LagStatus }) {
  return <span className={`status-badge status-${status}`}>{statusLabels[status]}</span>
}
