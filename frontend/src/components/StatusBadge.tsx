import { statusLabels, type LagStatus } from '../features/consumer-groups/status'

export function StatusBadge({ status }: { status: LagStatus }) {
  return <span className={`status-badge status-${status}`}>{statusLabels[status]}</span>
}
