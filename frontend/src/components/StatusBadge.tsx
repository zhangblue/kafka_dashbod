import { statusLabels, type LagStatus } from '../features/consumer-groups/status'
const colors: Record<LagStatus, string> = { normal: '#17633a', backlog: '#805500', high: '#a21c24', uncommitted: '#62449b', error: '#8d2525' }

export function StatusBadge({ status }: { status: LagStatus }) {
  return <span style={{ color: colors[status], border: '1px solid currentColor', borderRadius: '0.25rem', padding: '0.1rem 0.35rem' }}>{statusLabels[status]}</span>
}
