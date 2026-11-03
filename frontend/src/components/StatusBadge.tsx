import { useTranslation } from 'react-i18next'
import { statusTranslationKeys, type LagStatus } from '../features/consumer-groups/status'

// 消费组状态展示时统一渲染对应标签文字与状态色。
export function StatusBadge({ status }: { status: LagStatus }) {
  const { t } = useTranslation()
  return <span className={`status-badge status-${status}`}>{t(statusTranslationKeys[status])}</span>
}
