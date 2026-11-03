import { createContext, useContext } from 'react'
import type { ConsumerGroupTopicSnapshot } from '../../api/types'
import type { LagState, TrackedLag, resolveLagRule, rowHealth } from './status'

export interface ConsumerGroupLagRow {
  row: ConsumerGroupTopicSnapshot
  key: string
  health: ReturnType<typeof rowHealth>
  resolved: ReturnType<typeof resolveLagRule>
  state: LagState
  // 趋势保留 up/down/flat/null 原始值，由展示组件翻译，不把语言写入共享判定状态。
  trend: TrackedLag['trend'] | undefined
  displayRow?: ConsumerGroupTopicSnapshot
  staleAt?: string
}

export const ConsumerGroupLagContext = createContext<{
  rows: ConsumerGroupLagRow[]; isPending: boolean; error: unknown; ready: boolean
} | null>(null)

// 在概览和消费组组件渲染时读取共享 Lag 判定，缺少提供者时立即暴露装配错误。
export function useConsumerGroupLag() {
  const value = useContext(ConsumerGroupLagContext)
  if (!value) throw new Error('Consumer Groups requires the dashboard lag provider')
  return value
}
