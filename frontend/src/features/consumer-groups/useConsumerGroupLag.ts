import { createContext, useContext } from 'react'
import type { ConsumerGroupTopicSnapshot } from '../../api/types'
import type { LagState, TrackedLag, resolveLagRule, rowHealth } from './status'

export interface ConsumerGroupLagRow {
  row: ConsumerGroupTopicSnapshot
  key: string
  health: ReturnType<typeof rowHealth>
  resolved: ReturnType<typeof resolveLagRule>
  state: LagState
  trend: TrackedLag['trend'] | undefined
  displayRow?: ConsumerGroupTopicSnapshot
  staleAt?: string
}

export const ConsumerGroupLagContext = createContext<{
  rows: ConsumerGroupLagRow[]; isPending: boolean; error: unknown; ready: boolean
} | null>(null)

export function useConsumerGroupLag() {
  const value = useContext(ConsumerGroupLagContext)
  if (!value) throw new Error('Consumer Groups requires the dashboard lag provider')
  return value
}
