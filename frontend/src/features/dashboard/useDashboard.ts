import { useOutletContext } from 'react-router'
import type { ClusterDto } from '../../api/types'
import type { MonitorSession, RefreshSeconds } from './useMonitorSession'

export interface DashboardContext { cluster: ClusterDto | null; monitor: MonitorSession; refreshSeconds?: RefreshSeconds }
export const useDashboard = () => useOutletContext<DashboardContext>()
