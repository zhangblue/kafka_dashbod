import { useOutletContext } from 'react-router'
import type { ClusterDto } from '../../api/types'
import type { MonitorSession } from './useMonitorSession'

export interface DashboardContext { cluster: ClusterDto | null; monitor: MonitorSession }
export const useDashboard = () => useOutletContext<DashboardContext>()
