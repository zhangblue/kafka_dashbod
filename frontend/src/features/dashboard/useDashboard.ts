import { useOutletContext } from 'react-router'
import type { ClusterDto } from '../../api/types'
import type { MonitorSession, RefreshSeconds } from './useMonitorSession'

export interface DashboardContext { cluster: ClusterDto | null; monitor: MonitorSession; refreshSeconds?: RefreshSeconds }
// 仪表盘子路由渲染时读取父布局提供的集群、共享会话与刷新频率。
export const useDashboard = () => useOutletContext<DashboardContext>()
