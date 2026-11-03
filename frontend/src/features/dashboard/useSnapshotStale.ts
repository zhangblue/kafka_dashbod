import { useEffect, useState } from 'react'
import type { MonitorSession, RefreshSeconds } from './useMonitorSession'

// 监控页面渲染时判断采集错误或超过两个刷新周期的快照，返回陈旧展示状态。
export function useSnapshotStale(monitor: Pick<MonitorSession, 'snapshot' | 'status'>, refreshSeconds: RefreshSeconds = 5) {
  const [now, setNow] = useState(() => Date.now())
  // 挂载期间每秒刷新陈旧判断时钟；卸载后清除计时器，避免继续触发页面更新。
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])
  return Boolean(monitor.snapshot && (monitor.status === 'error'
    || now - Date.parse(monitor.snapshot.collectedAt) > refreshSeconds * 2000))
}
