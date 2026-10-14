import { useEffect, useState } from 'react'
import type { MonitorSession, RefreshSeconds } from './useMonitorSession'

export function useSnapshotStale(monitor: Pick<MonitorSession, 'snapshot' | 'status'>, refreshSeconds: RefreshSeconds = 5) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(timer)
  }, [])
  return Boolean(monitor.snapshot && (monitor.status === 'error'
    || now - Date.parse(monitor.snapshot.collectedAt) > refreshSeconds * 2000))
}
