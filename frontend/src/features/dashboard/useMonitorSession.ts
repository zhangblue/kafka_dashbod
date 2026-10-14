import { useEffect, useMemo, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError, sessionsApi } from '../../api/client'
import type { ClusterSnapshot, CreatedSession } from '../../api/types'
import { appendSample, type SnapshotSample } from './history'
import { snapshotUnavailable } from '../lag-rules/snapshotAvailability'
import { retainDisplaySnapshot, type DisplaySnapshot, type MonitorDisplay } from './displaySnapshot'
import { sessionTransition } from './sessionTransitions'

export type RefreshSeconds = 3 | 5 | 10
type Lease = CreatedSession & { expiresAt: number }
type MonitorStatus = 'idle' | 'connecting' | 'connected' | 'paused' | 'error'
let nextRuntimeId = 0

function closeLease(runtime: { lease: Lease | undefined }) {
  const lease = runtime.lease
  runtime.lease = undefined
  if (lease) void sessionTransition(() => sessionsApi.close(lease.sessionId)).catch(() => {})
}

function waitForCollection(milliseconds: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    const cancelled = () => {
      clearTimeout(timer)
      signal.removeEventListener('abort', cancelled)
      reject(new DOMException('Monitoring paused', 'AbortError'))
    }
    const timer = setTimeout(() => {
      signal.removeEventListener('abort', cancelled)
      resolve()
    }, milliseconds)
    if (signal.aborted) cancelled()
    else signal.addEventListener('abort', cancelled, { once: true })
  })
}

export function useMonitorSession(clusterId: string | null, refreshSeconds: RefreshSeconds = 5, enabled = true) {
  const client = useQueryClient()
  const [visible, setVisible] = useState(() => document.visibilityState === 'visible')
  const [pageActive, setPageActive] = useState(true)
  const [attempt, setAttempt] = useState(0)
  const [continuity, setContinuity] = useState(0)
  const [failure, setFailure] = useState<{ runtime: object; error: unknown } | null>(null)
  const [data, setData] = useState<{ runtime: object; sessionId: string; snapshot: ClusterSnapshot; display: DisplaySnapshot; history: SnapshotSample[] } | null>(null)
  const runtime = useMemo(() => ({ id: ++nextRuntimeId, clusterId, enabled, active: false, pageHidden: false,
    foreground: document.visibilityState === 'visible', lease: undefined as Lease | undefined,
    creating: undefined as Promise<Lease> | undefined }), [clusterId, enabled])
  const queryKey = useMemo(() => ['monitor-snapshot', clusterId, runtime.id, attempt] as const, [clusterId, runtime, attempt])
  const error = failure?.runtime === runtime ? failure.error : null
  const running = Boolean(clusterId) && enabled && visible && pageActive && !error
  const close = () => closeLease(runtime)

  useEffect(() => {
    // This object owns mutable network resources, never rendered React state.
    // oxlint-disable-next-line react/immutability
    runtime.active = true
    return () => { runtime.active = false; closeLease(runtime) }
  }, [runtime])

  useEffect(() => {
    const onVisibility = () => {
      const next = document.visibilityState === 'visible'
      runtime.foreground = next && !runtime.pageHidden
      setVisible(next)
      if (!next) {
        setContinuity(value => value + 1)
        void client.cancelQueries({ queryKey })
      }
    }
    const onPageHide = () => {
      runtime.pageHidden = true
      runtime.foreground = false
      setPageActive(false)
      setContinuity(value => value + 1)
      void client.cancelQueries({ queryKey })
      const lease = runtime.lease
      runtime.lease = undefined
      if (lease) void sessionTransition(() => fetch(new URL(`/api/sessions/${encodeURIComponent(lease.sessionId)}`, window.location.origin),
        { method: 'DELETE', keepalive: true })).catch(() => {})
    }
    const onPageShow = () => {
      runtime.pageHidden = false
      runtime.foreground = document.visibilityState === 'visible'
      setPageActive(true)
    }
    document.addEventListener('visibilitychange', onVisibility)
    window.addEventListener('pagehide', onPageHide)
    window.addEventListener('pageshow', onPageShow)
    return () => {
      document.removeEventListener('visibilitychange', onVisibility)
      window.removeEventListener('pagehide', onPageHide)
      window.removeEventListener('pageshow', onPageShow)
    }
  }, [client, queryKey, runtime])

  const query = useQuery({
    queryKey,
    enabled: running,
    retry: false,
    gcTime: 0,
    refetchOnWindowFocus: false,
    refetchOnReconnect: false,
    refetchInterval: running ? refreshSeconds * 1000 : false,
    refetchIntervalInBackground: false,
    queryFn: async ({ signal }) => {
      const usable = () => runtime.active && runtime.foreground && !signal.aborted
      const aborted = () => new DOMException('Monitoring paused', 'AbortError')
      async function connect() {
        if (runtime.lease && runtime.lease.expiresAt <= Date.now()) close()
        if (!runtime.lease) {
          setContinuity(value => value + 1)
          runtime.creating ??= sessionTransition(async () => {
            if (!runtime.active || !runtime.foreground) throw aborted()
            const session = await sessionsApi.create(runtime.clusterId!)
            const lease = { ...session, expiresAt: Date.now() + session.leaseSeconds * 1000 }
            if (!runtime.active || !runtime.foreground) {
              await sessionsApi.close(session.sessionId).catch(() => {})
              throw aborted()
            }
            runtime.lease = lease
            return lease
          }).finally(() => { runtime.creating = undefined })
          await runtime.creating
        }
        if (!usable()) throw aborted()
        return runtime.lease!
      }
      async function collect(lease: Lease): Promise<ClusterSnapshot> {
        let backoff = 500
        while (usable()) {
          try {
            return await sessionsApi.snapshot(lease.sessionId, signal)
          } catch (error) {
            if (!(error instanceof ApiError && error.status === 409 && error.code === 'SNAPSHOT_IN_PROGRESS') || !usable()) throw error
            // The server renews a live lease before checking its collection lock.
            // Cancelling HTTP does not cancel that server-side collection.
            lease.expiresAt = Date.now() + lease.leaseSeconds * 1000
            await waitForCollection(backoff, signal)
            backoff = Math.min(backoff * 2, 2000)
          }
        }
        throw aborted()
      }
      try {
        let lease = await connect()
        let snapshot: ClusterSnapshot
        try {
          snapshot = await collect(lease)
        } catch (error) {
          if (!(error instanceof ApiError && error.code === 'SESSION_EXPIRED') || !usable()) throw error
          close()
          lease = await connect()
          snapshot = await collect(lease)
        }
        if (!usable()) throw aborted()
        if (snapshotUnavailable(snapshot)) throw new ApiError(503, { code: 'KAFKA_UNAVAILABLE', message: '集群数据采集失败，请重新连接。' })
        lease.expiresAt = Date.now() + lease.leaseSeconds * 1000
        setData(previous => ({ runtime, sessionId: lease.sessionId, snapshot,
          display: retainDisplaySnapshot(snapshot, previous?.runtime === runtime ? previous.display : undefined), history: appendSample(previous?.runtime === runtime ? previous.history : [],
          { at: Date.parse(snapshot.collectedAt), snapshot }) }))
        return snapshot
      } catch (error) {
        if (usable()) {
          setFailure({ runtime, error })
          setContinuity(value => value + 1)
          close()
        }
        throw error
      }
    },
  })
  const snapshot = data?.runtime === runtime ? data.snapshot : null
  const history = data?.runtime === runtime ? data.history : []
  const sessionReady = Boolean(runtime.lease && data?.runtime === runtime && data.sessionId === runtime.lease.sessionId)
  const status: MonitorStatus = !clusterId || !enabled ? 'idle' : error ? 'error'
    : !visible || !pageActive ? 'paused' : sessionReady ? 'connected' : 'connecting'
  return {
    // Raw sampling data owns totals, history and timing. It may be the last accepted
    // snapshot while paused/reconnecting/failed; displaySnapshot is presentation only.
    snapshot, history, status, error, isFetching: query.isFetching,
    ...({ displaySnapshot: data?.runtime === runtime ? data.display.snapshot : null,
      staleResources: data?.runtime === runtime ? data.display.staleResources : {} } as MonitorDisplay),
    continuityKey: `${clusterId ?? ''}:${continuity}`,
    refresh: async () => { if (running) await query.refetch({ cancelRefetch: false }) },
    reconnect: () => { if (visible && enabled && pageActive) { close(); setFailure(null); setAttempt(value => value + 1) } },
  }
}

export type MonitorSession = ReturnType<typeof useMonitorSession>
