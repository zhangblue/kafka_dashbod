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

// 卸载、失败或重连时先撤销本地租约，再排队释放，避免后续调用复用正在关闭的会话。
function closeLease(runtime: { lease: Lease | undefined }) {
  const lease = runtime.lease
  runtime.lease = undefined
  if (lease) void sessionTransition(() => sessionsApi.close(lease.sessionId)).catch(() => {})
}

// 服务端采集仍在执行时等待退避间隔；取消信号同时终止等待并清理定时器与监听器。
function waitForCollection(milliseconds: number, signal: AbortSignal) {
  return new Promise<void>((resolve, reject) => {
    // 隐藏或卸载取消退避时清除定时器，再拒绝等待，防止取消后继续重试采集。
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

// 可见监控页面挂载时按需创建与轮询短租约，隔离集群状态并在切换、卸载时释放资源。
export function useMonitorSession(clusterId: string | null, refreshSeconds: RefreshSeconds = 5, enabled = true) {
  const client = useQueryClient()
  const [visible, setVisible] = useState(() => document.visibilityState === 'visible')
  const [pageActive, setPageActive] = useState(true)
  const [attempt, setAttempt] = useState(0)
  const [leaseClock, setLeaseClock] = useState(() => Date.now())
  const [leaseDeadline, setLeaseDeadline] = useState(0)
  const [continuity, setContinuity] = useState(0)
  const [failure, setFailure] = useState<{ runtime: object; error: unknown } | null>(null)
  const [data, setData] = useState<{ runtime: object; sessionId: string; snapshot: ClusterSnapshot; display: DisplaySnapshot; history: SnapshotSample[] } | null>(null)
  // 仅在集群或启用状态变化时更换资源归属，迟到响应按实例身份隔离，刷新间隔变化不清空历史。
  const runtime = useMemo(() => ({ id: ++nextRuntimeId, clusterId, enabled, active: false, pageHidden: false,
    foreground: document.visibilityState === 'visible', lease: undefined as Lease | undefined,
    creating: undefined as Promise<Lease> | undefined }), [clusterId, enabled])
  // 为当前实例和重连代次生成查询键，避免旧请求缓存或结果覆盖新会话。
  const queryKey = useMemo(() => ['monitor-snapshot', clusterId, runtime.id, attempt] as const, [clusterId, runtime, attempt])
  const error = failure?.runtime === runtime ? failure.error : null
  const running = Boolean(clusterId) && enabled && visible && pageActive && !error
  // 当前实例需要结束租约时统一撤销引用并排队关闭，供生命周期及错误恢复复用。
  const close = () => closeLease(runtime)

  useEffect(() => {
    // 此实例仅持有可变网络资源；挂载后激活，切换或卸载时先失活再释放，阻止迟到创建被接纳。
    // oxlint-disable-next-line react/immutability
    runtime.active = true
    return () => { runtime.active = false; closeLease(runtime) }
  }, [runtime])

  useEffect(() => {
    // 浏览器可见性变化时暂停或恢复采样；隐藏时取消 HTTP 并打断规则连续计时。
    const onVisibility = () => {
      const next = document.visibilityState === 'visible'
      setLeaseClock(Date.now())
      runtime.foreground = next && !runtime.pageHidden
      setVisible(next)
      if (!next) {
        setContinuity(value => value + 1)
        // 隐藏时取消当前代次的 HTTP 等待；服务端原生采集可能仍在运行，恢复时需处理忙碌响应。
        void client.cancelQueries({ queryKey })
      }
    }
    // 页面离开或进入缓存时停止采样并撤销租约，通过 keepalive 排队发送关闭请求。
    const onPageHide = () => {
      runtime.pageHidden = true
      runtime.foreground = false
      setPageActive(false)
      setContinuity(value => value + 1)
      void client.cancelQueries({ queryKey })
      const lease = runtime.lease
      runtime.lease = undefined
      // 离开页面仍按队列等待先前资源操作，以免后续页面恢复时提前创建新会话。
      if (lease) void sessionTransition(() => fetch(new URL(`/api/sessions/${encodeURIComponent(lease.sessionId)}`, window.location.origin),
        { method: 'DELETE', keepalive: true })).catch(() => {})
    }
    // 页面从缓存恢复时重新确认可见性和租约时钟，再允许查询恢复。
    const onPageShow = () => {
      runtime.pageHidden = false
      runtime.foreground = document.visibilityState === 'visible'
      setPageActive(true)
      setLeaseClock(Date.now())
    }
    document.addEventListener('visibilitychange', onVisibility)
    window.addEventListener('pagehide', onPageHide)
    window.addEventListener('pageshow', onPageShow)
    return () => {
      // 实例或查询代次变化时移除旧监听，防止同一浏览器事件驱动过期实例。
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
    // 轮询或手动刷新进入时串行连接与采集，只接纳仍活跃且可见实例的响应。
    queryFn: async ({ signal }) => {
      // 每个异步边界后检查实例、前台状态和请求取消状态，阻止过期响应继续更新页面。
      const usable = () => runtime.active && runtime.foreground && !signal.aborted
      // 异步流程失去可用性时生成统一取消错误，交给查询层结束本次采样。
      const aborted = () => new DOMException('Monitoring paused', 'AbortError')
      // 采集成功或忙碌响应到达时更新租约，以请求开始时间计算剩余可信窗口。
      const renew = (lease: Lease, requestedAt: number) => {
        // 服务端在采集前续租，响应延迟必须消耗租约窗口，不能从收到响应时重新计算完整租期。
        lease.expiresAt = requestedAt + lease.leaseSeconds * 1000
        setLeaseDeadline(lease.expiresAt)
        setLeaseClock(Date.now())
      }
      // 每次采集前复用有效租约；过期则释放重建，并等待同实例正在创建的会话。
      async function connect() {
        if (runtime.lease && runtime.lease.expiresAt <= Date.now()) close()
        if (!runtime.lease) {
          setContinuity(value => value + 1)
          // 创建经过页面级队列，等待旧关闭完成，且同实例并发入口共享一项创建任务。
          runtime.creating ??= sessionTransition(async () => {
            if (!runtime.active || !runtime.foreground) throw aborted()
            const session = await sessionsApi.create(runtime.clusterId!)
            const lease = { ...session, expiresAt: Date.now() + session.leaseSeconds * 1000 }
            if (!runtime.active || !runtime.foreground) {
              // 创建期间已卸载或隐藏时立即等待关闭新资源，避免把无人接管的会话遗留到后台。
              await sessionsApi.close(session.sessionId).catch(() => {})
              throw aborted()
            }
            runtime.lease = lease
            return lease
          }).finally(() => { runtime.creating = undefined })
          await runtime.creating
        }
        // 连接等待完成后再次检查，避免切换或隐藏期间完成的创建进入采集阶段。
        if (!usable()) throw aborted()
        return runtime.lease!
      }
      // 连接就绪后采集快照；服务端忙碌时在同一请求任务中有界退避，避免重叠采集。
      async function collect(lease: Lease): Promise<ClusterSnapshot> {
        let backoff = 500
        while (usable()) {
          const requestedAt = Date.now()
          try {
            const snapshot = await sessionsApi.snapshot(lease.sessionId, signal)
            if (usable()) renew(lease, requestedAt)
            return snapshot
          } catch (error) {
            if (!(error instanceof ApiError && error.status === 409 && error.code === 'SNAPSHOT_IN_PROGRESS') || !usable()) throw error
            // 服务端先续租再检查采集锁；取消 HTTP 不会取消原生采集，因此保留租约并可取消地退避重试。
            renew(lease, requestedAt)
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
          // 租约已失效时先排队释放，再允许一次重建与重新采集，避免无限重试。
          close()
          lease = await connect()
          snapshot = await collect(lease)
        }
        // 接纳快照前再检查取消状态，防止旧请求更新已切换或隐藏的页面。
        if (!usable()) throw aborted()
        // 全量采集失败仅记录稳定错误码，由公共展示层按当前语言翻译，不把用户文案存入会话状态。
        if (snapshotUnavailable(snapshot)) throw new ApiError(503, { code: 'KAFKA_UNAVAILABLE' })
        // 原始快照进入历史与判定，展示副本单独保留失败资源；仅复用同一实例的旧状态。
        setData(previous => ({ runtime, sessionId: lease.sessionId, snapshot,
          display: retainDisplaySnapshot(snapshot, previous?.runtime === runtime ? previous.display : undefined), history: appendSample(previous?.runtime === runtime ? previous.history : [],
          { at: Date.parse(snapshot.collectedAt), snapshot }) }))
        return snapshot
      } catch (error) {
        if (usable()) {
          // 有效采样失败后停止自动重试并重置连续计时，再关闭会话等待用户显式重连。
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
  useEffect(() => {
    const lease = runtime.lease
    if (!lease || data?.runtime !== runtime || data.sessionId !== lease.sessionId) return
    let timer: ReturnType<typeof setTimeout>
    // 已有成功快照的租约到期时刷新状态；若忙碌响应续租则按新截止时间重新设定计时器。
    const checkExpiry = () => {
      const remaining = lease.expiresAt - Date.now()
      // 忙碌快照可在不改变已接纳数据的情况下续租，因此每次回调都重新读取截止时间。
      if (remaining > 0) timer = setTimeout(checkExpiry, remaining)
      else setLeaseClock(Date.now())
    }
    // 先按当前截止时间安排检查；依赖变化或卸载时清除旧计时器，防止旧租约驱动状态。
    checkExpiry()
    return () => clearTimeout(timer)
  }, [runtime, data, leaseDeadline])
  const sessionReady = Boolean(runtime.lease && runtime.lease.expiresAt > leaseClock
    && data?.runtime === runtime && data.sessionId === runtime.lease.sessionId)
  const sessionId = running && runtime.active && runtime.foreground && sessionReady ? runtime.lease?.sessionId : undefined
  const status: MonitorStatus = !clusterId || !enabled ? 'idle' : error ? 'error'
    : !visible || !pageActive ? 'paused' : sessionReady ? 'connected' : 'connecting'
  return {
    // 汇总、历史和计时始终使用原始采样；暂停、重连或失败时可保留最后快照，展示保留值只供页面使用。
    snapshot, history, status, error, isFetching: query.isFetching,
    ...(sessionId ? { sessionId } : {}),
    ...({ displaySnapshot: data?.runtime === runtime ? data.display.snapshot : null,
      staleResources: data?.runtime === runtime ? data.display.staleResources : {} } as MonitorDisplay),
    continuityKey: `${clusterId ?? ''}:${continuity}`,
    // 用户手动刷新时仅在允许运行状态触发采样，复用在途查询避免取消后重叠采集。
    refresh: async () => { if (running) await query.refetch({ cancelRefetch: false }) },
    // 用户显式重连且页面可见时先关闭旧租约，再清除错误并递增请求代次。
    reconnect: () => { if (visible && enabled && pageActive) { close(); setFailure(null); setAttempt(value => value + 1) } },
  }
}

export type MonitorSession = ReturnType<typeof useMonitorSession>
