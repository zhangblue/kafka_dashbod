import i18n from '../../i18n'
import { useTranslation } from 'react-i18next'
import { localizeApiError } from '../../api/errorMessages'
import { act, cleanup, renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { StrictMode, useState, type ReactNode } from 'react'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { ApiError, sessionsApi } from '../../api/client'
import { snapshotFixture } from '../../test/handlers'
import { useMonitorSession } from './useMonitorSession'
import { sessionTransition } from './sessionTransitions'

let sequence: number
let created: string[]
let closed: string[]
let collected: string[]
let clock: number
// 每次 Hook 测试挂载时建立稳定且独立的查询客户端，隔离缓存与重试状态。
function Wrapper({ children }: { children: ReactNode }) {
  const [client] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: false } } }))
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}
// 异步状态断言前推进最小计时步长，等待查询通知与 React 更新完成。
async function flush() { await act(async () => { await vi.advanceTimersByTimeAsync(1) }) }
// 隐藏或恢复场景中派发真实可见性事件，再等待 Hook 处理查询取消或恢复。
async function visibility(value: DocumentVisibilityState) {
  await act(async () => {
    Object.defineProperty(document, 'visibilityState', { configurable: true, value })
    document.dispatchEvent(new Event('visibilitychange'))
  })
  await flush()
}
// 各用例前重置可见性、时钟和会话替身，确保生命周期场景相互隔离。
beforeEach(() => {
  vi.useFakeTimers()
  sequence = 0; clock = 0; created = []; closed = []; collected = []
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
  vi.spyOn(sessionsApi, 'create').mockImplementation(async cluster => {
    const sessionId = `${cluster}-${++sequence}`; created.push(sessionId)
    return { sessionId, leaseSeconds: 30 }
  })
  vi.spyOn(sessionsApi, 'close').mockImplementation(async id => { closed.push(id) })
  vi.spyOn(sessionsApi, 'snapshot').mockImplementation(async id => {
    collected.push(id)
    return { ...snapshotFixture, collectedAt: new Date(clock++ * 1000).toISOString() }
  })
})
// 每个会话用例结束时先卸载 Hook 并等待关闭队列，再恢复替身和真实计时器。
afterEach(async () => {
  // 本文件清理先于自动卸载执行，须在 API 替身仍有效时卸载 Hook，再等待页面级关闭队列后恢复客户端。
  cleanup()
  await sessionTransition(async () => {})
  vi.restoreAllMocks()
  vi.useRealTimers()
})

// 验证会话标识必须等自身首个快照成功后公开，切换或停用立即撤回标识。
it('exposes only the current lease after its first accepted snapshot and clears it on switching or disabling', async () => {
  let finish!: (value: typeof snapshotFixture) => void
  vi.mocked(sessionsApi.snapshot).mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
  const { result, rerender } = renderHook(({ cluster, active }) => useMonitorSession(cluster, 3, active), {
    wrapper: Wrapper, initialProps: { cluster: 'a', active: true },
  })
  await flush()
  expect(result.current.sessionId).toBeUndefined()
  await act(async () => finish(snapshotFixture))
  await flush()
  expect(result.current.sessionId).toBe('a-1')
  rerender({ cluster: 'b', active: true })
  expect(result.current.sessionId).toBeUndefined()
  await flush()
  expect(result.current.sessionId).toBe('b-2')
  rerender({ cluster: 'b', active: false })
  expect(result.current.sessionId).toBeUndefined()
})

// 验证隐藏、页面离开及采样失败期间不向管理操作暴露会话标识。
it('withholds session IDs while hidden, after pagehide, and after a sampling failure', async () => {
  vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(null, { status: 204 }))
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.sessionId).toBe('a-1')
  await visibility('hidden')
  expect(result.current.sessionId).toBeUndefined()
  await visibility('visible')
  expect(result.current.sessionId).toBe('a-1')
  await act(async () => window.dispatchEvent(new Event('pagehide')))
  expect(result.current.sessionId).toBeUndefined()
  await act(async () => window.dispatchEvent(new Event('pageshow')))
  await flush()
  expect(result.current.sessionId).toBe('a-2')
  vi.mocked(sessionsApi.snapshot).mockRejectedValueOnce(new ApiError(503, { code: 'KAFKA_UNAVAILABLE' }))
  await act(async () => { await result.current.refresh() })
  await flush()
  expect(result.current.status).toBe('error')
  expect(result.current.sessionId).toBeUndefined()
})

// 验证失效后替换的租约必须完成自己的快照后才能用于管理。
it('withholds a replacement lease until its own snapshot succeeds', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.sessionId).toBe('a-1')
  let finish!: (value: typeof snapshotFixture) => void
  vi.mocked(sessionsApi.snapshot).mockRejectedValueOnce(new ApiError(410, { code: 'SESSION_EXPIRED' }))
    .mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
  await act(async () => { void result.current.refresh() })
  await flush()
  expect(result.current.sessionId).toBeUndefined()
  await act(async () => finish(snapshotFixture))
  await flush()
  expect(result.current.sessionId).toBe('a-2')
})

// 验证下次快照一直等待时，到期会话标识仍会按时撤回。
it('withdraws an expired ID even when the next snapshot remains pending', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.sessionId).toBe('a-1')
  vi.mocked(sessionsApi.snapshot).mockImplementationOnce(() => new Promise(() => {}))
  await act(async () => { await vi.advanceTimersByTimeAsync(30_001) })
  expect(result.current.sessionId).toBeUndefined()
  expect(result.current.status).toBe('connecting')
})

// 验证初次或刷新请求迟到时不能延长已失效租约。
it.each(['initial', 'refresh'] as const)('does not reactivate a lease from a late %s snapshot response', async phase => {
  vi.setSystemTime(0)
  let finish!: (value: typeof snapshotFixture) => void
  // 迟到响应场景先挂起快照并暴露完成入口，由测试选择返回时机。
  const pending = () => new Promise<typeof snapshotFixture>(resolve => { finish = resolve })
  if (phase === 'initial') vi.mocked(sessionsApi.snapshot).mockImplementationOnce(pending)
  const { result, rerender } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  if (phase === 'refresh') {
    expect(result.current.sessionId).toBe('a-1')
    vi.mocked(sessionsApi.snapshot).mockImplementationOnce(pending)
    await act(async () => { void result.current.refresh() })
  }
  // The request renews at 0 or 1 ms, so any reply after 31 s cannot extend it.
  await act(async () => { await vi.advanceTimersByTimeAsync(31_000) })
  expect(result.current.sessionId).toBeUndefined()
  await act(async () => finish(snapshotFixture))
  await flush()
  rerender()
  expect(result.current.sessionId).toBeUndefined()
  expect(result.current.status).toBe('connecting')
})

// 验证忙碌响应晚于旧到期检查时仍会重新安排续租后的到期检查。
it('rearms expiry after a busy reply arrives after the previous expiry timer has stopped', async () => {
  vi.setSystemTime(0)
  const { result, rerender } = renderHook(({ interval }) => useMonitorSession('a', interval), {
    wrapper: Wrapper, initialProps: { interval: 10 as 3 | 5 | 10 },
  })
  await flush()
  expect(result.current.sessionId).toBe('a-1')
  let busyReply!: (reason: ApiError) => void
  vi.mocked(sessionsApi.snapshot)
    .mockImplementationOnce(() => new Promise((_, reject) => { busyReply = reject }))
    .mockImplementationOnce(() => new Promise(() => {}))
  // At 10 s a request starts and renews the server lease through 40 s.
  await act(async () => { await vi.advanceTimersByTimeAsync(9_999) })
  // Its 409 reply arrives only after the original 30 s timer has fired.
  await act(async () => { await vi.advanceTimersByTimeAsync(20_001) })
  expect(result.current.sessionId).toBeUndefined()
  await act(async () => busyReply(new ApiError(409, { code: 'SNAPSHOT_IN_PROGRESS' })))
  rerender({ interval: 5 })
  expect(result.current.sessionId).toBe('a-1')
  // The retry stays pending. The renewed 40 s deadline must still withdraw ID.
  await act(async () => { await vi.advanceTimersByTimeAsync(10_000) })
  expect(result.current.sessionId).toBeUndefined()
  expect(result.current.status).toBe('connecting')
})

// 验证三种刷新频率均按周期采集，变更频率保留历史且卸载会关闭会话。
it.each([3, 5, 10] as const)('collects at %ss without clearing history when cadence changes', async seconds => {
  const { result, rerender, unmount } = renderHook(({ interval }) => useMonitorSession('a', interval), { wrapper: Wrapper, initialProps: { interval: seconds } })
  await flush()
  expect(result.current.status).toBe('connected')
  expect(result.current.history).toHaveLength(1)
  await act(async () => { await vi.advanceTimersByTimeAsync(seconds * 1000) })
  await flush()
  expect(result.current.history).toHaveLength(2)
  rerender({ interval: seconds === 3 ? 5 : 3 })
  expect(result.current.history).toHaveLength(2)
  unmount()
  expect(closed).toEqual(['a-1'])
})

// 验证隐藏时暂停采集并重置连续性，可见后立即恢复有效会话。
it('pauses hidden pages, immediately resumes a valid session, and resets continuity', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  const continuity = result.current.continuityKey
  await visibility('hidden')
  await act(async () => { await vi.advanceTimersByTimeAsync(10_000) })
  expect(collected).toEqual(['a-1'])
  expect(result.current.status).toBe('paused')
  expect(result.current.continuityKey).not.toBe(continuity)
  await visibility('visible')
  expect(collected).toEqual(['a-1', 'a-1'])
  expect(created).toEqual(['a-1'])
  expect(result.current.history).toHaveLength(2)
})

// 验证初始隐藏或停用时不会创建会话，恢复可见仍尊重停用状态。
it('never creates a session when mounted hidden or disabled', async () => {
  await visibility('hidden')
  const { rerender } = renderHook(({ active }) => useMonitorSession('a', 3, active), { wrapper: Wrapper, initialProps: { active: true } })
  await flush()
  expect(created).toEqual([])
  rerender({ active: false })
  await visibility('visible')
  expect(created).toEqual([])
})

// 验证隐藏超出租期后重建连接，并以服务端采集时间淘汰旧历史。
it('rebuilds an expired lease after hiding and retains server-time history', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  await visibility('hidden')
  await act(async () => { await vi.advanceTimersByTimeAsync(31_000) })
  clock = 3602
  await visibility('visible')
  expect(created).toEqual(['a-1', 'a-2'])
  expect(closed).toContain('a-1')
  expect(result.current.history.map(item => item.at)).toEqual([3_602_000])
})

// 验证会话失效只自动重建一次，再失败后必须显式重连。
it('rebuilds once on SESSION_EXPIRED then stops errors until explicit reconnect', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  vi.mocked(sessionsApi.snapshot).mockRejectedValue(new ApiError(410, { code: 'SESSION_EXPIRED' }))
  await act(async () => { await result.current.refresh() })
  await flush()
  expect(created).toEqual(['a-1', 'a-2'])
  expect(result.current.status).toBe('error')
  await visibility('hidden'); await visibility('visible')
  await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })
  expect(created).toHaveLength(2)
  vi.mocked(sessionsApi.snapshot).mockResolvedValue({ ...snapshotFixture, collectedAt: new Date(1000).toISOString() })
  await act(async () => { result.current.reconnect() })
  await flush()
  expect(result.current.status).toBe('connected')
  expect(result.current.history).toHaveLength(2)
})

// 验证切换集群立即隔离快照与历史，并在切换和卸载时释放各自会话。
it('releases old sessions and clears current cluster state when switching', async () => {
  const { result, rerender, unmount } = renderHook(({ cluster }) => useMonitorSession(cluster, 3), { wrapper: Wrapper, initialProps: { cluster: 'a' } })
  await flush()
  rerender({ cluster: 'b' })
  expect(result.current.snapshot).toBeNull()
  expect(result.current.history).toEqual([])
  await flush()
  expect(closed).toContain('a-1')
  expect(created).toEqual(['a-1', 'b-2'])
  unmount()
  expect(closed).toContain('b-2')
})

// 验证跨组件挂载创建新会话前必须等旧 DELETE 完成，避免原生资源重叠。
it('waits for the previous mount DELETE to finish before creating another cluster session', async () => {
  let finishClose!: () => void
  vi.mocked(sessionsApi.close).mockImplementation(async id => {
    closed.push(id)
    await new Promise<void>(resolve => { finishClose = resolve })
  })
  const first = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  first.unmount()
  const second = renderHook(() => useMonitorSession('b', 3), { wrapper: Wrapper })
  try {
    await flush()
    expect(closed).toEqual(['a-1'])
    expect(created).toEqual(['a-1'])
  } finally {
    vi.mocked(sessionsApi.close).mockImplementation(async id => { closed.push(id) })
    await act(async () => finishClose())
  }
  await flush()
  expect(created).toEqual(['a-1', 'b-2'])
  expect(second.result.current.status).toBe('connected')
  second.unmount()
})

// 验证跨挂载的废弃创建及其关闭全部结束后，才创建新集群会话。
it('waits for an abandoned in-flight creation and its DELETE across mounts', async () => {
  let finishCreate!: (value: { sessionId: string; leaseSeconds: number }) => void
  let finishClose!: () => void
  vi.mocked(sessionsApi.create).mockImplementationOnce(() => new Promise(resolve => { finishCreate = resolve }))
  vi.mocked(sessionsApi.close).mockImplementationOnce(async id => {
    closed.push(id)
    await new Promise<void>(resolve => { finishClose = resolve })
  })
  const first = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush(); first.unmount()
  const second = renderHook(() => useMonitorSession('b', 3), { wrapper: Wrapper })
  await flush()
  try { expect(created).toEqual([]) } finally {
    await act(async () => finishCreate({ sessionId: 'abandoned', leaseSeconds: 30 }))
    await flush()
    await act(async () => finishClose())
  }
  await flush()
  expect(created).toEqual(['b-1'])
  expect(closed).toContain('abandoned')
  second.unmount()
})

// 验证慢采集与轮询、连续手动刷新不产生重叠请求。
it('does not overlap slow snapshots with polling or manual refresh', async () => {
  let finish!: (value: typeof snapshotFixture) => void
  vi.mocked(sessionsApi.snapshot).mockImplementation(id => {
    collected.push(id)
    return new Promise(resolve => { finish = resolve })
  })
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  await act(async () => { void result.current.refresh(); void result.current.refresh(); await vi.advanceTimersByTimeAsync(10_000) })
  expect(collected).toEqual(['a-1'])
  await act(async () => { finish(snapshotFixture) })
  await flush()
  expect(result.current.history).toHaveLength(1)
})

// 验证卸载后才完成创建的会话会立即释放，且不会采样。
it('releases a session whose creation finishes after unmount', async () => {
  let finish!: (value: { sessionId: string; leaseSeconds: number }) => void
  vi.mocked(sessionsApi.create).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  const { unmount } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush(); unmount()
  await act(async () => { finish({ sessionId: 'late', leaseSeconds: 30 }) })
  expect(closed).toContain('late')
  expect(collected).toEqual([])
})

// 验证 pagehide 通过 keepalive 关闭会话，离开后持续停止采样。
it('releases its session with keepalive on pagehide and stays stopped', async () => {
  const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(null, { status: 204 }))
  renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  await act(async () => { window.dispatchEvent(new Event('pagehide')) })
  expect(fetch).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ method: 'DELETE', keepalive: true }))
  await act(async () => { await vi.advanceTimersByTimeAsync(10_000) })
  expect(collected).toEqual(['a-1'])
})

// 验证 pagehide 后才创建完成的会话不会被接纳或采样。
it('does not retain or collect a session created after pagehide', async () => {
  let finish!: (value: { sessionId: string; leaseSeconds: number }) => void
  vi.mocked(sessionsApi.create).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  await act(async () => { window.dispatchEvent(new Event('pagehide')) })
  await act(async () => { finish({ sessionId: 'late-pagehide', leaseSeconds: 30 }) })
  expect(closed).toContain('late-pagehide')
  expect(collected).toEqual([])
})

// 验证历史保留原始未知分区值与部分失败，避免转为伪造零值。
it('keeps unknown partition values and partial failures unchanged in history', async () => {
  const partial = { ...snapshotFixture, partialFailures: [{ scope: 'watermark', resource: 'topic:0', code: 'TIMEOUT', message: 'timeout' }] }
  vi.mocked(sessionsApi.snapshot).mockResolvedValue(partial)
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.snapshot).toEqual(partial)
  expect(result.current.history[0].snapshot.consumerGroups[1].partitions[0].lag).toBeNull()
  expect(result.current.history[0].snapshot.partialFailures).toEqual(partial.partialFailures)
})

// 验证 HTTP 200 全量失败会断连，但不覆盖上次成功快照和历史。
it('treats a collector HTTP 200 full-failure response as disconnected without replacing successful data or history', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  const successful = result.current.snapshot
  vi.mocked(sessionsApi.snapshot).mockResolvedValue({ clusterId: 'cluster-a', collectedAt: new Date(1000).toISOString(),
    topics: [], consumerGroupIds: [], consumerGroups: [], partialFailures: [
      { scope: 'topics', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' },
      { scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' },
    ] })
  await act(async () => { await result.current.refresh() }); await flush()
  expect(result.current.status).toBe('error')
  expect(result.current.snapshot).toEqual(successful)
  expect(result.current.history).toHaveLength(1)
  expect(closed).toContain('a-1')
  vi.mocked(sessionsApi.snapshot).mockResolvedValue({ ...snapshotFixture, collectedAt: new Date(2000).toISOString() })
  await act(async () => { result.current.reconnect() }); await flush()
  expect(result.current.status).toBe('connected')
  expect(result.current.history.map(sample => sample.at)).toEqual([0, 2000])
})

// 验证普通错误停止自动尝试，隐藏时屏蔽手动操作，显式重连才恢复。
it('stops non-expiry errors, blocks hidden manual refresh, and reconnects explicitly', async () => {
  vi.mocked(sessionsApi.snapshot).mockRejectedValueOnce(new ApiError(503, { code: 'KAFKA_ERROR', message: 'unavailable' }))
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.status).toBe('error')
  expect(closed).toEqual(['a-1'])
  await act(async () => { await vi.advanceTimersByTimeAsync(20_000) })
  expect(created).toEqual(['a-1'])
  await visibility('hidden')
  await act(async () => { await result.current.refresh(); result.current.reconnect() })
  expect(created).toEqual(['a-1'])
  await visibility('visible')
  expect(result.current.status).toBe('error')
  await act(async () => { result.current.reconnect() })
  await flush()
  expect(result.current.status).toBe('connected')
})

// 验证严格模式回放生命周期不会泄漏会话或重复采样。
it('survives React StrictMode effect replay without leaking or double collecting', async () => {
  // 严格模式风险用例渲染时回放挂载清理，验证会话不会泄漏或重复采集。
  function StrictWrapper({ children }: { children: ReactNode }) {
    return <StrictMode><Wrapper>{children}</Wrapper></StrictMode>
  }
  const { result, unmount } = renderHook(() => useMonitorSession('a', 3), { wrapper: StrictWrapper })
  await flush()
  expect(result.current.history).toHaveLength(1)
  expect(collected).toHaveLength(1)
  unmount()
  expect(closed).toEqual(created)
})

// 验证重建期间保留上次快照供展示，但连接状态必须体现尚未就绪。
it('retains the last snapshot but marks it disconnected while rebuilding', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  let finish!: (value: { sessionId: string; leaseSeconds: number }) => void
  vi.mocked(sessionsApi.create).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  await act(async () => { result.current.reconnect() })
  expect(result.current.status).toBe('connecting')
  expect(result.current.snapshot).toEqual({ ...snapshotFixture, collectedAt: new Date(0).toISOString() })
  expect(result.current.history).toHaveLength(1)
  await act(async () => { finish({ sessionId: 'rebuilt', leaseSeconds: 30 }) })
  await flush()
  expect(result.current.status).toBe('connected')
  expect(result.current.history).toHaveLength(2)
})

// 验证隐藏取消仅终止 HTTP 后，恢复会话会退避等待服务端原生采集结束且不重叠。
it('backs off on a server collection still running after hidden cancellation and resumes the same session', async () => {
  let serverBusy = false
  let finishCollection!: () => void
  let requests = 0
  let activeCollections = 0
  let maxCollections = 0
  vi.mocked(sessionsApi.snapshot).mockImplementation(async (id, signal) => {
    collected.push(id)
    requests++
    if (serverBusy) throw new ApiError(409, { code: 'SNAPSHOT_IN_PROGRESS' })
    activeCollections++
    maxCollections = Math.max(maxCollections, activeCollections)
    if (requests === 2) {
      serverBusy = true
      // 模拟原生采集真正结束时释放服务端忙碌标志，而非在 HTTP 被取消时提前释放。
      finishCollection = () => { serverBusy = false; activeCollections-- }
      // Abort ends only the HTTP request; the simulated server collection stays busy.
      await new Promise<never>((_, reject) => signal!.addEventListener('abort',
        () => reject(new DOMException('HTTP request cancelled', 'AbortError')), { once: true }))
    }
    activeCollections--
    return { ...snapshotFixture, collectedAt: new Date(requests * 1000).toISOString() }
  })
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  await act(async () => { await vi.advanceTimersByTimeAsync(3000) })
  expect(requests).toBe(2)
  await visibility('hidden')
  await visibility('visible')
  expect(requests).toBe(3)
  expect(result.current.status).toBe('connected')
  expect(result.current.error).toBeNull()
  expect(created).toEqual(['a-1'])
  expect(closed).toEqual([])
  await act(async () => { void result.current.refresh(); await vi.advanceTimersByTimeAsync(250) })
  expect(requests).toBe(3)
  finishCollection()
  await act(async () => { await vi.advanceTimersByTimeAsync(250) })
  await flush()
  expect(requests).toBe(4)
  expect(result.current.history).toHaveLength(2)
  expect(result.current.snapshot?.collectedAt).toBe(new Date(4000).toISOString())
  expect(created).toEqual(['a-1'])
  expect(closed).toEqual([])
  expect(maxCollections).toBe(1)
  await act(async () => { await vi.advanceTimersByTimeAsync(3000) })
  await flush()
  expect(requests).toBe(5)
})

// 验证隐藏或卸载取消忙碌退避后不再重试，并按生命周期决定是否释放会话。
it.each(['hidden', 'unmount'] as const)('cancels SNAPSHOT_IN_PROGRESS backoff on %s', async action => {
  let requests = 0
  vi.mocked(sessionsApi.snapshot).mockImplementation(async () => {
    requests++
    throw new ApiError(409, { code: 'SNAPSHOT_IN_PROGRESS' })
  })
  const { result, unmount } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.status).toBe('connecting')
  expect(result.current.error).toBeNull()
  if (action === 'hidden') await visibility('hidden')
  else unmount()
  await act(async () => { await vi.advanceTimersByTimeAsync(20_000) })
  expect(requests).toBe(1)
  expect(created).toEqual(['a-1'])
  expect(closed).toEqual(action === 'hidden' ? [] : ['a-1'])
})

// 验证忙碌退避上限为两秒，且不消耗会话失效的一次重建机会。
it('caps busy backoff and preserves the independent single expiry rebuild allowance', async () => {
  const busy = new ApiError(409, { code: 'SNAPSHOT_IN_PROGRESS' })
  const expired = new ApiError(410, { code: 'SESSION_EXPIRED' })
  const failures = [busy, busy, busy, busy, expired, busy]
  vi.mocked(sessionsApi.snapshot).mockImplementation(async id => {
    collected.push(id)
    const failure = failures.shift()
    if (failure) throw failure
    return snapshotFixture
  })
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  // Retries occur at 500, 1500, 3500, and 5500 ms: the delay caps at 2 seconds.
  await act(async () => { await vi.advanceTimersByTimeAsync(5498) })
  expect(collected).toEqual(['a-1', 'a-1', 'a-1', 'a-1'])
  expect(created).toEqual(['a-1'])
  expect(closed).toEqual([])
  await act(async () => { await vi.advanceTimersByTimeAsync(2) })
  expect(created).toEqual(['a-1', 'a-2'])
  expect(closed).toEqual(['a-1'])
  expect(result.current.error).toBeNull()
  await act(async () => { await vi.advanceTimersByTimeAsync(500) })
  await flush()
  expect(collected).toEqual(['a-1', 'a-1', 'a-1', 'a-1', 'a-1', 'a-2', 'a-2'])
  expect(result.current.status).toBe('connected')
  expect(result.current.history).toHaveLength(1)
})

// 验证本地全量失败以稳定 code 交给公共翻译，切换语言不触发隐式会话重试。
it('localizes synthetic collection failures by stable code without retrying on language changes', async () => {
  vi.mocked(sessionsApi.snapshot).mockResolvedValue({ ...snapshotFixture, topics: [], consumerGroups: [], consumerGroupIds: [],
    partialFailures: [
      { scope: 'topics', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' },
      { scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' },
    ],
  })
  const { result } = renderHook(() => {
    const monitor = useMonitorSession('a', 3)
    const { t } = useTranslation()
    return { monitor, message: localizeApiError(monitor.error, t).message }
  }, { wrapper: Wrapper })
  await flush()
  expect(result.current.monitor.status).toBe('error')
  expect(result.current.monitor.error).toBeInstanceOf(ApiError)
  expect((result.current.monitor.error as ApiError).code).toBe('KAFKA_UNAVAILABLE')
  expect(result.current.message).toBe('Kafka 不可用，请检查连接后重试。')
  const continuity = result.current.monitor.continuityKey
  await act(async () => { await i18n.changeLanguage('en') })
  expect(result.current.message).toBe('Kafka is unavailable. Check the connection and try again.')
  expect(result.current.monitor.continuityKey).toBe(continuity)
  await act(async () => { await vi.advanceTimersByTimeAsync(60_000) })
  expect(created).toEqual(['a-1'])
  expect(closed).toEqual(['a-1'])
  expect(result.current.monitor.history).toHaveLength(0)
})
