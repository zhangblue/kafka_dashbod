import { act, renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { StrictMode, useState, type ReactNode } from 'react'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { ApiError, sessionsApi } from '../../api/client'
import { snapshotFixture } from '../../test/handlers'
import { useMonitorSession } from './useMonitorSession'

let sequence: number
let created: string[]
let closed: string[]
let collected: string[]
let clock: number
function Wrapper({ children }: { children: ReactNode }) {
  const [client] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: false } } }))
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}
async function flush() { await act(async () => { await vi.advanceTimersByTimeAsync(1) }) }
async function visibility(value: DocumentVisibilityState) {
  await act(async () => {
    Object.defineProperty(document, 'visibilityState', { configurable: true, value })
    document.dispatchEvent(new Event('visibilitychange'))
  })
  await flush()
}
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
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })

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

it('never creates a session when mounted hidden or disabled', async () => {
  await visibility('hidden')
  const { rerender } = renderHook(({ active }) => useMonitorSession('a', 3, active), { wrapper: Wrapper, initialProps: { active: true } })
  await flush()
  expect(created).toEqual([])
  rerender({ active: false })
  await visibility('visible')
  expect(created).toEqual([])
})

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

it('releases a session whose creation finishes after unmount', async () => {
  let finish!: (value: { sessionId: string; leaseSeconds: number }) => void
  vi.mocked(sessionsApi.create).mockImplementation(() => new Promise(resolve => { finish = resolve }))
  const { unmount } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush(); unmount()
  await act(async () => { finish({ sessionId: 'late', leaseSeconds: 30 }) })
  expect(closed).toContain('late')
  expect(collected).toEqual([])
})

it('releases its session with keepalive on pagehide and stays stopped', async () => {
  const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response(null, { status: 204 }))
  renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  await act(async () => { window.dispatchEvent(new Event('pagehide')) })
  expect(fetch).toHaveBeenCalledWith(expect.anything(), expect.objectContaining({ method: 'DELETE', keepalive: true }))
  await act(async () => { await vi.advanceTimersByTimeAsync(10_000) })
  expect(collected).toEqual(['a-1'])
})

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

it('keeps unknown partition values and partial failures unchanged in history', async () => {
  const partial = { ...snapshotFixture, partialFailures: [{ scope: 'watermark', resource: 'topic:0', code: 'TIMEOUT', message: 'timeout' }] }
  vi.mocked(sessionsApi.snapshot).mockResolvedValue(partial)
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.snapshot).toEqual(partial)
  expect(result.current.history[0].snapshot.consumerGroups[1].partitions[0].lag).toBeNull()
  expect(result.current.history[0].snapshot.partialFailures).toEqual(partial.partialFailures)
})

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

it('survives React StrictMode effect replay without leaking or double collecting', async () => {
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
