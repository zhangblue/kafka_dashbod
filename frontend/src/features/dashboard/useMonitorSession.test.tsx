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
afterEach(async () => {
  // Vitest runs this file's teardown before setup.ts's automatic cleanup.
  // Release hooks while the API fakes still own their resources, then await the
  // page-wide queue before restoring fetch clients or starting the next test.
  cleanup()
  await sessionTransition(async () => {})
  vi.restoreAllMocks()
  vi.useRealTimers()
})

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

it('withdraws an expired ID even when the next snapshot remains pending', async () => {
  const { result } = renderHook(() => useMonitorSession('a', 3), { wrapper: Wrapper })
  await flush()
  expect(result.current.sessionId).toBe('a-1')
  vi.mocked(sessionsApi.snapshot).mockImplementationOnce(() => new Promise(() => {}))
  await act(async () => { await vi.advanceTimersByTimeAsync(30_001) })
  expect(result.current.sessionId).toBeUndefined()
  expect(result.current.status).toBe('connecting')
})

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
