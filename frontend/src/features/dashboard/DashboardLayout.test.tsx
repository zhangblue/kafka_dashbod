import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MemoryRouter, Outlet, Route, Routes, useOutletContext } from 'react-router'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { clustersApi, sessionsApi } from '../../api/client'
import { clusterFixture, snapshotFixture } from '../../test/handlers'
import { DashboardLayout, type DashboardContext } from './DashboardLayout'

let closed: string[]
let created: string[]
// 各用例前重置可见性、时钟和会话替身，确保生命周期场景相互隔离。
beforeEach(() => {
  created = []; closed = []
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture, { ...clusterFixture, id: 'cluster-b', name: '备用 Kafka' }])
  vi.spyOn(sessionsApi, 'create').mockImplementation(async id => { created.push(id); return { sessionId: id, leaseSeconds: 30 } })
  vi.spyOn(sessionsApi, 'snapshot').mockImplementation(async id => ({ ...snapshotFixture, clusterId: id }))
  vi.spyOn(sessionsApi, 'close').mockImplementation(async id => { closed.push(id) })
})
// 每个布局用例结束后恢复 API 替身和真实计时器，避免模拟时钟影响后续用例。
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })
// 布局测试渲染子路由时读取上下文，验证集群与历史是否及时更新。
function Consumer() {
  const { cluster, monitor } = useOutletContext<DashboardContext>()
  return <p>当前：{cluster?.name}，样本：{monitor.history.length}</p>
}
// 每个布局用例开始时挂载独立查询客户端和内存路由，避免共享缓存干扰。
function renderLayout() {
  return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
    <MemoryRouter><Routes><Route element={<DashboardLayout />}><Route index element={<Consumer />} /></Route></Routes><Outlet /></MemoryRouter>
  </QueryClientProvider>)
}

// 验证公共顶部栏和子路由上下文同步显示当前集群、刷新频率、采集时间与手动刷新状态。
it('shows cluster, interval, last server collection time, manual refresh and child context', async () => {
  renderLayout()
  await screen.findByText('当前：生产 Kafka，样本：1')
  expect(screen.getByRole('combobox', { name: '集群' })).toHaveValue('cluster-a')
  expect(screen.getByRole('combobox', { name: '刷新频率' })).toHaveValue('5')
  expect(screen.getByText('已连接')).toBeInTheDocument()
  expect(screen.getByLabelText('最后更新时间')).toHaveAttribute('datetime', snapshotFixture.collectedAt)
  fireEvent.change(screen.getByRole('combobox', { name: '刷新频率' }), { target: { value: '10' } })
  expect(screen.getByText('当前：生产 Kafka，样本：1')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('button', { name: '手动刷新' }))
  await waitFor(() => expect(screen.getByRole('button', { name: '手动刷新' })).not.toBeDisabled())
})

// 验证慢采样期间按时间显示陈旧提示，并随刷新频率变化重新判断阈值。
it('exposes time-stale data in the common header while a slow sample is pending', async () => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.setSystemTime(new Date(snapshotFixture.collectedAt))
  renderLayout()
  await screen.findByText('当前：生产 Kafka，样本：1')
  vi.mocked(sessionsApi.snapshot).mockImplementation(() => new Promise(() => {}))
  expect(screen.queryByLabelText('监控数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(11000))
  expect(screen.getByLabelText('监控数据已陈旧')).toBeInTheDocument()
  fireEvent.change(screen.getByRole('combobox', { name: '刷新频率' }), { target: { value: '10' } })
  expect(screen.queryByLabelText('监控数据已陈旧')).not.toBeInTheDocument()
})

// 验证切换集群时清空旧展示并释放旧会话，防止跨集群复用资源。
it('clears the previous cluster and releases its session on cluster change', async () => {
  renderLayout()
  await screen.findByText('当前：生产 Kafka，样本：1')
  fireEvent.change(screen.getByRole('combobox', { name: '集群' }), { target: { value: 'cluster-b' } })
  await screen.findByText('当前：备用 Kafka，样本：1')
  expect(closed).toContain('cluster-a')
  expect(created).toEqual(['cluster-a', 'cluster-b'])
})

// 验证无配置时提供设置入口且不创建监控会话。
it('offers cluster configuration without opening a session when none exists', async () => {
  vi.mocked(clustersApi.list).mockResolvedValue([])
  renderLayout()
  expect(await screen.findByRole('link', { name: '配置集群' })).toHaveAttribute('href', '/settings')
  expect(created).toEqual([])
})

// 验证部分失败快照与未知值仍进入共享历史，避免被当作全量断连。
it('preserves partial snapshots and explicit unknowns in the outlet history', async () => {
  const partial = { ...snapshotFixture, partialFailures: [{ scope: 'watermark', resource: 'orders:0', code: 'TIMEOUT', message: 'timeout' }] }
  vi.mocked(sessionsApi.snapshot).mockResolvedValue(partial)
  renderLayout()
  await act(async () => {})
  expect(await screen.findByText(/部分数据采集失败/)).toBeInTheDocument()
  expect(screen.getByText('当前：生产 Kafka，样本：1')).toBeInTheDocument()
  expect(screen.getByText('已连接')).toBeInTheDocument()
})

// 验证断连保留最后快照，显式重连会建立新会话。
it('retains the last snapshot after disconnection and reconnects with a new session', async () => {
  let requests = 0
  vi.mocked(sessionsApi.snapshot).mockImplementation(async () => {
    requests++
    if (requests === 2) throw new Error('Broker unavailable')
    return snapshotFixture
  })
  renderLayout()
  await screen.findByText('当前：生产 Kafka，样本：1')
  fireEvent.click(screen.getByRole('button', { name: '手动刷新' }))
  expect(await screen.findByText('连接已断开，显示最后一次快照')).toBeInTheDocument()
  expect(screen.getByText('当前：生产 Kafka，样本：1')).toBeInTheDocument()
  expect(screen.getByLabelText('最后更新时间')).toHaveAttribute('datetime', snapshotFixture.collectedAt)
  fireEvent.click(screen.getByRole('button', { name: '重新连接' }))
  await screen.findByText('已连接')
  expect(created).toEqual(['cluster-a', 'cluster-a'])
})

// 验证 HTTP 200 的全量发现失败仍视为断连，并允许用户显式重连。
it('shows disconnection and reconnect for an HTTP 200 empty collector snapshot with cluster discovery failures', async () => {
  vi.mocked(sessionsApi.snapshot).mockResolvedValueOnce(snapshotFixture).mockResolvedValueOnce({
    clusterId: 'cluster-a', collectedAt: '2026-10-15T00:00:05Z', topics: [], consumerGroupIds: [], consumerGroups: [],
    partialFailures: [
      { scope: 'topics', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' },
      { scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' },
    ],
  }).mockResolvedValue(snapshotFixture)
  renderLayout()
  await screen.findByText('当前：生产 Kafka，样本：1')
  fireEvent.click(screen.getByRole('button', { name: '手动刷新' }))
  await screen.findByText('连接已断开，显示最后一次快照')
  expect(screen.getByText('当前：生产 Kafka，样本：1')).toBeInTheDocument()
  expect(screen.getByLabelText('最后更新时间')).toHaveAttribute('datetime', snapshotFixture.collectedAt)
  fireEvent.click(screen.getByRole('button', { name: '重新连接' }))
  await screen.findByText('已连接')
  expect(created).toEqual(['cluster-a', 'cluster-a'])
})
