import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MemoryRouter, Outlet, Route, Routes, useOutletContext } from 'react-router'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { clustersApi, sessionsApi } from '../../api/client'
import { clusterFixture, snapshotFixture } from '../../test/handlers'
import { DashboardLayout, type DashboardContext } from './DashboardLayout'

let closed: string[]
let created: string[]
beforeEach(() => {
  created = []; closed = []
  Object.defineProperty(document, 'visibilityState', { configurable: true, value: 'visible' })
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture, { ...clusterFixture, id: 'cluster-b', name: '备用 Kafka' }])
  vi.spyOn(sessionsApi, 'create').mockImplementation(async id => { created.push(id); return { sessionId: id, leaseSeconds: 30 } })
  vi.spyOn(sessionsApi, 'snapshot').mockImplementation(async id => ({ ...snapshotFixture, clusterId: id }))
  vi.spyOn(sessionsApi, 'close').mockImplementation(async id => { closed.push(id) })
})
afterEach(() => vi.restoreAllMocks())
function Consumer() {
  const { cluster, monitor } = useOutletContext<DashboardContext>()
  return <p>当前：{cluster?.name}，样本：{monitor.history.length}</p>
}
function renderLayout() {
  return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
    <MemoryRouter><Routes><Route element={<DashboardLayout />}><Route index element={<Consumer />} /></Route></Routes><Outlet /></MemoryRouter>
  </QueryClientProvider>)
}

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

it('clears the previous cluster and releases its session on cluster change', async () => {
  renderLayout()
  await screen.findByText('当前：生产 Kafka，样本：1')
  fireEvent.change(screen.getByRole('combobox', { name: '集群' }), { target: { value: 'cluster-b' } })
  await screen.findByText('当前：备用 Kafka，样本：1')
  expect(closed).toContain('cluster-a')
  expect(created).toEqual(['cluster-a', 'cluster-b'])
})

it('offers cluster configuration without opening a session when none exists', async () => {
  vi.mocked(clustersApi.list).mockResolvedValue([])
  renderLayout()
  expect(await screen.findByRole('link', { name: '配置集群' })).toHaveAttribute('href', '/settings')
  expect(created).toEqual([])
})

it('preserves partial snapshots and explicit unknowns in the outlet history', async () => {
  const partial = { ...snapshotFixture, partialFailures: [{ scope: 'watermark', resource: 'orders:0', code: 'TIMEOUT', message: 'timeout' }] }
  vi.mocked(sessionsApi.snapshot).mockResolvedValue(partial)
  renderLayout()
  await act(async () => {})
  expect(await screen.findByText(/部分数据采集失败/)).toBeInTheDocument()
  expect(screen.getByText('当前：生产 Kafka，样本：1')).toBeInTheDocument()
  expect(screen.getByText('已连接')).toBeInTheDocument()
})
