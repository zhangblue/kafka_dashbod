import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import App from './App'
import { clustersApi, lagRulesApi, sessionsApi } from '../api/client'
import { clusterFixture, rulesFixture, snapshotFixture } from '../test/handlers'

afterEach(() => { vi.restoreAllMocks(); window.history.replaceState({}, '', '/') })

it('renders the dashboard navigation', () => {
  vi.spyOn(clustersApi, 'list').mockResolvedValue([])
  render(<App />)
  expect(screen.getByText('Kafka Lens')).toBeInTheDocument()
  expect(screen.getByRole('navigation')).toBeInTheDocument()
  expect(screen.getByText('概览')).toBeInTheDocument()
  expect(screen.getByText('Topics')).toBeInTheDocument()
  expect(screen.getByText('Consumer Groups')).toBeInTheDocument()
  expect(screen.getByText('集群设置')).toBeInTheDocument()
})

it('opens settings without retaining a dashboard session and supports direct settings entry', async () => {
  const closed: string[] = []
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'monitor', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'snapshot').mockResolvedValue(snapshotFixture)
  vi.spyOn(sessionsApi, 'close').mockImplementation(async id => { closed.push(id) })
  const first = render(<App />)
  await screen.findByText('已连接')
  fireEvent.click(screen.getByRole('link', { name: '集群设置' }))
  expect(await screen.findByRole('button', { name: '新增集群' })).toBeInTheDocument()
  expect(closed).toEqual(['monitor'])
  first.unmount()
  vi.mocked(sessionsApi.create).mockClear()
  render(<App />)
  expect(await screen.findByRole('button', { name: '新增集群' })).toBeInTheDocument()
  expect(sessionsApi.create).not.toHaveBeenCalled()
})

it('opens the Consumer Groups route with real snapshot rows', async () => {
  window.history.replaceState({}, '', '/consumer-groups')
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue(rulesFixture)
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'groups', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'snapshot').mockResolvedValue(snapshotFixture)
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  render(<App />)
  expect(await screen.findByRole('table', { name: 'Consumer Groups 汇总' })).toBeInTheDocument()
  expect(screen.getByText('audit-worker')).toBeInTheDocument()
})

// Catches lag state being owned by a route that unmounts while the monitoring session remains live.
it('preserves lag duration and trend across Topics navigation without replaying retained samples', async () => {
  window.history.replaceState({}, '', '/consumer-groups')
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue(rulesFixture)
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'groups', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  let seconds = 0
  let lag = 6000
  vi.spyOn(sessionsApi, 'snapshot').mockImplementation(async () => ({ ...snapshotFixture,
    collectedAt: new Date(Date.parse(snapshotFixture.collectedAt) + seconds * 1000).toISOString(),
    consumerGroups: [{ groupId: 'busy-worker', topic: 'events', lag, unknownPartitionCount: 0, incomplete: false,
      partitions: [{ partition: 0, high: 10000, committedOffset: 10000 - lag, lag }] }],
  }))
  render(<App />)
  const dataRow = () => screen.getByRole('row', { name: /busy-worker/ })
  await screen.findByRole('table', { name: 'Consumer Groups 汇总' })
  const refresh = async (nextSeconds: number, nextLag: number) => {
    seconds = nextSeconds; lag = nextLag
    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }))
    await waitFor(() => expect(screen.getByLabelText('最后更新时间')).toHaveAttribute('datetime', new Date(Date.parse(snapshotFixture.collectedAt) + seconds * 1000).toISOString()))
    await waitFor(() => expect(screen.getByRole('button', { name: '手动刷新' })).toBeEnabled())
  }
  await refresh(20, 6500)
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 上升')).toBeInTheDocument()
  await refresh(30, 7000)
  expect(within(dataRow()).getByText('高 Lag')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  // Collection continues on Topics; the duration gate remains satisfied after a change of range.
  await refresh(40, 2000)
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(within(dataRow()).getByText('积压')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 下降')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(within(dataRow()).getByText('积压')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 下降')).toBeInTheDocument()
})
