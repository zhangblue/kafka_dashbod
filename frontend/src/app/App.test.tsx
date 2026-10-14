import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import App from './App'
import { clustersApi, lagRulesApi, sessionsApi } from '../api/client'
import { clusterFixture, rulesFixture, snapshotFixture } from '../test/handlers'
import type { ClusterSnapshot } from '../api/types'

// Canvas rendering is covered by the chart lifecycle test; jsdom has no canvas context.
vi.mock('echarts/core', () => ({ init: () => ({ setOption() {}, resize() {}, dispose() {} }), use() {} }))

afterEach(() => { vi.restoreAllMocks(); window.history.replaceState({}, '', '/') })

it('renders the dashboard navigation', () => {
  vi.spyOn(clustersApi, 'list').mockResolvedValue([])
  render(<App />)
  expect(screen.getByText('Kafka Lens')).toBeInTheDocument()
  expect(screen.getByRole('navigation')).toBeInTheDocument()
  expect(screen.getByRole('link', { name: '概览' })).toBeInTheDocument()
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

it('retains failed resource display values across overview, topics and groups while using raw samples for totals and timing', async () => {
  const complete: ClusterSnapshot = { clusterId: 'cluster-a', collectedAt: new Date().toISOString(), partialFailures: [], consumerGroupIds: ['worker'],
    topics: [{ topic: 'orders', low: 0, latestMessageOffset: 99, retainedOffsetSpan: 200, incomplete: false, partitions: [
      { partition: 0, low: 0, high: 100, latestMessageOffset: 99, retainedOffsetSpan: 100 },
      { partition: 1, low: 0, high: 100, latestMessageOffset: 99, retainedOffsetSpan: 100 },
    ] }], consumerGroups: [{ groupId: 'worker', topic: 'orders', lag: 100, incomplete: false, unknownPartitionCount: 0, partitions: [
      { partition: 0, high: 100, committedOffset: 50, lag: 50 }, { partition: 1, high: 100, committedOffset: 50, lag: 50 },
    ] }],
  }
  let sample = complete
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue({ defaults: { normalMax: 10, highMin: 80, overLimitSeconds: 30 }, overrides: [] })
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'display', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  vi.spyOn(sessionsApi, 'snapshot').mockImplementation(async () => sample)
  render(<App />)
  await screen.findByText('暂无异常 Consumer Groups。')
  const collect = async (next: ClusterSnapshot) => {
    sample = next
    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }))
    await waitFor(() => expect(screen.getByLabelText('最后更新时间')).toHaveAttribute('datetime', next.collectedAt))
    await waitFor(() => expect(screen.getByRole('button', { name: '手动刷新' })).toBeEnabled())
  }
  const at = (seconds: number) => new Date(Date.parse(complete.collectedAt) + seconds * 1000).toISOString()
  await collect({ ...complete, collectedAt: at(10), partialFailures: [{ scope: 'committed', resource: 'worker/orders:0', code: 'KAFKA_TIMEOUT', message: 'timeout' }],
    consumerGroups: [{ ...complete.consumerGroups[0], lag: 20, incomplete: true, unknownPartitionCount: 1, partitions: [
      { partition: 0, high: 100, committedOffset: null, lag: null }, { partition: 1, high: 100, committedOffset: 80, lag: 20 },
    ] }] })
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('20 已知 Lag')
  const overviewRow = screen.getByRole('row', { name: /worker \/ orders/ })
  expect(overviewRow).toHaveTextContent('70')
  expect(overviewRow).toHaveTextContent('采集异常')
  expect(overviewRow).toHaveTextContent('数据已陈旧')
  expect(within(overviewRow).getByRole('time')).toHaveAttribute('datetime', complete.collectedAt)
  const chartRows = within(screen.getByRole('table', { name: '最近 1 小时总 Lag 数据' })).getAllByRole('row')
  expect(chartRows.at(-1)).toHaveTextContent('未知 / 不完整')
  fireEvent.click(screen.getByRole('link', { name: 'worker / orders' }))
  expect(screen.getByRole('table', { name: 'worker / orders Partitions' })).toHaveTextContent('50')
  expect(screen.getByRole('row', { name: /收起 worker orders/ })).toHaveTextContent('70')

  await collect({ ...complete, collectedAt: at(20), partialFailures: [{ scope: 'watermark', resource: 'orders:0', code: 'KAFKA_TIMEOUT', message: 'timeout' }],
    topics: [{ ...complete.topics[0], low: 10, latestMessageOffset: 119, retainedOffsetSpan: 110, incomplete: true, partitions: [
      { partition: 0, low: null, high: null, latestMessageOffset: null, retainedOffsetSpan: null },
      { partition: 1, low: 10, high: 120, latestMessageOffset: 119, retainedOffsetSpan: 110 },
    ] }], consumerGroups: [{ ...complete.consumerGroups[0], lag: 30, incomplete: true, unknownPartitionCount: 1, partitions: [
      { partition: 0, high: null, committedOffset: 60, lag: null }, { partition: 1, high: 120, committedOffset: 90, lag: 30 },
    ] }] })
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  fireEvent.click(screen.getByRole('button', { name: '展开 orders' }))
  const partitionRows = within(screen.getByRole('table', { name: 'orders Partitions' })).getAllByRole('row')
  expect(partitionRows[1]).toHaveTextContent('100')
  expect(partitionRows[1]).toHaveTextContent('数据已陈旧')
  expect(partitionRows[2]).toHaveTextContent('120')
  expect(partitionRows[2]).not.toHaveTextContent('数据已陈旧')

  await collect({ ...complete, collectedAt: at(30), consumerGroupIds: [], consumerGroups: [],
    partialFailures: [{ scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' }] })
  fireEvent.click(screen.getByRole('link', { name: '概览' }))
  expect(screen.getByRole('link', { name: 'worker / orders' })).toBeInTheDocument()
  expect(screen.getByRole('region', { name: 'Consumer Group 数' })).toHaveTextContent('1')
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('—')
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(screen.getByRole('row', { name: /worker orders/ })).toHaveTextContent('采集异常')
  await collect({ ...complete, collectedAt: at(60) })
  expect(screen.getByRole('row', { name: /worker orders/ })).toHaveTextContent('正常')
  expect(screen.getByRole('row', { name: /worker orders/ })).not.toHaveTextContent('数据已陈旧')
})
