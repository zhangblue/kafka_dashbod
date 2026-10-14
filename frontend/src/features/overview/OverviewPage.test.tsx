import { act, render, screen, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import { lagRulesApi } from '../../api/client'
import type { ClusterSnapshot, ConsumerGroupTopicSnapshot } from '../../api/types'
import { clusterFixture } from '../../test/handlers'
import { ConsumerGroupLagProvider } from '../consumer-groups/ConsumerGroupLagProvider'
import type { DashboardContext } from '../dashboard/useDashboard'
import { OverviewPage } from './OverviewPage'
import { retainDisplaySnapshot } from '../dashboard/displaySnapshot'

vi.mock('echarts/core', () => ({ init: () => ({ setOption() {}, resize() {}, dispose() {} }), use() {} }))
beforeEach(() => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.setSystemTime(new Date('2026-10-15T00:00:00Z'))
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue({ defaults: { normalMax: 999, highMin: 5000, overLimitSeconds: 0 }, overrides: [] })
})
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks() })

const row = (groupId: string, lag: number): ConsumerGroupTopicSnapshot => ({ groupId, topic: 'orders', lag,
  unknownPartitionCount: 0, incomplete: false, partitions: [{ partition: 0, high: 10000, committedOffset: 10000 - lag, lag }] })
function fixture(): ClusterSnapshot {
  return { clusterId: 'cluster-a', collectedAt: '2026-10-15T00:00:00Z',
    topics: [{ topic: 'orders', low: 0, latestMessageOffset: 9999, retainedOffsetSpan: 10000, incomplete: false,
      partitions: [{ partition: 0, low: 0, high: 10000, latestMessageOffset: 9999, retainedOffsetSpan: 10000 }] }],
    consumerGroupIds: ['normal', 'backlog', 'high', 'uncommitted', 'failed'],
    consumerGroups: [row('normal', 5), row('backlog', 1200), row('high', 6000),
      { ...row('uncommitted', 0), incomplete: true, unknownPartitionCount: 1, partitions: [{ partition: 0, high: 10000, committedOffset: null, lag: null }] },
      { ...row('failed', 0), incomplete: true, unknownPartitionCount: 1, partitions: [{ partition: 0, high: 10000, committedOffset: null, lag: null }] }],
    partialFailures: [{ scope: 'committed', resource: 'failed', code: 'TIMEOUT', message: 'timeout' }] }
}
function mount(snapshot: ClusterSnapshot | null) {
  const context: DashboardContext = { cluster: clusterFixture, refreshSeconds: 5, monitor: { snapshot,
    history: snapshot ? [{ at: Date.parse(snapshot.collectedAt), snapshot }] : [], status: 'connected', error: null,
    isFetching: false, continuityKey: 'a', refresh: async () => {}, reconnect: vi.fn() } }
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const tree = () => <QueryClientProvider client={client}><MemoryRouter><Routes><Route element={
    <ConsumerGroupLagProvider clusterId="cluster-a" monitor={context.monitor}><Outlet context={context} /></ConsumerGroupLagProvider>
  }><Route index element={<OverviewPage />} /></Route></Routes></MemoryRouter></QueryClientProvider>
  const view = render(tree())
  return { context, update: (patch: Partial<DashboardContext['monitor']>, refreshSeconds = context.refreshSeconds) => {
    Object.assign(context.monitor, patch); context.refreshSeconds = refreshSeconds; view.rerender(tree())
  } }
}

it('aggregates known Lag, counts unique abnormal groups and orders actionable rows without classifying normal nonzero lag', async () => {
  const snapshot = fixture()
  snapshot.consumerGroups.push({ ...row('high', 10), topic: 'secondary' })
  mount(snapshot)
  await screen.findByRole('table', { name: '异常 Consumer Groups' })
  expect(screen.getByRole('region', { name: 'Topic 数' })).toHaveTextContent('1')
  expect(screen.getByRole('region', { name: 'Consumer Group 数' })).toHaveTextContent('5')
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('7,215')
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('已知 Lag')
  expect(screen.getByRole('region', { name: '异常 Consumer Group 数' })).toHaveTextContent('4')
  const table = screen.getByRole('table', { name: '异常 Consumer Groups' })
  expect(within(table).queryByRole('link', { name: 'normal / orders' })).not.toBeInTheDocument()
  expect(within(table).getAllByRole('link').map(link => link.textContent)).toEqual(['high / orders', 'backlog / orders', 'failed / orders', 'uncommitted / orders'])
  expect(within(table).getByRole('link', { name: 'high / orders' })).toHaveAttribute('href', '/consumer-groups?groupId=high&topic=orders')
  expect(within(table).getByRole('row', { name: /failed \/ orders/ })).toHaveTextContent('数据已陈旧')
  expect(within(table).getByRole('row', { name: /uncommitted \/ orders/ })).not.toHaveTextContent('数据已陈旧')
  expect(screen.getByRole('table', { name: '最近 1 小时总 Lag 数据' })).toHaveTextContent('未知 / 不完整')
})

it('shows a skeleton only before the first sample and preserves the snapshot during refresh and disconnect', async () => {
  const view = mount(null)
  expect(screen.getByLabelText('正在加载概览')).toBeInTheDocument()
  view.update({ snapshot: fixture() })
  await screen.findByRole('table', { name: '异常 Consumer Groups' })
  view.update({ isFetching: true })
  expect(screen.queryByLabelText('正在加载概览')).not.toBeInTheDocument()
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('7,205')
  view.update({ status: 'error', error: new Error('offline'), isFetching: false })
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('7,205')
  expect(screen.getByLabelText('图表数据已陈旧')).toBeInTheDocument()
})

it('marks data stale only after two current refresh intervals and recalculates when the interval changes', async () => {
  const snapshot = fixture(); snapshot.consumerGroups = [row('normal', 5)]; snapshot.partialFailures = []
  const view = mount(snapshot)
  await screen.findByText('暂无异常 Consumer Groups。')
  expect(screen.queryByText('数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(10000))
  expect(screen.queryByText('数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(1000))
  expect(screen.getByText('数据已陈旧')).toBeInTheDocument()
  view.update({}, 10)
  expect(screen.queryByText('数据已陈旧')).not.toBeInTheDocument()
})

it('shows no fabricated zero total when discovery fails without any known rows', async () => {
  const snapshot = fixture(); snapshot.consumerGroups = []; snapshot.consumerGroupIds = []
  snapshot.partialFailures = [{ scope: 'groups', resource: 'cluster-a', code: 'TIMEOUT', message: 'timeout' }]
  mount(snapshot)
  const card = screen.getByRole('region', { name: '总 Lag' })
  expect(card).toHaveTextContent('—')
  expect(card).toHaveTextContent('不完整')
})

it('distinguishes fresh uncommitted data from stale collection failures', async () => {
  const snapshot = fixture()
  snapshot.consumerGroups = [snapshot.consumerGroups[3]]
  snapshot.partialFailures = []
  mount(snapshot)
  await screen.findByRole('table', { name: '异常 Consumer Groups' })
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('不完整')
  expect(screen.queryByLabelText('图表数据已陈旧')).not.toBeInTheDocument()
})

it.each(['topics', 'groups'])('does not count confirmed-absent group/topic rows as abnormal when %s discovery fails', async scope => {
  const previous = fixture()
  previous.consumerGroups = [row('worker', 6000)]; previous.consumerGroupIds = ['worker']; previous.partialFailures = []
  const raw: ClusterSnapshot = { ...previous, topics: [], consumerGroupIds: [], consumerGroups: [],
    partialFailures: [{ scope, resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' }] }
  const display = retainDisplaySnapshot(raw, retainDisplaySnapshot(previous))
  const view = mount(raw)
  view.update({ displaySnapshot: display.snapshot, staleResources: display.staleResources })
  await screen.findByText('暂无异常 Consumer Groups。')
  expect(screen.queryByRole('link', { name: 'worker / orders' })).not.toBeInTheDocument()
  expect(screen.getByRole('region', { name: '异常 Consumer Group 数' })).toHaveTextContent('0')
  expect(screen.getByRole('region', { name: 'Consumer Group 数' })).toHaveTextContent(scope === 'groups' ? '1' : '0')
})
