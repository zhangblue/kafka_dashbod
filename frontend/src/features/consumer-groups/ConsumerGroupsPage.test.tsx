import { act, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router'
import { afterEach, expect, it, vi } from 'vitest'
import type { ClusterSnapshot, ConsumerGroupTopicSnapshot, LagRuleSet } from '../../api/types'
import { clusterFixture, snapshotFixture } from '../../test/handlers'
import type { DashboardContext } from '../dashboard/useDashboard'
import { ConsumerGroupsPage } from './ConsumerGroupsPage'
import { ConsumerGroupLagProvider } from './ConsumerGroupLagProvider'

const chart = vi.hoisted(() => ({ setOption: vi.fn(), resize() {}, dispose() {} }))
vi.mock('echarts/core', () => ({ init: () => chart, use() {} }))
afterEach(() => { vi.useRealTimers(); vi.clearAllMocks() })

const row = (groupId: string, topic: string, lag: number): ConsumerGroupTopicSnapshot => ({ groupId, topic, lag,
  incomplete: false, unknownPartitionCount: 0, partitions: [{ partition: 0, high: 50000, committedOffset: 50000 - lag, lag }] })
const snapshot: ClusterSnapshot = { ...snapshotFixture, consumerGroups: [row('low-worker', 'audit', 3),
  row('flink-orders', 'orders', 13000), row('busy-worker', 'events', 5000),
  { ...row('new-worker', 'new', 50), incomplete: true, unknownPartitionCount: 1,
    partitions: [row('x', 'x', 50).partitions[0], { partition: 1, high: 100, committedOffset: null, lag: null }] }],
}
const rules: LagRuleSet = { defaults: { normalMax: 999, highMin: 5000, overLimitSeconds: 0 },
  overrides: [{ groupId: 'flink-orders', topic: 'orders', normalMax: 12000, highMin: 30000, overLimitSeconds: 120 }] }

function show(value: ClusterSnapshot | null = snapshot, ruleSet = rules, entry = '/') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })
  client.setQueryData(['lag-rules', clusterFixture.id], ruleSet)
  const context: DashboardContext = { cluster: clusterFixture, monitor: { snapshot: value, history: [], status: 'connected',
    error: null, isFetching: false, continuityKey: 'a', refresh: async () => {}, reconnect: () => {} } }
  const tree = () => <QueryClientProvider client={client}><MemoryRouter initialEntries={[entry]}><Routes><Route element={
    <ConsumerGroupLagProvider clusterId={context.cluster!.id} monitor={context.monitor}><Outlet context={{ ...context }} /></ConsumerGroupLagProvider>}>
    <Route index element={<ConsumerGroupsPage />} />
  </Route></Routes></MemoryRouter></QueryClientProvider>
  const view = render(tree())
  return { update: (patch: Partial<DashboardContext['monitor']>, refreshSeconds = context.refreshSeconds) => {
    context.monitor = { ...context.monitor, ...patch }; context.refreshSeconds = refreshSeconds; view.rerender(tree())
  }, client }
}

// Catches sorting lexicographically, filtering on only one field, and status filtering before evaluation.
it('sorts by numeric lag descending and filters by group, topic and evaluated status', async () => {
  const user = userEvent.setup()
  show()
  const table = screen.getByRole('table', { name: 'Consumer Groups 汇总' })
  expect(within(table).getAllByRole('row').slice(1).map(item => within(item).getAllByRole('cell')[0].textContent))
    .toEqual(['展开 flink-orders', '展开 busy-worker', '展开 new-worker', '展开 low-worker'])
  await user.type(screen.getByRole('searchbox', { name: '搜索 Consumer Group 或 Topic' }), 'ORDERS')
  expect(within(table).getAllByRole('row')).toHaveLength(2)
  await user.clear(screen.getByRole('searchbox'))
  await user.selectOptions(screen.getByRole('combobox', { name: '状态筛选' }), 'high')
  expect(within(table).getByText('busy-worker')).toBeInTheDocument()
  expect(within(table).queryByText('flink-orders')).not.toBeInTheDocument()
  await user.selectOptions(screen.getByRole('combobox', { name: '状态筛选' }), 'uncommitted')
  expect(within(table).getByText('new-worker')).toBeInTheDocument()
})

it('selects multiple topic and partition raw histories with null gaps and retains them when cadence changes', async () => {
  const user = userEvent.setup()
  const first = { ...snapshot, consumerGroups: [row('worker', 'orders', 12), row('worker', 'events', 6)] }
  const second = { ...first, collectedAt: '2026-10-15T00:00:05Z', consumerGroups: [
    { ...row('worker', 'orders', 0), incomplete: true, unknownPartitionCount: 1,
      partitions: [{ partition: 0, high: null, committedOffset: 10, lag: null }] },
    row('worker', 'events', 9),
  ] }
  const { update } = show(second)
  update({ displaySnapshot: first, history: [{ at: 1000, snapshot: first }, { at: 2000, snapshot: second }] })
  expect(chart.setOption).toHaveBeenLastCalledWith(expect.objectContaining({ series: [
    expect.objectContaining({ name: 'orders / 总 Lag', data: [[1000, 12], [2000, null]] }),
  ] }), true)
  await user.click(screen.getByRole('checkbox', { name: 'orders / 总 Lag' }))
  await user.click(screen.getByRole('checkbox', { name: 'orders / Partition 0' }))
  await user.click(screen.getByRole('checkbox', { name: 'events / Partition 0' }))
  expect(chart.setOption).toHaveBeenLastCalledWith(expect.objectContaining({ series: [
    expect.objectContaining({ name: 'orders / Partition 0', data: [[1000, 12], [2000, null]], connectNulls: false }),
    expect.objectContaining({ name: 'events / Partition 0', data: [[1000, 6], [2000, 9]], connectNulls: false }),
  ] }), true)
  expect(screen.getByRole('table', { name: 'worker Lag 历史数据' })).toHaveTextContent('未知 / 不完整')
  update({}, 10)
  expect(screen.getByRole('checkbox', { name: 'orders / Partition 0' })).toBeChecked()
  expect(chart.setOption).toHaveBeenLastCalledWith(expect.objectContaining({ series: [
    expect.objectContaining({ data: [[1000, 12], [2000, null]] }), expect.objectContaining({ data: [[1000, 6], [2000, 9]] }),
  ] }), true)
  await user.click(screen.getByRole('checkbox', { name: 'orders / Partition 0' }))
  expect(chart.setOption).toHaveBeenLastCalledWith(expect.objectContaining({ series: [expect.objectContaining({ name: 'events / Partition 0' })] }), true)
})

it('gaps only the affected partition on local failure and every selected partition on group discovery failure', async () => {
  const user = userEvent.setup()
  const first = { ...snapshot, consumerGroups: [{ ...row('worker', 'orders', 11), partitions: [
    { partition: 0, high: 10, committedOffset: 5, lag: 5 }, { partition: 1, high: 10, committedOffset: 4, lag: 6 },
  ] }] }
  const partitionFailure = { ...first, partialFailures: [{ scope: 'committed', resource: 'worker/orders:1', code: 'TIMEOUT', message: 'timeout' }] }
  const groupFailure = { ...first, partialFailures: [{ scope: 'groups', resource: 'worker', code: 'TIMEOUT', message: 'timeout' }] }
  const { update } = show(first)
  update({ history: [{ at: 1000, snapshot: first }, { at: 2000, snapshot: partitionFailure }, { at: 3000, snapshot: groupFailure }] })
  await user.click(screen.getByRole('checkbox', { name: 'orders / 总 Lag' }))
  await user.click(screen.getByRole('checkbox', { name: 'orders / Partition 0' }))
  await user.click(screen.getByRole('checkbox', { name: 'orders / Partition 1' }))
  expect(chart.setOption).toHaveBeenLastCalledWith(expect.objectContaining({ series: [
    expect.objectContaining({ name: 'orders / Partition 0', data: [[1000, 5], [2000, 5], [3000, null]] }),
    expect.objectContaining({ name: 'orders / Partition 1', data: [[1000, 6], [2000, null], [3000, null]] }),
  ] }), true)
})

it('shows nonempty maximum offsets and dashes for empty or unknown watermarks', () => {
  const value: ClusterSnapshot = { ...snapshot, topics: [{ topic: 'orders', low: 0, latestMessageOffset: 9, retainedOffsetSpan: 10, incomplete: true,
    partitions: [{ partition: 0, low: 0, high: 10, latestMessageOffset: 9, retainedOffsetSpan: 10 },
      { partition: 1, low: 20, high: 20, latestMessageOffset: null, retainedOffsetSpan: 0 },
      { partition: 2, low: null, high: null, latestMessageOffset: null, retainedOffsetSpan: null }] }],
    consumerGroups: [{ ...row('worker', 'orders', 5), partitions: [
      { partition: 0, high: 11, committedOffset: 5, lag: 6 },
      { partition: 1, high: 20, committedOffset: 20, lag: 0 },
      { partition: 2, high: null, committedOffset: 3, lag: null }] }] }
  show(value, rules, '/?groupId=worker&topic=orders')
  const table = screen.getByRole('table', { name: 'worker / orders Partitions' })
  expect(within(table).getByRole('columnheader', { name: /最大消息 Offset/ })).toBeInTheDocument()
  expect(within(table).getAllByRole('row').slice(1).map(item => within(item).getAllByRole('cell')[2].textContent)).toEqual(['9', '—', '—'])
})

it('marks the whole group page time-stale and updates the threshold with refresh frequency', () => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.setSystemTime(new Date(snapshot.collectedAt))
  const { update } = show(snapshot)
  expect(screen.queryByLabelText('Consumer Groups 数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(11000))
  expect(screen.getByLabelText('Consumer Groups 数据已陈旧')).toBeInTheDocument()
  update({}, 10)
  expect(screen.queryByLabelText('Consumer Groups 数据已陈旧')).not.toBeInTheDocument()
})

it('opens the exact group and topic partition details from an overview link', () => {
  show(snapshot, rules, '/?groupId=new-worker&topic=new')
  expect(screen.getByRole('table', { name: 'new-worker / new Partitions' })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: '收起 new-worker / new' })).toHaveAttribute('aria-expanded', 'true')
})

it('keeps the linked detail open when its first snapshot arrives after the route', () => {
  const { update } = show(null, rules, '/?groupId=new-worker&topic=new')
  update({ snapshot })
  expect(screen.getByRole('table', { name: 'new-worker / new Partitions' })).toBeInTheDocument()
})

it('shows known lag plus unknown partitions and expands committed offsets without coercing null', async () => {
  const user = userEvent.setup()
  show()
  const summary = screen.getByRole('row', { name: /new-worker/ })
  expect(within(summary).getByText('50 + 1 未知')).toBeInTheDocument()
  expect(within(summary).getAllByRole('cell').slice(3, 5).map(cell => cell.textContent)).toEqual(['1', '1'])
  await user.click(screen.getByRole('button', { name: '展开 new-worker / new' }))
  const partitions = screen.getByRole('table', { name: 'new-worker / new Partitions' })
  expect(within(within(partitions).getAllByRole('row')[2]).getAllByRole('cell').map(cell => cell.textContent))
    .toEqual(['1', '100', '—', '—', '—'])
  expect(screen.getByRole('button', { name: '收起 new-worker / new' })).toHaveAttribute('aria-expanded', 'true')
})

it('exposes the actual override thresholds on focus and explains all status metrics', async () => {
  const user = userEvent.setup()
  show()
  expect(screen.getAllByText('自定义规则')).toHaveLength(1)
  const custom = screen.getByRole('button', { name: '自定义规则 flink-orders / orders 字段说明' })
  expect(custom).toHaveTextContent('自定义规则')
  await user.hover(custom)
  expect(screen.getByRole('tooltip')).toHaveTextContent(/12,000.*30,000.*120/)
  await user.unhover(custom)
  act(() => custom.focus())
  expect(custom).toHaveAccessibleDescription(/12,000.*30,000.*120/)
  await user.keyboard('{Escape}')
  expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  for (const label of ['状态', '有效分区', '未提交分区', '总 Lag', '趋势']) {
    const help = screen.getByRole('button', { name: `${label} 字段说明` })
    await user.click(help)
    expect(help).toHaveAccessibleDescription()
    await user.keyboard('{Escape}')
  }
})

// Catches timers running on rerender, stale cached snapshots, hidden intervals, or trend direction.
it('uses distinct connected samples for duration and trend, resetting on continuity changes', () => {
  const initial = { ...snapshot, consumerGroups: [row('flink-orders', 'orders', 13000)] }
  const { update } = show(initial)
  const next = (seconds: number, lag: number) => ({ ...initial, collectedAt: new Date(Date.parse(initial.collectedAt) + seconds * 1000).toISOString(), consumerGroups: [row('flink-orders', 'orders', lag)] })
  const dataRow = () => screen.getByRole('row', { name: /flink-orders/ })
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
  update({ snapshot: next(60, 31000) })
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 上升')).toBeInTheDocument()
  update({ snapshot: next(120, 13500) })
  expect(within(dataRow()).getByText('积压')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 下降')).toBeInTheDocument()
  update({ status: 'paused' })
  expect(screen.getByText(/暂停计时/)).toBeInTheDocument()
  update({ status: 'connected', snapshot: next(1000, 13500) })
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
  update({ snapshot: next(1120, 13500) })
  expect(within(dataRow()).getByText('积压')).toBeInTheDocument()
  update({ continuityKey: 'rebuilt' })
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
})

it('flags partial data as collection errors and does not show incomplete totals as complete', () => {
  show({ ...snapshot, consumerGroups: [{ ...row('missing-worker', 'broken', 0), incomplete: true }],
    partialFailures: [{ scope: 'topics', resource: 'broken', code: 'TIMEOUT', message: 'timeout' }] })
  const dataRow = screen.getByRole('row', { name: /missing-worker/ })
  expect(within(dataRow).getByText('采集异常')).toBeInTheDocument()
  expect(within(dataRow).getByText(/0.*不完整/)).toBeInTheDocument()
  expect(screen.getByText(/部分数据采集失败/)).toBeInTheDocument()
})

it('distinguishes a missing snapshot from an empty group list', () => {
  show(null)
  expect(screen.getByText('尚未采集 Consumer Groups 数据。')).toBeInTheDocument()
  expect(screen.queryByText('暂无 Consumer Group / Topic 数据。')).not.toBeInTheDocument()
})

// Catches an HTTP collection failure retaining a misleading normal badge from the last successful snapshot.
it('marks retained rows as collection errors immediately when monitoring fails', () => {
  const { update } = show({ ...snapshot, consumerGroups: [row('healthy-worker', 'healthy', 0)] })
  update({ status: 'error', error: new Error('Collection failed') })
  expect(within(screen.getByRole('row', { name: /healthy-worker/ })).getByText('采集异常')).toBeInTheDocument()
  expect(screen.queryByText('正常', { selector: 'span' })).not.toBeInTheDocument()
})
