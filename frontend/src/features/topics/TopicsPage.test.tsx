import { act, render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router'
import { afterEach, expect, it, vi } from 'vitest'
import type { ClusterSnapshot } from '../../api/types'
import { clusterFixture, snapshotFixture } from '../../test/handlers'
import type { DashboardContext } from '../dashboard/useDashboard'
import { TopicsPage } from './TopicsPage'
afterEach(() => vi.useRealTimers())

const snapshot: ClusterSnapshot = { ...snapshotFixture, topics: [
  { topic: 'order-events', low: 10, latestMessageOffset: 105101, retainedOffsetSpan: 105097, incomplete: false,
    partitions: [
      { partition: 0, low: 10, high: 105102, latestMessageOffset: 105101, retainedOffsetSpan: 105092 },
      { partition: 1, low: 100, high: 105, latestMessageOffset: 104, retainedOffsetSpan: 5 },
      { partition: 2, low: 20, high: 20, latestMessageOffset: null, retainedOffsetSpan: 0 },
    ] },
  { topic: 'audit-log', low: 0, latestMessageOffset: null, retainedOffsetSpan: 0, incomplete: false,
    partitions: [{ partition: 0, low: 0, high: 0, latestMessageOffset: null, retainedOffsetSpan: 0 }] },
] }

function show(value: ClusterSnapshot | null = snapshot) {
  const context: DashboardContext = { cluster: clusterFixture, monitor: {
    snapshot: value, history: [], status: 'connected', error: null, isFetching: false,
    continuityKey: 'test', refresh: async () => {}, reconnect: () => {},
  } }
  render(<MemoryRouter><Routes><Route element={<Outlet context={context} />}>
    <Route index element={<TopicsPage />} />
  </Route></Routes></MemoryRouter>)
}

it('marks the whole Topics page stale after two refresh intervals', () => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.setSystemTime(new Date(snapshot.collectedAt))
  show()
  expect(screen.queryByLabelText('Topics 数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(11000))
  expect(screen.getByLabelText('Topics 数据已陈旧')).toBeInTheDocument()
})

// Catches incorrect aggregation across partition offset spaces and lost search filtering.
it('shows topic overview calculations and filters by topic name', async () => {
  const user = userEvent.setup()
  show()
  const row = screen.getByRole('row', { name: /order-events/ })
  expect(within(row).getAllByRole('cell').slice(1).map(cell => cell.textContent)).toEqual([
    '3', '10', '105,101', '105,097', '完整',
  ])
  expect(screen.getByText(/仅供概览.*Partition.*为准/)).toBeInTheDocument()
  await user.type(screen.getByRole('searchbox', { name: '搜索 Topic' }), 'ORDER')
  expect(screen.queryByText('audit-log')).not.toBeInTheDocument()
  expect(screen.getByText('order-events')).toBeInTheDocument()
  await user.clear(screen.getByRole('searchbox', { name: '搜索 Topic' }))
  await user.type(screen.getByRole('searchbox', { name: '搜索 Topic' }), 'missing')
  expect(screen.getByText('没有匹配的 Topic。')).toBeInTheDocument()
})

// Catches high mistaken for latest offset, empty partitions shown as high - 1, and inaccessible expansion.
it('expands authoritative partition offsets and preserves empty partitions', async () => {
  const user = userEvent.setup()
  show()
  await user.click(screen.getByRole('button', { name: '展开 order-events' }))
  const table = screen.getByRole('table', { name: 'order-events Partitions' })
  const rows = within(table).getAllByRole('row')
  expect(within(rows[1]).getAllByRole('cell').map(cell => cell.textContent)).toEqual(['0', '10', '105,102', '105,101'])
  expect(within(rows[3]).getAllByRole('cell').map(cell => cell.textContent)).toEqual(['2', '20', '20', '—'])
  expect(screen.getByRole('button', { name: '收起 order-events' })).toHaveAttribute('aria-expanded', 'true')
  await user.click(screen.getByRole('button', { name: '收起 order-events' }))
  expect(screen.queryByRole('table', { name: 'order-events Partitions' })).not.toBeInTheDocument()
})

// Catches incomplete totals silently presented as full totals and failed watermarks coerced to zero.
it('marks partial data and shows unavailable offsets as dashes', async () => {
  const user = userEvent.setup()
  show({ ...snapshot, partialFailures: [{ scope: 'watermark', resource: 'order-events:1', code: 'TIMEOUT', message: 'timeout' }],
    topics: [{ ...snapshot.topics[0], incomplete: true, partitions: [snapshot.topics[0].partitions[0],
      { partition: 1, low: null, high: null, latestMessageOffset: null, retainedOffsetSpan: null }] }] })
  const row = screen.getByRole('row', { name: /order-events/ })
  expect(within(row).getAllByRole('cell').slice(2, 5).map(cell => cell.textContent)).toEqual(['—', '—', '—'])
  expect(within(row).getByText(/不完整/)).toBeInTheDocument()
  expect(screen.getByText(/部分数据采集失败/)).toBeInTheDocument()
  await user.click(screen.getByRole('button', { name: '展开 order-events' }))
  const rows = within(screen.getByRole('table', { name: 'order-events Partitions' })).getAllByRole('row')
  expect(within(rows[2]).getAllByRole('cell').map(cell => cell.textContent)).toEqual(['1', '—', '—', '—'])
})

// Catches keyboard/screen reader regressions and missing definitions or formula boundaries.
it('explains fields on keyboard focus and supports hover, click and Escape', async () => {
  const user = userEvent.setup()
  show()
  await user.tab()
  await user.tab()
  const firstHelp = screen.getByRole('button', { name: 'Partitions 字段说明' })
  expect(firstHelp).toHaveFocus()
  expect(firstHelp).toHaveAccessibleDescription(/分区数量/)
  await user.keyboard('{Escape}')
  expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  const maxHelp = screen.getByRole('button', { name: '最大消息 Offset 字段说明' })
  await user.hover(maxHelp)
  expect(screen.getByRole('tooltip')).toHaveTextContent(/high - 1/)
  expect(screen.getByRole('tooltip')).toHaveTextContent(/空分区/)
  await user.unhover(maxHelp)
  expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  await user.click(maxHelp)
  expect(screen.getByRole('tooltip')).toHaveTextContent(/不能跨 Partition/)
  await user.keyboard('{Escape}')
  await user.click(screen.getByRole('button', { name: '展开 order-events' }))
  const highHelp = screen.getByRole('button', { name: 'High watermark 字段说明' })
  await user.click(highHelp)
  expect(highHelp).toHaveAccessibleDescription(/下一条消息将写入的位置/)
  await user.keyboard('{Escape}')
  await user.click(screen.getByRole('button', { name: '保留消息估算 字段说明' }))
  expect(screen.getByRole('tooltip')).toHaveTextContent(/high - low/)
  expect(screen.getByRole('tooltip')).toHaveTextContent(/压缩|删除/)
})

it('distinguishes snapshots not yet collected from an empty topic list', () => {
  show(null)
  expect(screen.getByText('尚未采集 Topic 数据。')).toBeInTheDocument()
  expect(screen.queryByText('暂无 Topic。')).not.toBeInTheDocument()
})

// Catches Escape handlers attached only to the help button while hover leaves focus elsewhere.
it('dismisses hover help with Escape while search keeps keyboard focus', async () => {
  const user = userEvent.setup()
  show()
  const search = screen.getByRole('searchbox', { name: '搜索 Topic' })
  await user.click(search)
  const help = screen.getByRole('button', { name: 'Partitions 字段说明' })
  await user.hover(help)
  expect(search).toHaveFocus()
  expect(screen.getByRole('tooltip')).toHaveTextContent(/分区数量/)
  await user.keyboard('o')
  expect(search).toHaveValue('o')
  expect(screen.getByRole('tooltip')).toBeInTheDocument()
  await user.keyboard('{Escape}')
  expect(search).toHaveFocus()
  expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  await user.tab()
  expect(help).toHaveFocus()
  expect(help).toHaveAccessibleDescription(/分区数量/)
  await user.keyboard('{Escape}')
  expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
  await user.click(help)
  expect(help).toHaveAccessibleDescription(/分区数量/)
  await user.keyboard('{Escape}')
  expect(screen.queryByRole('tooltip')).not.toBeInTheDocument()
})
