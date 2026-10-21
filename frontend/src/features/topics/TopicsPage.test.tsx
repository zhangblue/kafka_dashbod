/// <reference types="node" />
import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router'
import { afterAll, afterEach, beforeAll, expect, it, vi } from 'vitest'
import type { ClusterSnapshot } from '../../api/types'
import { clusterFixture, server, snapshotFixture } from '../../test/handlers'
import type { DashboardContext } from '../dashboard/useDashboard'
import { TopicsPage } from './TopicsPage'
import { readFileSync } from 'node:fs'
const appStyles = readFileSync('src/index.css', 'utf8')
beforeAll(() => server.listen())
afterEach(() => { vi.useRealTimers(); server.resetHandlers(); vi.restoreAllMocks() })
afterAll(() => server.close())

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

function show(value: ClusterSnapshot | null = snapshot, overrides: Partial<DashboardContext['monitor']> = {}) {
  const context: DashboardContext = { cluster: clusterFixture, monitor: {
    snapshot: value, history: [], status: 'connected', error: null, isFetching: false,
    sessionId: 'session-a', continuityKey: 'test', refresh: async () => {}, reconnect: () => {}, ...overrides,
  } }
  const content = () => <MemoryRouter><Routes><Route element={<Outlet context={context} />}>
    <Route index element={<TopicsPage />} />
  </Route></Routes></MemoryRouter>
  const view = render(content())
  return { update: (monitor: Partial<DashboardContext['monitor']>) => {
    Object.assign(context.monitor, monitor)
    view.rerender(content())
  } }
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
  expect(within(row).getAllByRole('cell').slice(1, 6).map(cell => cell.textContent)).toEqual([
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
  expect(within(rows[1]).getAllByRole('cell').map(cell => cell.textContent)).toEqual(['0', '10', '105,102', '105,101', '105,092'])
  expect(within(rows[3]).getAllByRole('cell').map(cell => cell.textContent)).toEqual(['2', '20', '20', '—', '0'])
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
  expect(within(rows[2]).getAllByRole('cell').map(cell => cell.textContent)).toEqual(['1', '—', '—', '—', '—'])
})

// Catches keyboard/screen reader regressions and missing definitions or formula boundaries.
it('explains fields on keyboard focus and supports hover, click and Escape', async () => {
  const user = userEvent.setup()
  show()
  await user.tab()
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

// Catches missing management entry points and accidentally exposing internal Topic deletion.
it('opens the real management dialogs and protects internal Topics from deletion', async () => {
  const user = userEvent.setup()
  show({ ...snapshot, topics: [...snapshot.topics, { ...snapshot.topics[1], topic: '__consumer_offsets' }] })
  const create = screen.getByRole('button', { name: '新建 Topic' })
  await user.click(create)
  expect(screen.getByRole('dialog', { name: '新建 Topic' })).toBeVisible()
  await user.click(screen.getByRole('button', { name: '取消' }))
  expect(create).toHaveFocus()
  await user.click(screen.getByRole('button', { name: '配置 order-events' }))
  expect(await screen.findByLabelText('消息保留时间')).toHaveValue(604800000)
  await user.click(screen.getByRole('button', { name: '取消' }))
  expect(screen.getByRole('button', { name: '配置 order-events' })).toHaveFocus()
  await user.click(screen.getByRole('button', { name: '删除 order-events' }))
  expect(screen.getByRole('dialog', { name: '删除 Topic' })).toBeVisible()
  await user.click(screen.getByRole('button', { name: '取消' }))
  expect(screen.getByRole('button', { name: '删除 order-events' })).toHaveFocus()
  expect(screen.getByRole('button', { name: '配置 __consumer_offsets' })).toBeEnabled()
  expect(screen.queryByRole('button', { name: '删除 __consumer_offsets' })).not.toBeInTheDocument()
})

// Catches a retained display snapshot being mistaken for an authorized live session.
it.each(['connecting', 'paused', 'error'] as const)('disables management without a valid session while %s', status => {
  show(snapshot, { sessionId: undefined, status })
  expect(screen.getByRole('button', { name: '新建 Topic' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '配置 order-events' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '删除 order-events' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '展开 order-events' })).toBeEnabled()
})

// Catches routing successful writes around the shared refresh, or closing before it settles.
it('awaits a fresh snapshot before closing successful creation', async () => {
  const user = userEvent.setup()
  let release!: () => void
  const refresh = vi.fn(() => new Promise<void>(resolve => { release = resolve }))
  show(snapshot, { refresh })
  await user.click(screen.getByRole('button', { name: '新建 Topic' }))
  await user.type(screen.getByLabelText('Topic 名称'), 'created-orders')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(screen.getByRole('dialog', { name: '新建 Topic' })).toBeVisible()
  expect(screen.getByRole('button', { name: 'Topic 已创建' })).toBeDisabled()
  await act(async () => release())
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('Topic 已创建')
})

// Catches falsely claiming a 202 is a confirmed save, especially when the list refresh rejects.
it.each([false, true])('preserves the fixed unconfirmed notice after config 202 (refresh failure: %s)', async failRefresh => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', () =>
    HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })))
  const refresh = vi.fn(async () => { if (failRefresh) throw new Error('synthetic refresh failure') })
  show(snapshot, { refresh })
  await user.click(screen.getByRole('button', { name: '配置 order-events' }))
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  if (failRefresh) expect(await screen.findByRole('alert')).toHaveTextContent('列表刷新失败')
  await user.click(screen.getByRole('button', { name: '取消' }))
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('已提交但暂时无法确认当前值')
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).not.toHaveTextContent('配置已保存')
})

// Catches a failed list refresh re-enabling an already committed create request.
it('keeps a created Topic locked after refresh failure and retains its page notice', async () => {
  const user = userEvent.setup()
  let creates = 0
  server.use(http.post('/api/sessions/:id/topics', () => {
    creates++
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  show(snapshot, { refresh: async () => { throw new Error('synthetic refresh failure') } })
  await user.click(screen.getByRole('button', { name: '新建 Topic' }))
  await user.type(screen.getByLabelText('Topic 名称'), 'created-orders')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  expect(await screen.findByText(/Topic 已创建，但列表刷新失败/)).toBeVisible()
  await user.click(screen.getByRole('button', { name: 'Topic 已创建' }))
  expect(creates).toBe(1)
  await user.click(screen.getByRole('button', { name: '关闭' }))
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('Topic 已创建')
})

// Catches deleting without the typed confirmation, missing refresh and misleading metadata propagation results.
it('refreshes after deletion and reports metadata propagation until the Topic disappears', async () => {
  const user = userEvent.setup()
  let received: unknown
  server.use(http.delete('/api/sessions/session-a/topics/order-events', async ({ request }) => {
    received = await request.json()
    return new HttpResponse(null, { status: 204 })
  }))
  const refresh = vi.fn(async () => {})
  const view = show(snapshot, { refresh })
  await user.click(screen.getByRole('button', { name: '删除 order-events' }))
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'order-events')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(received).toEqual({ confirmationName: 'order-events' })
  expect(refresh).toHaveBeenCalledOnce()
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('删除请求已成功，正在等待集群状态更新')
  view.update({ snapshot: { ...snapshot, topics: [snapshot.topics[1]] } })
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('Topic 已删除')
})

// Catches failed configuration writes being treated as successful page actions.
it('retains the configuration dialog on failure without refreshing or reporting success', async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', () => HttpResponse.json({
    code: 'TOPIC_AUTHORIZATION_FAILED', message: 'Forbidden',
  }, { status: 403 })))
  const refresh = vi.fn()
  show(snapshot, { refresh })
  await user.click(screen.getByRole('button', { name: '配置 order-events' }))
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Kafka 账户无权')
  expect(screen.getByRole('dialog', { name: 'Topic 参数配置' })).toBeVisible()
  expect(screen.getByLabelText('清理策略')).toHaveValue('compact')
  expect(refresh).not.toHaveBeenCalled()
  expect(screen.queryByRole('status', { name: 'Topic 操作结果' })).not.toBeInTheDocument()
})

// Catches restoring a stale editor after the session is hidden, replaced or expired.
it('discards an open management target when the live session disappears', async () => {
  const user = userEvent.setup()
  const view = show()
  await user.click(screen.getByRole('button', { name: '配置 order-events' }))
  await screen.findByLabelText('清理策略')
  view.update({ sessionId: undefined, status: 'paused' })
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
  view.update({ sessionId: 'session-a', status: 'connected' })
  expect(screen.queryByRole('dialog')).not.toBeInTheDocument()
})

// Catches successful configuration writes not refreshing the shared monitor.
it('refreshes the list after confirmed configuration changes', async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', () => HttpResponse.json({ configs: [] })))
  const refresh = vi.fn(async () => {})
  show(snapshot, { refresh })
  await user.click(screen.getByRole('button', { name: '配置 order-events' }))
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  await waitFor(() => expect(refresh).toHaveBeenCalledOnce())
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('配置已保存')
  expect(screen.getByRole('dialog')).toBeVisible()
})

// Catches losing keyboard focus when a successful refresh removes the dialog's original trigger.
it('restores focus to the toolbar when the deleted Topic row disappears during refresh', async () => {
  const user = userEvent.setup()
  server.use(http.delete('/api/sessions/:id/topics/:topic', () => new HttpResponse(null, { status: 204 })))
  const view = show(snapshot, { refresh: async () => { view.update({ snapshot: { ...snapshot, topics: [snapshot.topics[1]] } }) } })
  await user.click(screen.getByRole('button', { name: '删除 order-events' }))
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'order-events')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(screen.getByRole('button', { name: '新建 Topic' })).toHaveFocus()
})

// Catches later metadata propagation removing the restored trigger, and focus recovery stealing a user's new focus.
it.each(['trigger', 'search', 'background'] as const)('recovers delayed deletion focus only while it remains on the trigger (%s)', async focus => {
  const user = userEvent.setup()
  server.use(http.delete('/api/sessions/:id/topics/:topic', () => new HttpResponse(null, { status: 204 })))
  const view = show(snapshot, { refresh: async () => {} })
  const trigger = screen.getByRole('button', { name: '删除 order-events' })
  await user.click(trigger)
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'order-events')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(trigger).toHaveFocus()
  const search = screen.getByRole('searchbox', { name: '搜索 Topic' })
  if (focus === 'search') await user.click(search)
  if (focus === 'background') await user.click(screen.getByRole('heading', { name: 'Topics' }))
  view.update({ snapshot: { ...snapshot, topics: [snapshot.topics[1]] } })
  expect(trigger).not.toBeInTheDocument()
  expect(focus === 'trigger' ? screen.getByRole('button', { name: '新建 Topic' })
    : focus === 'search' ? search : document.body).toHaveFocus()
})

// Catches indefinitely promising that metadata propagation is still in progress.
it('bounds the deletion propagation notice without reporting that a successful DELETE failed', async () => {
  const user = userEvent.setup()
  let release!: () => void
  const pending = new Promise<void>(resolve => { release = resolve })
  server.use(http.delete('/api/sessions/:id/topics/:topic', async () => {
    await pending
    return new HttpResponse(null, { status: 204 })
  }))
  show()
  await user.click(screen.getByRole('button', { name: '删除 order-events' }))
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'order-events')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] })
  await act(async () => { release(); await vi.advanceTimersByTimeAsync(1) })
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('正在等待集群状态更新')
  await act(async () => { await vi.advanceTimersByTimeAsync(30000) })
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('删除请求已成功，请稍后刷新确认集群状态')
  expect(screen.queryByRole('alert')).not.toBeInTheDocument()
})

// Catches later generic action styles overriding danger color.
it('keeps destructive actions visually distinct', () => {
  show()
  render(<style>{appStyles}</style>)
  const remove = screen.getByRole('button', { name: '删除 order-events' })
  const configure = screen.getByRole('button', { name: '配置 order-events' })
  expect(getComputedStyle(remove).color).not.toBe(getComputedStyle(configure).color)
})

// Catches the expand button wrapping onto a separate line at narrow table widths.
it('keeps Topic names and their expansion on one table line', () => {
  show()
  render(<style>{appStyles}</style>)
  const cell = within(screen.getByRole('row', { name: /order-events/ })).getAllByRole('cell')[0]
  expect(getComputedStyle(cell).whiteSpace).toBe('nowrap')
})

// Catches a refresh failure invalidating the live session and erasing an already successful 202 notice.
it('retains the submitted outcome when refreshing disconnects the monitor session', async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', () =>
    HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })))
  const view = show(snapshot, { refresh: async () => {
    view.update({ sessionId: undefined, status: 'error' })
    throw new Error('synthetic refresh failure')
  } })
  await user.click(screen.getByRole('button', { name: '配置 order-events' }))
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('已提交但暂时无法确认当前值')
})
