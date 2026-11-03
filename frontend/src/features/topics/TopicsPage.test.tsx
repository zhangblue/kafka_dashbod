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
beforeAll(/* 在测试组开始时启动请求拦截，避免单元测试访问真实服务。 */ () => server.listen())
afterEach(/* 在每个用例结束时恢复本用例修改的测试环境，避免拦截或模拟状态影响后续断言。 */ () => { vi.useRealTimers(); server.resetHandlers(); vi.restoreAllMocks() })
afterAll(/* 在测试组结束时关闭请求拦截，释放本文件注册的模拟服务。 */ () => server.close())

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

// 在页面交互测试准备时挂载仪表盘上下文，提供后续快照和会话状态更新入口。
function show(value: ClusterSnapshot | null = snapshot, overrides: Partial<DashboardContext['monitor']> = {}) {
  const context: DashboardContext = { cluster: clusterFixture, monitor: {
    snapshot: value, history: [], status: 'connected', error: null, isFetching: false,
    sessionId: 'session-a', continuityKey: 'test', refresh: /* 在页面交互测试模拟快照刷新完成或失败，验证写入结果与刷新反馈的顺序。 */ async () => {}, reconnect: /* 在页面测试提供重连入口替身，隔离真实 Kafka 会话创建。 */ () => {}, ...overrides,
  } }
  // 在测试挂载和重渲染时组合仪表盘上下文与路由，保留页面状态。
  const content = () => <MemoryRouter><Routes><Route element={<Outlet context={context} />}>
    <Route index element={<TopicsPage />} />
  </Route></Routes></MemoryRouter>
  const view = render(content())
  return { update: /* 在测试模拟采样或连接变化时修改上下文并重渲染，以验证连续性及 UI 更新。 */ (monitor: Partial<DashboardContext['monitor']>) => {
    Object.assign(context.monitor, monitor)
    view.rerender(content())
  } }
}

it('marks the whole Topics page stale after two refresh intervals', /* 在本用例执行时验证两个刷新间隔后整页标为陈旧。 */ () => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.setSystemTime(new Date(snapshot.collectedAt))
  show()
  expect(screen.queryByLabelText('Topics 数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(11000))
  expect(screen.getByLabelText('Topics 数据已陈旧')).toBeInTheDocument()
})

// Catches incorrect aggregation across partition offset spaces and lost search filtering.
it('shows topic overview calculations and filters by topic name', /* 在本用例执行时验证Topic 汇总计算与按名称搜索。 */ async () => {
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
it('expands authoritative partition offsets and preserves empty partitions', /* 在本用例执行时验证展开权威分区 Offset 并保留空分区。 */ async () => {
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
it('marks partial data and shows unavailable offsets as dashes', /* 在本用例执行时验证部分数据标记不完整且未知 Offset 显示破折号。 */ async () => {
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
it('explains fields on keyboard focus and supports hover, click and Escape', /* 在本用例执行时验证指标说明支持键盘、悬停、点击与退出键。 */ async () => {
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

it('distinguishes snapshots not yet collected from an empty topic list', /* 在本用例执行时验证缺少首个快照与空 Topic 列表区别显示。 */ () => {
  show(null)
  expect(screen.getByText('尚未采集 Topic 数据。')).toBeInTheDocument()
  expect(screen.queryByText('暂无 Topic。')).not.toBeInTheDocument()
})

// Catches Escape handlers attached only to the help button while hover leaves focus elsewhere.
it('dismisses hover help with Escape while search keeps keyboard focus', /* 在本用例执行时验证退出键关闭悬停帮助且保留搜索框焦点。 */ async () => {
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
it('opens the real management dialogs and protects internal Topics from deletion', /* 在本用例执行时验证真实管理弹窗可打开且内部 Topic 无删除入口。 */ async () => {
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
it.each(['connecting', 'paused', 'error'] as const)('disables management without a valid session while %s', /* 在本用例执行时验证没有有效会话时禁止管理操作。 */ status => {
  show(snapshot, { sessionId: undefined, status })
  expect(screen.getByRole('button', { name: '新建 Topic' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '配置 order-events' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '删除 order-events' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '展开 order-events' })).toBeEnabled()
})

// Catches routing successful writes around the shared refresh, or closing before it settles.
it('awaits a fresh snapshot before closing successful creation', /* 在本用例执行时验证创建成功等待新快照后关闭。 */ async () => {
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
it.each([false, true])('preserves the fixed unconfirmed notice after config 202 (refresh failure: %s)', /* 在本用例执行时验证配置 202 后保留固定未确认提示，不受刷新失败影响。 */ async failRefresh => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () =>
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
it('keeps a created Topic locked after refresh failure and retains its page notice', /* 在本用例执行时验证创建成功但刷新失败时锁定重复写入并保留页面提示。 */ async () => {
  const user = userEvent.setup()
  let creates = 0
  server.use(http.post('/api/sessions/:id/topics', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => {
    creates++
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  show(snapshot, { refresh: /* 在页面交互测试模拟快照刷新完成或失败，验证写入结果与刷新反馈的顺序。 */ async () => { throw new Error('synthetic refresh failure') } })
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
it('refreshes after deletion and reports metadata propagation until the Topic disappears', /* 在本用例执行时验证删除后刷新，在元数据收敛前提示传播延迟。 */ async () => {
  const user = userEvent.setup()
  let received: unknown
  server.use(http.delete('/api/sessions/session-a/topics/order-events', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ async ({ request }) => {
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
it('retains the configuration dialog on failure without refreshing or reporting success', /* 在本用例执行时验证配置失败保留弹窗且不刷新或误报成功。 */ async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => HttpResponse.json({
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
it('discards an open management target when the live session disappears', /* 在本用例执行时验证有效会话消失立即丢弃管理目标。 */ async () => {
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
it('refreshes the list after confirmed configuration changes', /* 在本用例执行时验证配置确认成功后刷新列表。 */ async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => HttpResponse.json({ configs: [] })))
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
it('restores focus to the toolbar when the deleted Topic row disappears during refresh', /* 在本用例执行时验证删除行刷新消失后焦点回到工具栏。 */ async () => {
  const user = userEvent.setup()
  server.use(http.delete('/api/sessions/:id/topics/:topic', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => new HttpResponse(null, { status: 204 })))
  const view = show(snapshot, { refresh: /* 在页面交互测试模拟快照刷新完成或失败，验证写入结果与刷新反馈的顺序。 */ async () => { view.update({ snapshot: { ...snapshot, topics: [snapshot.topics[1]] } }) } })
  await user.click(screen.getByRole('button', { name: '删除 order-events' }))
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'order-events')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(screen.getByRole('button', { name: '新建 Topic' })).toHaveFocus()
})

// Catches later metadata propagation removing the restored trigger, and focus recovery stealing a user's new focus.
it.each(['trigger', 'search', 'background'] as const)('recovers delayed deletion focus only while it remains on the trigger (%s)', /* 在本用例执行时验证延迟删除焦点只在仍停留触发器时恢复。 */ async focus => {
  const user = userEvent.setup()
  server.use(http.delete('/api/sessions/:id/topics/:topic', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => new HttpResponse(null, { status: 204 })))
  const view = show(snapshot, { refresh: /* 在页面交互测试模拟快照刷新完成或失败，验证写入结果与刷新反馈的顺序。 */ async () => {} })
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
it('bounds the deletion propagation notice without reporting that a successful DELETE failed', /* 在本用例执行时验证删除传播提示有界，不能把已成功删除误报为失败。 */ async () => {
  const user = userEvent.setup()
  let release!: () => void
  const pending = new Promise<void>(resolve => { release = resolve })
  server.use(http.delete('/api/sessions/:id/topics/:topic', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ async () => {
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
it('keeps destructive actions visually distinct', /* 在本用例执行时验证破坏性操作有明确视觉区别。 */ () => {
  show()
  render(<style>{appStyles}</style>)
  const remove = screen.getByRole('button', { name: '删除 order-events' })
  const configure = screen.getByRole('button', { name: '配置 order-events' })
  expect(getComputedStyle(remove).color).not.toBe(getComputedStyle(configure).color)
})

// Catches the expand button wrapping onto a separate line at narrow table widths.
it('keeps Topic names and their expansion on one table line', /* 在本用例执行时验证Topic 名称与展开按钮保持同一表格行。 */ () => {
  show()
  render(<style>{appStyles}</style>)
  const cell = within(screen.getByRole('row', { name: /order-events/ })).getAllByRole('cell')[0]
  expect(getComputedStyle(cell).whiteSpace).toBe('nowrap')
})

// Catches a refresh failure invalidating the live session and erasing an already successful 202 notice.
it('retains the submitted outcome when refreshing disconnects the monitor session', /* 在本用例执行时验证刷新断开监控会话后仍保留已提交结果。 */ async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () =>
    HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })))
  const view = show(snapshot, { refresh: /* 在页面交互测试模拟快照刷新完成或失败，验证写入结果与刷新反馈的顺序。 */ async () => {
    view.update({ sessionId: undefined, status: 'error' })
    throw new Error('synthetic refresh failure')
  } })
  await user.click(screen.getByRole('button', { name: '配置 order-events' }))
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  await waitFor(() => expect(screen.queryByRole('dialog')).not.toBeInTheDocument())
  expect(screen.getByRole('status', { name: 'Topic 操作结果' })).toHaveTextContent('已提交但暂时无法确认当前值')
})
