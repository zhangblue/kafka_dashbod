import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { delay, http, HttpResponse } from 'msw'
import { afterAll, afterEach, beforeAll, expect, it, vi } from 'vitest'
import type { CreateTopicInput } from '../../api/types'
import { server } from '../../test/handlers'
import { CreateTopicModal } from './CreateTopicModal'

beforeAll(/* 在测试组开始时启动请求拦截，避免单元测试访问真实服务。 */ () => server.listen())
afterEach(/* 在每个用例结束时恢复本用例修改的测试环境，避免拦截或模拟状态影响后续断言。 */ () => { server.resetHandlers(); vi.restoreAllMocks() })
afterAll(/* 在测试组结束时关闭拦截并恢复全局配置。 */ () => server.close())

// Catches sending demo config values, a fixed replica factor, or duplicate create requests.
it('creates with one partition, cluster-default replicas and no unset configs while locking submission', /* 在本用例执行时验证默认单分区、默认副本且不发送未填参数，提交中锁定表单。 */ async () => {
  const user = userEvent.setup()
  const requests: CreateTopicInput[] = []
  server.use(http.post('/api/sessions/session-a/topics', /* 在本用例请求拦截阶段提供受控响应或记录请求，隔离真实外部发送并验证精确请求边界。 */ async ({ request }) => {
    requests.push(await request.json() as CreateTopicInput)
    await delay(150)
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  const onApplied = vi.fn()
  const onClose = vi.fn()
  render(<CreateTopicModal sessionId="session-a" onClose={onClose} onApplied={onApplied} />)
  expect(screen.getByLabelText('分区数')).toHaveValue(1)
  expect(screen.getByLabelText('副本因子')).toHaveValue(null)
  expect(screen.getByLabelText('消息保留时间')).toHaveValue(null)
  expect(screen.getByLabelText('清理策略')).toHaveValue('')
  expect(screen.getByText(/每个 Partition.*并非 Topic 总量/)).toBeVisible()
  expect(screen.getByText(/缩短保留时间可能/)).toBeVisible()
  await user.type(screen.getByLabelText('Topic 名称'), 'orders.v2')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  expect(screen.getByRole('button', { name: '创建中…' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '关闭弹窗' })).toBeDisabled()
  await user.click(screen.getByRole('button', { name: '创建中…' }))
  await waitFor(() => expect(onApplied).toHaveBeenCalledOnce())
  await waitFor(() => expect(onClose).toHaveBeenCalledOnce())
  expect(requests).toEqual([{ name: 'orders.v2', partitions: 1, replicationFactor: null }])
})

// Catches treating a failed list refresh as a failed POST and reopening a duplicate creation path.
it('keeps a successful creation locked and offers direct close when the list refresh rejects', /* 在本用例执行时验证创建成功后即使刷新失败也锁定写入并允许直接关闭。 */ async () => {
  const user = userEvent.setup()
  let creates = 0
  server.use(http.post('/api/sessions/:id/topics', /* 在本用例请求拦截阶段提供受控响应或记录请求，隔离真实外部发送并验证精确请求边界。 */ () => {
    creates++
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  const onApplied = vi.fn(async () => { throw new Error('synthetic refresh failure') })
  const onClose = vi.fn()
  const confirm = vi.spyOn(window, 'confirm')
  render(<CreateTopicModal sessionId="session-a" onClose={onClose} onApplied={onApplied} />)
  await user.type(screen.getByLabelText('Topic 名称'), 'created-orders')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  expect(await screen.findByRole('status')).toHaveTextContent('Topic 已创建，但列表刷新失败。请关闭弹窗后手动刷新。')
  expect(onApplied).toHaveBeenCalledWith({ status: 'created' })
  const created = screen.getByRole('button', { name: 'Topic 已创建' })
  expect(created).toBeDisabled()
  expect(screen.getByLabelText('Topic 名称')).toBeDisabled()
  await user.click(created)
  expect(creates).toBe(1)
  expect(onClose).not.toHaveBeenCalled()
  await user.click(screen.getByRole('button', { name: '关闭' }))
  expect(confirm).not.toHaveBeenCalled()
  expect(onClose).toHaveBeenCalledOnce()
})

// Catches dropping optional values, missing front-end validation, or losing the draft on API errors.
it('validates inputs and retains optional values and API field errors after failure', /* 在本用例执行时验证非法输入被拒绝且请求失败后保留可选值和字段错误。 */ async () => {
  const user = userEvent.setup()
  let received: CreateTopicInput | undefined
  server.use(http.post('/api/sessions/:id/topics', /* 在本用例请求拦截阶段提供受控响应或记录请求，隔离真实外部发送并验证精确请求边界。 */ async ({ request }) => {
    received = await request.json() as CreateTopicInput
    return HttpResponse.json({ code: 'TOPIC_ALREADY_EXISTS', message: '同名 Topic 已存在。',
      fields: { name: '请使用其他 Topic 名称。' }, requestId: 'request-create' }, { status: 409 })
  }))
  render(<CreateTopicModal sessionId="session-a" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
  await user.type(screen.getByLabelText('Topic 名称'), '..')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  expect(screen.getByLabelText('Topic 名称')).toHaveAttribute('aria-invalid', 'true')
  expect(received).toBeUndefined()
  await user.clear(screen.getByLabelText('Topic 名称'))
  await user.type(screen.getByLabelText('Topic 名称'), 'orders')
  await user.clear(screen.getByLabelText('分区数'))
  await user.type(screen.getByLabelText('分区数'), '3')
  await user.type(screen.getByLabelText('副本因子'), '2')
  await user.type(screen.getByLabelText('消息保留时间'), '3600000')
  await user.selectOptions(screen.getByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  expect(await screen.findByText('同名 Topic 已存在。')).toBeVisible()
  expect(screen.getByLabelText('Topic 名称')).toHaveAccessibleDescription(/请使用其他 Topic 名称/)
  expect(screen.getByText(/request-create/)).toBeVisible()
  expect(screen.getByLabelText('消息保留时间')).toHaveValue(3600000)
  expect(received).toEqual({ name: 'orders', partitions: 3, replicationFactor: 2,
    configs: { 'retention.ms': '3600000', 'cleanup.policy': 'compact' } })
})

// Catches accepting counts that the native create API cannot represent.
it.each([
  ['分区数', '100001'],
  ['副本因子', '10001'],
])('rejects %s above the native limit before sending a request', /* 在本用例执行时验证超过原生数量上限时不发送创建请求。 */ async (label, value) => {
  const user = userEvent.setup()
  const requests: CreateTopicInput[] = []
  server.use(http.post('/api/sessions/:id/topics', /* 在本用例请求拦截阶段提供受控响应或记录请求，隔离真实外部发送并验证精确请求边界。 */ async ({ request }) => {
    requests.push(await request.json() as CreateTopicInput)
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  render(<CreateTopicModal sessionId="session-a" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
  await user.type(screen.getByLabelText('Topic 名称'), 'orders')
  await user.clear(screen.getByLabelText(label))
  await user.type(screen.getByLabelText(label), value)
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  expect(screen.getByLabelText(label)).toHaveAttribute('aria-invalid', 'true')
  expect(screen.getByLabelText(label)).toHaveAccessibleDescription(label === '分区数'
    ? '分区数必须为 1–100000 的整数。'
    : '副本因子必须为 1–10000 的整数，或留空使用集群默认值。')
  expect(requests).toEqual([])
})

// Catches an off-by-one upper-bound check rejecting supported creation values.
it('submits creation counts at the native upper boundaries', /* 在本用例执行时验证原生数量上限边界可以提交。 */ async () => {
  const user = userEvent.setup()
  const requests: CreateTopicInput[] = []
  server.use(http.post('/api/sessions/:id/topics', /* 在本用例请求拦截阶段提供受控响应或记录请求，隔离真实外部发送并验证精确请求边界。 */ async ({ request }) => {
    requests.push(await request.json() as CreateTopicInput)
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  render(<CreateTopicModal sessionId="session-a" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
  await user.type(screen.getByLabelText('Topic 名称'), 'orders')
  await user.clear(screen.getByLabelText('分区数'))
  await user.type(screen.getByLabelText('分区数'), '100000')
  await user.type(screen.getByLabelText('副本因子'), '10000')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  await waitFor(() => expect(requests).toEqual([
    { name: 'orders', partitions: 100000, replicationFactor: 10000 },
  ]))
})

// Catches silently discarding a partly completed form.
it('confirms before discarding a dirty form and closes a pristine form directly', /* 在本用例执行时验证有修改时确认放弃，空白表单直接关闭。 */ async () => {
  const user = userEvent.setup()
  const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
  const onClose = vi.fn()
  render(<CreateTopicModal sessionId="session-a" onClose={onClose} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
  await user.click(screen.getByRole('button', { name: '取消' }))
  expect(onClose).toHaveBeenCalledOnce()
  onClose.mockClear()
  await user.type(screen.getByLabelText('Topic 名称'), 'unfinished')
  await user.click(screen.getByRole('button', { name: '关闭弹窗' }))
  expect(onClose).not.toHaveBeenCalled()
  confirm.mockReturnValue(true)
  await user.click(screen.getByRole('button', { name: '取消' }))
  expect(onClose).toHaveBeenCalledOnce()
})

// Catches a completed request refreshing or closing a later session after this modal was removed.
it('does not apply a completed creation to a page that has already left the session', /* 在本用例执行时验证离开会话后忽略晚到的创建结果。 */ async () => {
  const user = userEvent.setup()
  let release!: () => void
  const pending = new Promise<void>(resolve => { release = resolve })
  let finished = false
  server.use(http.post('/api/sessions/:id/topics', /* 在本用例请求拦截阶段提供受控响应或记录请求，隔离真实外部发送并验证精确请求边界。 */ async () => {
    await pending
    finished = true
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  const onApplied = vi.fn()
  const onClose = vi.fn()
  const view = render(<CreateTopicModal sessionId="session-a" onClose={onClose} onApplied={onApplied} />)
  await user.type(screen.getByLabelText('Topic 名称'), 'orders')
  await user.click(screen.getByRole('button', { name: '创建 Topic' }))
  view.unmount()
  release()
  await waitFor(() => expect(finished).toBe(true))
  await new Promise(resolve => setTimeout(resolve, 30))
  expect(onApplied).not.toHaveBeenCalled()
  expect(onClose).not.toHaveBeenCalled()
})
