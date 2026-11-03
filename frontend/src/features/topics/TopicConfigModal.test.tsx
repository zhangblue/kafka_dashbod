/// <reference types="node" />
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { delay, http, HttpResponse } from 'msw'
import { afterAll, afterEach, beforeAll, expect, it, vi } from 'vitest'
import type { UpdateTopicConfigsInput } from '../../api/types'
import { server, topicConfigsFixture } from '../../test/handlers'
import { TopicConfigModal } from './TopicConfigModal'
import { readFileSync } from 'node:fs'

const appStyles = readFileSync('src/index.css', 'utf8')

beforeAll(/* 在测试组开始时启动请求拦截，避免单元测试访问真实服务。 */ () => server.listen())
afterEach(/* 在每个用例结束时恢复本用例修改的测试环境，避免拦截或模拟状态影响后续断言。 */ () => { server.resetHandlers(); vi.restoreAllMocks() })
afterAll(/* 在测试组结束时关闭请求拦截，释放本文件注册的模拟服务。 */ () => server.close())

// Catches flex min-content sizing and unbroken topic names making the dialog scroll horizontally.
// jsdom has no layout: verify the actual stylesheet's computed constraints, then check dimensions in a browser.
it.each([80, 249])('keeps a legal %i-character Topic description wrappable inside a shrinkable heading', /* 在本用例执行时验证合法长 Topic 名称在可收缩标题中正常换行。 */ async length => {
  const topic = 'a'.repeat(length)
  render(<><style>{appStyles}</style><TopicConfigModal sessionId="session-a" topic={topic} onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} /></>)
  await screen.findByLabelText('消息保留时间')
  const description = screen.getByText(`${topic} · 修改后直接应用到 Kafka，无需重启 Broker`)
  expect(getComputedStyle(description.parentElement!).minWidth).toBe('0px')
  expect(getComputedStyle(description).overflowWrap).toBe('anywhere')
})

// Catches mock defaults, missing sources, and allowing writes to restricted or unavailable keys.
it('loads current Kafka values immediately and preserves read-only and unavailable entries', /* 在本用例执行时验证打开立即读当前值并保留只读和不可用状态。 */ async () => {
  server.use(http.get('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ async () => {
    await delay(60)
    return HttpResponse.json({ configs: topicConfigsFixture.configs.filter(item => item.key !== 'compression.type') })
  }))
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
  expect(screen.getByRole('status')).toHaveTextContent(/正在读取/)
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  const retention = await screen.findByLabelText('消息保留时间')
  expect(retention).toHaveValue(604800000)
  expect(within(screen.getByRole('article', { name: '消息保留时间 配置项' })).getByText('Topic 覆盖值')).toBeVisible()
  expect(within(screen.getByRole('article', { name: '分区保留空间 配置项' })).getByText('Broker 默认值')).toBeVisible()
  expect(screen.getByLabelText('最少同步副本数')).toHaveValue(2)
  expect(screen.getByLabelText('最少同步副本数')).toBeDisabled()
  expect(screen.getByLabelText('压缩方式')).toBeDisabled()
  expect(screen.getByLabelText('压缩方式')).toHaveValue('')
  expect(screen.getByRole('button', { name: '恢复 最少同步副本数 为 Broker 默认值' })).toBeDisabled()
})

// Catches whole-form updates, missing optimistic concurrency values, and trusting submitted or PUT values as actual.
it('sends only changes with expectedValues, resets via null and rereads actual Kafka values after saving', /* 在本用例执行时验证只发送变更及比较基线，用空值恢复默认并在保存后重读 Kafka。 */ async () => {
  const user = userEvent.setup()
  let reads = 0
  let received: UpdateTopicConfigsInput | undefined
  server.use(
    http.get('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => {
      reads++
      return HttpResponse.json(reads === 1 ? topicConfigsFixture : { configs: topicConfigsFixture.configs.map(item =>
        item.key === 'retention.ms' ? { ...item, value: '7200000', source: 'brokerDefault' } : item) })
    }),
    http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ async ({ request }) => {
      received = await request.json() as UpdateTopicConfigsInput
      await delay(100)
      return HttpResponse.json(topicConfigsFixture)
    }),
  )
  const onApplied = vi.fn()
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={onApplied} />)
  await screen.findByLabelText('消息保留时间')
  await user.click(screen.getByRole('button', { name: '恢复 消息保留时间 为 Broker 默认值' }))
  expect(screen.getByText(/保存后继承 Broker 默认值/)).toBeVisible()
  await user.selectOptions(screen.getByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(screen.getByRole('button', { name: '保存中…' })).toBeDisabled()
  expect(screen.getByLabelText('清理策略')).toBeDisabled()
  await waitFor(() => expect(onApplied).toHaveBeenCalledOnce())
  expect(received).toEqual({ changes: { 'retention.ms': null, 'cleanup.policy': 'compact' },
    expectedValues: { 'retention.ms': '604800000', 'cleanup.policy': 'delete' } })
  expect(reads).toBe(2)
  expect(screen.getByLabelText('消息保留时间')).toHaveValue(7200000)
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
})

// Catches using cached/demo values after a failed GET or offering save before a successful retry.
it('blocks saving on GET failure and retries a fresh read', /* 在本用例执行时验证读取失败禁止保存且可重试新读取。 */ async () => {
  const user = userEvent.setup()
  server.use(http.get('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => HttpResponse.json({
    code: 'TOPIC_AUTHORIZATION_FAILED', message: 'Kafka authorization failed', requestId: 'request-read',
  }, { status: 403 })))
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
  expect(await screen.findByRole('alert')).toHaveTextContent(/Kafka 账户无权执行此 Topic 操作/)
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  expect(screen.queryByLabelText('消息保留时间')).not.toBeInTheDocument()
  server.use(http.get('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => HttpResponse.json(topicConfigsFixture)))
  await user.click(screen.getByRole('button', { name: '重新读取' }))
  expect(await screen.findByLabelText('消息保留时间')).toHaveValue(604800000)
})

// Catches falsely claiming the draft is Kafka's actual state after a 202 or a failed confirmation read.
it.each(['accepted', 'read-failed'])('marks submitted values unconfirmed for %s and disables saving until reread', /* 在本用例执行时验证无法确认结果时保留未确认状态，重读前禁止再保存。 */ async outcome => {
  const user = userEvent.setup()
  let reads = 0
  server.use(
    http.get('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => {
      reads++
      return reads === 1 ? HttpResponse.json(topicConfigsFixture) : HttpResponse.json({
        code: 'TOPIC_OPERATION_TIMEOUT', message: '读取超时。',
      }, { status: 504 })
    }),
    http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => outcome === 'accepted'
      ? HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })
      : HttpResponse.json(topicConfigsFixture)),
  )
  const onApplied = vi.fn()
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={onApplied} />)
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(await screen.findByText('已提交但暂时无法确认当前值')).toBeVisible()
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  expect(screen.queryByLabelText('清理策略')).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: '重新读取' })).toBeEnabled()
  await waitFor(() => expect(onApplied).toHaveBeenCalledWith({ status: 'appliedButUnconfirmed' }))
})

// Catches losing the 202 uncertainty notice when refreshing the page also fails.
it('preserves the unconfirmed status and reports a refresh rejection separately after a 202', /* 在本用例执行时验证202 已提交结果保留，列表刷新失败独立提示。 */ async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () =>
    HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })))
  const onApplied = vi.fn(async () => { throw new Error('synthetic refresh failure') })
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={/* 在关闭弹窗时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} onApplied={onApplied} />)
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('列表刷新失败，请稍后手动刷新。')
  expect(screen.getByRole('status')).toHaveTextContent('已提交但暂时无法确认当前值')
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '重新读取' })).toBeEnabled()
  expect(onApplied).toHaveBeenCalledWith({ status: 'appliedButUnconfirmed' })
})

// Catches invalid updates, lost edits or silent closure after an API conflict.
it('validates edited values, keeps a failed save and confirms discarding unsaved edits', /* 在本用例执行时验证编辑值校验、失败草稿保留与放弃修改确认。 */ async () => {
  const user = userEvent.setup()
  const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
  const onClose = vi.fn()
  let updates = 0
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', /* 在本用例请求被拦截时执行场景设定的响应逻辑，使交互验证不依赖真实后端。 */ () => {
    updates++
    return HttpResponse.json({ code: 'TOPIC_CONFIG_CHANGED', message: '配置已被其他客户端修改，请重新读取。',
      fields: { 'changes.segment.ms': '当前值已改变。' }, requestId: 'request-save' }, { status: 409 })
  }))
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={onClose} onApplied={/* 在操作已应用时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
  const segment = await screen.findByLabelText('日志段最大时间')
  await user.clear(segment)
  await user.type(segment, '0')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(updates).toBe(0)
  expect(segment).toHaveAttribute('aria-invalid', 'true')
  await user.clear(segment)
  await user.type(segment, '1000')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(await screen.findByText(/配置已被其他客户端修改/)).toBeVisible()
  expect(screen.getByLabelText('日志段最大时间')).toHaveValue(1000)
  expect(segment).toHaveAccessibleDescription(/当前值已改变/)
  await user.click(screen.getByRole('button', { name: '取消' }))
  expect(onClose).not.toHaveBeenCalled()
  confirm.mockReturnValue(true)
  await user.click(screen.getByRole('button', { name: '关闭弹窗' }))
  expect(onClose).toHaveBeenCalledOnce()
})
