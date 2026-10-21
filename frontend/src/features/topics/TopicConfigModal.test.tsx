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

beforeAll(() => server.listen())
afterEach(() => { server.resetHandlers(); vi.restoreAllMocks() })
afterAll(() => server.close())

// Catches flex min-content sizing and unbroken topic names making the dialog scroll horizontally.
// jsdom has no layout: verify the actual stylesheet's computed constraints, then check dimensions in a browser.
it.each([80, 249])('keeps a legal %i-character Topic description wrappable inside a shrinkable heading', async length => {
  const topic = 'a'.repeat(length)
  render(<><style>{appStyles}</style><TopicConfigModal sessionId="session-a" topic={topic} onClose={() => {}} onApplied={() => {}} /></>)
  await screen.findByLabelText('消息保留时间')
  const description = screen.getByText(`${topic} · 修改后直接应用到 Kafka，无需重启 Broker`)
  expect(getComputedStyle(description.parentElement!).minWidth).toBe('0px')
  expect(getComputedStyle(description).overflowWrap).toBe('anywhere')
})

// Catches mock defaults, missing sources, and allowing writes to restricted or unavailable keys.
it('loads current Kafka values immediately and preserves read-only and unavailable entries', async () => {
  server.use(http.get('/api/sessions/:id/topics/:topic/configs', async () => {
    await delay(60)
    return HttpResponse.json({ configs: topicConfigsFixture.configs.filter(item => item.key !== 'compression.type') })
  }))
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={() => {}} onApplied={() => {}} />)
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
it('sends only changes with expectedValues, resets via null and rereads actual Kafka values after saving', async () => {
  const user = userEvent.setup()
  let reads = 0
  let received: UpdateTopicConfigsInput | undefined
  server.use(
    http.get('/api/sessions/:id/topics/:topic/configs', () => {
      reads++
      return HttpResponse.json(reads === 1 ? topicConfigsFixture : { configs: topicConfigsFixture.configs.map(item =>
        item.key === 'retention.ms' ? { ...item, value: '7200000', source: 'brokerDefault' } : item) })
    }),
    http.put('/api/sessions/:id/topics/:topic/configs', async ({ request }) => {
      received = await request.json() as UpdateTopicConfigsInput
      await delay(100)
      return HttpResponse.json(topicConfigsFixture)
    }),
  )
  const onApplied = vi.fn()
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={() => {}} onApplied={onApplied} />)
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
it('blocks saving on GET failure and retries a fresh read', async () => {
  const user = userEvent.setup()
  server.use(http.get('/api/sessions/:id/topics/:topic/configs', () => HttpResponse.json({
    code: 'TOPIC_AUTHORIZATION_FAILED', message: 'Kafka authorization failed', requestId: 'request-read',
  }, { status: 403 })))
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={() => {}} onApplied={() => {}} />)
  expect(await screen.findByRole('alert')).toHaveTextContent(/Kafka 账户无权执行此 Topic 操作/)
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  expect(screen.queryByLabelText('消息保留时间')).not.toBeInTheDocument()
  server.use(http.get('/api/sessions/:id/topics/:topic/configs', () => HttpResponse.json(topicConfigsFixture)))
  await user.click(screen.getByRole('button', { name: '重新读取' }))
  expect(await screen.findByLabelText('消息保留时间')).toHaveValue(604800000)
})

// Catches falsely claiming the draft is Kafka's actual state after a 202 or a failed confirmation read.
it.each(['accepted', 'read-failed'])('marks submitted values unconfirmed for %s and disables saving until reread', async outcome => {
  const user = userEvent.setup()
  let reads = 0
  server.use(
    http.get('/api/sessions/:id/topics/:topic/configs', () => {
      reads++
      return reads === 1 ? HttpResponse.json(topicConfigsFixture) : HttpResponse.json({
        code: 'TOPIC_OPERATION_TIMEOUT', message: '读取超时。',
      }, { status: 504 })
    }),
    http.put('/api/sessions/:id/topics/:topic/configs', () => outcome === 'accepted'
      ? HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })
      : HttpResponse.json(topicConfigsFixture)),
  )
  const onApplied = vi.fn()
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={() => {}} onApplied={onApplied} />)
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(await screen.findByText('已提交但暂时无法确认当前值')).toBeVisible()
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  expect(screen.queryByLabelText('清理策略')).not.toBeInTheDocument()
  expect(screen.getByRole('button', { name: '重新读取' })).toBeEnabled()
  await waitFor(() => expect(onApplied).toHaveBeenCalledWith({ status: 'appliedButUnconfirmed' }))
})

// Catches losing the 202 uncertainty notice when refreshing the page also fails.
it('preserves the unconfirmed status and reports a refresh rejection separately after a 202', async () => {
  const user = userEvent.setup()
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', () =>
    HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })))
  const onApplied = vi.fn(async () => { throw new Error('synthetic refresh failure') })
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={() => {}} onApplied={onApplied} />)
  await user.selectOptions(await screen.findByLabelText('清理策略'), 'compact')
  await user.click(screen.getByRole('button', { name: '保存配置' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('列表刷新失败，请稍后手动刷新。')
  expect(screen.getByRole('status')).toHaveTextContent('已提交但暂时无法确认当前值')
  expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  expect(screen.getByRole('button', { name: '重新读取' })).toBeEnabled()
  expect(onApplied).toHaveBeenCalledWith({ status: 'appliedButUnconfirmed' })
})

// Catches invalid updates, lost edits or silent closure after an API conflict.
it('validates edited values, keeps a failed save and confirms discarding unsaved edits', async () => {
  const user = userEvent.setup()
  const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
  const onClose = vi.fn()
  let updates = 0
  server.use(http.put('/api/sessions/:id/topics/:topic/configs', () => {
    updates++
    return HttpResponse.json({ code: 'TOPIC_CONFIG_CHANGED', message: '配置已被其他客户端修改，请重新读取。',
      fields: { 'changes.segment.ms': '当前值已改变。' }, requestId: 'request-save' }, { status: 409 })
  }))
  render(<TopicConfigModal sessionId="session-a" topic="orders" onClose={onClose} onApplied={() => {}} />)
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
