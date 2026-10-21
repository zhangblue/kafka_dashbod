import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { delay, http, HttpResponse } from 'msw'
import { afterAll, afterEach, beforeAll, expect, it, vi } from 'vitest'
import type { CreateTopicInput } from '../../api/types'
import { server } from '../../test/handlers'
import { CreateTopicModal } from './CreateTopicModal'

beforeAll(() => server.listen())
afterEach(() => { server.resetHandlers(); vi.restoreAllMocks() })
afterAll(() => server.close())

// Catches sending demo config values, a fixed replica factor, or duplicate create requests.
it('creates with one partition, cluster-default replicas and no unset configs while locking submission', async () => {
  const user = userEvent.setup()
  const requests: CreateTopicInput[] = []
  server.use(http.post('/api/sessions/session-a/topics', async ({ request }) => {
    requests.push(await request.json() as CreateTopicInput)
    await delay(150)
    return HttpResponse.json({ status: 'created' }, { status: 201 })
  }))
  const onApplied = vi.fn()
  render(<CreateTopicModal sessionId="session-a" onClose={() => {}} onApplied={onApplied} />)
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
  expect(requests).toEqual([{ name: 'orders.v2', partitions: 1, replicationFactor: null }])
})

// Catches dropping optional values, missing front-end validation, or losing the draft on API errors.
it('validates inputs and retains optional values and API field errors after failure', async () => {
  const user = userEvent.setup()
  let received: CreateTopicInput | undefined
  server.use(http.post('/api/sessions/:id/topics', async ({ request }) => {
    received = await request.json() as CreateTopicInput
    return HttpResponse.json({ code: 'TOPIC_ALREADY_EXISTS', message: '同名 Topic 已存在。',
      fields: { name: '请使用其他 Topic 名称。' }, requestId: 'request-create' }, { status: 409 })
  }))
  render(<CreateTopicModal sessionId="session-a" onClose={() => {}} onApplied={() => {}} />)
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

// Catches silently discarding a partly completed form.
it('confirms before discarding a dirty form and closes a pristine form directly', async () => {
  const user = userEvent.setup()
  const confirm = vi.spyOn(window, 'confirm').mockReturnValue(false)
  const onClose = vi.fn()
  render(<CreateTopicModal sessionId="session-a" onClose={onClose} onApplied={() => {}} />)
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
it('does not apply a completed creation to a page that has already left the session', async () => {
  const user = userEvent.setup()
  let release!: () => void
  const pending = new Promise<void>(resolve => { release = resolve })
  let finished = false
  server.use(http.post('/api/sessions/:id/topics', async () => {
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
