import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { http, HttpResponse } from 'msw'
import { afterAll, afterEach, beforeAll, expect, it, vi } from 'vitest'
import { server } from '../../test/handlers'
import { DeleteTopicModal } from './DeleteTopicModal'

beforeAll(() => server.listen())
afterEach(() => { server.resetHandlers(); vi.restoreAllMocks() })
afterAll(() => server.close())

// Catches case folding or trimming the confirmation, missing DELETE body, and duplicate submissions.
it('requires the exact complete case-sensitive name and awaits refresh before closing', async () => {
  const user = userEvent.setup()
  const requests: unknown[] = []
  server.use(http.delete('/api/sessions/session-a/topics/Orders.v2', async ({ request }) => {
    requests.push(await request.json())
    return new HttpResponse(null, { status: 204 })
  }))
  let release!: () => void
  const onApplied = vi.fn(() => new Promise<void>(resolve => { release = resolve }))
  const onClose = vi.fn()
  render(<DeleteTopicModal sessionId="session-a" topic="Orders.v2" onClose={onClose} onApplied={onApplied} />)
  expect(screen.getByRole('dialog', { name: '删除 Topic' })).toBeVisible()
  expect(screen.getByText(/不可恢复/)).toBeVisible()
  const input = screen.getByLabelText('确认 Topic 名称')
  const submit = screen.getByRole('button', { name: '确认删除' })
  expect(submit).toBeDisabled()
  for (const name of ['Orders', 'orders.v2', 'Orders.v2 ']) {
    await user.clear(input)
    await user.type(input, name)
    expect(submit).toBeDisabled()
  }
  expect(requests).toEqual([])
  await user.clear(input)
  await user.type(input, 'Orders.v2')
  expect(submit).toBeEnabled()
  await user.click(submit)
  await waitFor(() => expect(onApplied).toHaveBeenCalledOnce())
  expect(input).toBeDisabled()
  expect(screen.getByRole('button', { name: '关闭弹窗' })).toBeDisabled()
  await user.keyboard('{Enter}')
  expect(requests).toEqual([{ confirmationName: 'Orders.v2' }])
  expect(onClose).not.toHaveBeenCalled()
  await act(async () => release())
  expect(onClose).toHaveBeenCalledOnce()
})

// Catches closing on a failed DELETE, losing confirmation, or leaking raw error details.
it('keeps the confirmation and dialog after a safe API failure and allows retry', async () => {
  const user = userEvent.setup()
  let deletes = 0
  server.use(http.delete('/api/sessions/:id/topics/:topic', () => {
    deletes++
    return HttpResponse.json({ code: 'TOPIC_AUTHORIZATION_FAILED', message: 'Native broker error', requestId: 'request-delete' }, { status: 403 })
  }))
  const onClose = vi.fn()
  const onApplied = vi.fn()
  render(<DeleteTopicModal sessionId="session-a" topic="orders" onClose={onClose} onApplied={onApplied} />)
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'orders')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Kafka 账户无权执行此 Topic 操作')
  expect(screen.getByRole('alert')).toHaveTextContent('request-delete')
  expect(screen.getByRole('alert')).not.toHaveTextContent('Native broker error')
  expect(screen.getByLabelText('确认 Topic 名称')).toHaveValue('orders')
  expect(onClose).not.toHaveBeenCalled()
  expect(onApplied).not.toHaveBeenCalled()
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  await waitFor(() => expect(deletes).toBe(2))
})

// Catches repeating a successful DELETE after only the list refresh has failed.
it('locks a successful deletion and offers close if refreshing rejects', async () => {
  const user = userEvent.setup()
  let deletes = 0
  server.use(http.delete('/api/sessions/:id/topics/:topic', () => {
    deletes++
    return new HttpResponse(null, { status: 204 })
  }))
  const onClose = vi.fn()
  render(<DeleteTopicModal sessionId="session-a" topic="orders" onClose={onClose}
    onApplied={async () => { throw new Error('synthetic refresh failure') }} />)
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'orders')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  expect(await screen.findByRole('status')).toHaveTextContent('删除请求已成功，但列表刷新失败。请关闭弹窗后手动刷新。')
  expect(screen.getByRole('button', { name: '删除请求已成功' })).toBeDisabled()
  await user.click(screen.getByRole('button', { name: '删除请求已成功' }))
  expect(deletes).toBe(1)
  expect(onClose).not.toHaveBeenCalled()
  await user.click(screen.getByRole('button', { name: '关闭' }))
  expect(onClose).toHaveBeenCalledOnce()
})

// Catches a delayed DELETE response mutating a page after it left the live session.
it('ignores a deletion response after unmount', async () => {
  const user = userEvent.setup()
  let release!: () => void
  const pending = new Promise<void>(resolve => { release = resolve })
  let finished = false
  server.use(http.delete('/api/sessions/:id/topics/:topic', async () => {
    await pending
    finished = true
    return new HttpResponse(null, { status: 204 })
  }))
  const onApplied = vi.fn()
  const onClose = vi.fn()
  const view = render(<DeleteTopicModal sessionId="session-a" topic="orders" onClose={onClose} onApplied={onApplied} />)
  await user.type(screen.getByLabelText('确认 Topic 名称'), 'orders')
  await user.click(screen.getByRole('button', { name: '确认删除' }))
  view.unmount()
  release()
  await waitFor(() => expect(finished).toBe(true))
  await act(async () => { await new Promise(resolve => setTimeout(resolve, 20)) })
  expect(onApplied).not.toHaveBeenCalled()
  expect(onClose).not.toHaveBeenCalled()
})
