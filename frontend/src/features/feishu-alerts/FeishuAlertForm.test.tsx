import { configure, fireEvent, getConfig, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { http, HttpResponse } from 'msw'
import { feishuAlertsApi } from '../../api/client'
import type { ClusterSnapshot, FeishuAlertSettings } from '../../api/types'
import { server, snapshotFixture } from '../../test/handlers'
import { FeishuAlertForm } from './FeishuAlertForm'

// A missing selector must never dump a form containing a newly entered credential.
const originalGetElementError = getConfig().getElementError
configure({ getElementError: message => new Error(message ?? '未找到表单控件') })
beforeAll(() => server.listen({ onUnhandledFrame: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => { server.close(); configure({ getElementError: originalGetElementError }) })

const settings: FeishuAlertSettings = {
  accessTokenConfigured: true, maskedAccessToken: '••••••••a1b2',
  rules: [{ id: 'rule-a', groupId: 'flink-order-enrich', topic: 'raw-orders', lagThreshold: 0, growthSeconds: 10, enabled: true }],
}
const freshSnapshot = (): ClusterSnapshot => ({ ...snapshotFixture, collectedAt: new Date().toISOString() })
const input = (name: string) => screen.getByLabelText<HTMLInputElement>(name)
const draftToken = () => ['draft', 'credential', 'a1b2'].join('-')

function mount(props: Partial<React.ComponentProps<typeof FeishuAlertForm>> = {}) {
  return render(<FeishuAlertForm clusterId="cluster-a" value={settings} snapshot={freshSnapshot()} {...props} />)
}

describe('飞书告警配置', () => {
  it('shows only the saved mask and toggles visibility of this edit only', async () => {
    const user = userEvent.setup()
    mount()
    expect(screen.getByText('••••••••a1b2')).toBeInTheDocument()
    expect(input('更新 Token').value === '').toBe(true)
    const show = screen.getByRole('button', { name: '显示 Token' })
    expect(show).toHaveAttribute('title', '显示 Token')
    expect(show).toBeDisabled()
    await user.type(input('更新 Token'), draftToken())
    expect(input('更新 Token').type === 'password').toBe(true)
    await user.click(show)
    expect(input('更新 Token').type === 'text').toBe(true)
    expect(input('更新 Token').value === draftToken()).toBe(true)
    const hide = screen.getByRole('button', { name: '隐藏 Token' })
    expect(hide.title === '隐藏 Token').toBe(true)
    await user.click(hide)
    expect(input('更新 Token').type === 'password').toBe(true)
    expect(document.body.textContent?.includes(draftToken())).toBe(false)
  })

  it('clears a new draft when selecting token removal and persists the explicit clear', async () => {
    const user = userEvent.setup()
    let body: Record<string, unknown> | undefined
    server.use(http.put('/api/clusters/cluster-a/feishu-alert', async ({ request }) => {
      body = await request.json() as Record<string, unknown>
      return HttpResponse.json({ ...settings, accessTokenConfigured: false, maskedAccessToken: null })
    }))
    mount()
    await user.type(input('更新 Token'), draftToken())
    await user.click(screen.getByRole('checkbox', { name: '清除已保存 Token' }))
    expect(input('更新 Token').disabled).toBe(true)
    expect(input('更新 Token').value === '').toBe(true)
    expect(screen.getByRole('button', { name: '测试通知' })).toBeDisabled()
    await user.click(screen.getByRole('button', { name: '保存配置' }))
    await screen.findByText('飞书告警配置已保存')
    expect(body?.clearAccessToken).toBe(true)
    expect(Object.hasOwn(body ?? {}, 'accessToken')).toBe(false)
    expect(screen.queryByText('••••••••a1b2')).not.toBeInTheDocument()
  })

  it('derives sorted unique group and committed-topic options and resets topic on group change', async () => {
    const user = userEvent.setup()
    const snapshot = freshSnapshot()
    snapshot.consumerGroupIds = ['flink-order-enrich', 'audit-worker', 'audit-worker', 'no-offsets']
    snapshot.consumerGroups = [...snapshot.consumerGroups, snapshot.consumerGroups[0]]
    mount({ snapshot })
    const groups = input('Consumer Group')
    expect(within(groups).getAllByRole('option').map(option => option.textContent)).toEqual([
      '请选择 Consumer Group', 'audit-worker', 'flink-order-enrich', 'no-offsets',
    ])
    expect(within(input('Topic')).getAllByRole('option').map(option => option.textContent)).toEqual(['请选择 Topic', 'raw-orders'])
    await user.selectOptions(groups, 'audit-worker')
    expect(input('Topic').value).toBe('')
    expect(screen.getByRole('option', { name: 'audit-log' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
  })

  it.each([
    ['持续时间（秒）', '9', /至少为 10 秒/],
    ['持续时间（秒）', '', /至少为 10 秒/],
    ['Lag 阈值', '-1', /非负安全整数/],
    ['Lag 阈值', '1.5', /非负安全整数/],
    ['Lag 阈值', '9007199254740992', /非负安全整数/],
  ])('rejects invalid %s before sending any save', async (name, value, message) => {
    let saves = 0
    server.use(http.put('/api/clusters/cluster-a/feishu-alert', () => { saves++; return HttpResponse.json(settings) }))
    mount()
    fireEvent.change(input(name), { target: { value } })
    expect(screen.getByRole('alert')).toHaveTextContent(message)
    expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
    fireEvent.submit(screen.getByRole('form', { name: '飞书告警配置' }))
    expect(saves).toBe(0)
  })

  it('adds and removes rules, rejects duplicates including disabled rules, and saves edited values', async () => {
    const user = userEvent.setup()
    let body: Record<string, unknown> | undefined
    let saved = false
    server.use(http.put('/api/clusters/cluster-a/feishu-alert', async ({ request }) => {
      body = await request.json() as Record<string, unknown>
      return HttpResponse.json({ ...settings, rules: [{ ...settings.rules[0], lagThreshold: 250, growthSeconds: 30, enabled: false }] })
    }))
    mount({ onSaved: () => { saved = true } })
    await user.click(screen.getByRole('button', { name: '新增规则' }))
    const second = within(screen.getByRole('group', { name: '告警规则 2' }))
    await user.selectOptions(second.getByLabelText('Consumer Group'), 'flink-order-enrich')
    await user.selectOptions(second.getByLabelText('Topic'), 'raw-orders')
    await user.click(second.getByLabelText('启用规则'))
    expect(screen.getByRole('alert')).toHaveTextContent(/组合不能重复/)
    expect(screen.getByRole('button', { name: '保存配置' })).toBeDisabled()
    await user.click(second.getByRole('button', { name: '删除规则' }))
    fireEvent.change(input('Lag 阈值'), { target: { value: '250' } })
    fireEvent.change(input('持续时间（秒）'), { target: { value: '30' } })
    await user.click(screen.getByLabelText('启用规则'))
    await user.click(screen.getByRole('button', { name: '保存配置' }))
    await screen.findByText('飞书告警配置已保存')
    expect(body).toEqual({ clearAccessToken: false, rules: [{ id: 'rule-a', groupId: 'flink-order-enrich', topic: 'raw-orders', lagThreshold: 250, growthSeconds: 30, enabled: false }] })
    expect(saved).toBe(true)
    expect(input('Lag 阈值').value).toBe('250')
  })

  it.each(['missing', 'stale', 'other-cluster', 'discovery-failed'] as const)('keeps recovery edits available with %s snapshot', async kind => {
    const user = userEvent.setup()
    const snapshot = kind === 'missing' ? null : freshSnapshot()
    if (snapshot && kind === 'stale') snapshot.collectedAt = '2000-01-01T00:00:00Z'
    if (snapshot && kind === 'other-cluster') snapshot.clusterId = 'cluster-b'
    if (snapshot && kind === 'discovery-failed') {
      snapshot.topics = []; snapshot.consumerGroups = []; snapshot.consumerGroupIds = []
      snapshot.partialFailures = ['topics', 'groups'].map(scope => ({ scope, resource: 'cluster-a', code: 'UNAVAILABLE', message: '采集不可用' }))
    }
    mount({ snapshot })
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
    expect(input('Consumer Group').disabled).toBe(true)
    expect(input('Topic').disabled).toBe(true)
    expect(input('Lag 阈值').disabled).toBe(false)
    expect(input('持续时间（秒）').disabled).toBe(false)
    await user.click(screen.getByLabelText('启用规则'))
    expect(input('启用规则').checked).toBe(false)
    expect(screen.getByRole('button', { name: '保存配置' })).toBeEnabled()
    expect(screen.getByRole('button', { name: '测试通知' })).toBeEnabled()
    expect(input('更新 Token').disabled).toBe(false)
    expect(screen.getByRole('checkbox', { name: '清除已保存 Token' })).toBeEnabled()
  })

  it('tests the saved token without sending it, and a new token without saving it', async () => {
    const user = userEvent.setup()
    const bodies: Record<string, unknown>[] = []
    server.use(http.post('/api/clusters/cluster-a/feishu-alert/test', async ({ request }) => {
      bodies.push(await request.json() as Record<string, unknown>)
      return HttpResponse.json({ ok: true })
    }))
    mount()
    await user.click(screen.getByRole('button', { name: '测试通知' }))
    await screen.findByText('测试通知已发送')
    expect(Object.keys(bodies[0])).toEqual([])
    await user.type(input('更新 Token'), draftToken())
    await user.click(screen.getByRole('button', { name: '测试通知' }))
    await waitFor(() => expect(bodies.length).toBe(2))
    expect(bodies[1].accessToken === draftToken()).toBe(true)
    expect(input('更新 Token').value === draftToken()).toBe(true)
  })

  it('requires a configured or new valid token for testing and rejects bad draft format', () => {
    mount({ value: { ...settings, accessTokenConfigured: false, maskedAccessToken: null } })
    expect(screen.getByRole('button', { name: '测试通知' })).toBeDisabled()
    for (const value of ['.', '..', 'é'.repeat(257), 'bad\u0001token']) {
      fireEvent.change(input('更新 Token'), { target: { value } })
      expect(screen.getByRole('button', { name: '测试通知' }).hasAttribute('disabled')).toBe(true)
      expect(screen.getByRole('button', { name: '保存配置' }).hasAttribute('disabled')).toBe(true)
    }
  })

  it('shows Chinese safe failures without rendering remote messages or submitted credentials', async () => {
    const user = userEvent.setup()
    server.use(http.post('/api/clusters/cluster-a/feishu-alert/test', () => HttpResponse.json({ code: 'FEISHU_SEND_FAILED', message: draftToken() }, { status: 502 })))
    mount()
    await user.type(input('更新 Token'), draftToken())
    await user.click(screen.getByRole('button', { name: '测试通知' }))
    await waitFor(() => expect(screen.getByRole('alert').textContent?.includes('测试通知发送失败')).toBe(true))
    expect(document.body.textContent?.includes(draftToken())).toBe(false)
  })

  it('resets unsaved credentials and rules when the cluster or loaded settings change', async () => {
    const user = userEvent.setup()
    const view = mount()
    await user.type(input('更新 Token'), draftToken())
    await user.click(screen.getByRole('button', { name: '显示 Token' }))
    view.rerender(<FeishuAlertForm clusterId="cluster-b" value={{ ...settings, rules: [] }} snapshot={null} />)
    expect(input('更新 Token').value === '').toBe(true)
    expect(input('更新 Token').type === 'password').toBe(true)
    expect(screen.queryByLabelText('Lag 阈值')).not.toBeInTheDocument()
    await user.type(input('更新 Token'), draftToken())
    view.rerender(<FeishuAlertForm clusterId="cluster-b" value={{ ...settings, maskedAccessToken: '••••••••c3d4' }} snapshot={null} />)
    expect(input('更新 Token').value === '').toBe(true)
    expect(screen.getByText('••••••••c3d4')).toBeInTheDocument()
  })

  it('uses unified compact control classes and native minimum constraints', () => {
    mount()
    for (const name of ['更新 Token', 'Consumer Group', 'Topic', 'Lag 阈值', '持续时间（秒）']) {
      expect(input(name)).toHaveClass('feishu-control')
    }
    expect(input('Lag 阈值')).toHaveAttribute('min', '0')
    expect(input('持续时间（秒）')).toHaveAttribute('min', '10')
    expect(input('Lag 阈值')).toHaveClass('feishu-threshold')
    expect(input('持续时间（秒）')).toHaveClass('feishu-duration')
    expect(input('Lag 阈值').closest('.feishu-rules-scroll')).not.toBeNull()
  })
})

describe('飞书告警 API boundary', () => {
  it('encodes cluster IDs, sends exact endpoint bodies, and supports cancelled reads', async () => {
    const calls: { method: string; path: string; body: unknown }[] = []
    server.use(http.all('*', async ({ request }) => {
      calls.push({ method: request.method, path: new URL(request.url).pathname, body: request.method === 'GET' ? null : await request.json() })
      return HttpResponse.json(request.url.endsWith('/test') ? { ok: true } : settings)
    }))
    const value = { clearAccessToken: false, rules: [] }
    expect(await feishuAlertsApi.get('a/b?')).toEqual(settings)
    expect(await feishuAlertsApi.save('a/b?', value)).toEqual(settings)
    expect(await feishuAlertsApi.test('a/b?')).toEqual({ ok: true })
    expect(calls).toEqual([
      { method: 'GET', path: '/api/clusters/a%2Fb%3F/feishu-alert', body: null },
      { method: 'PUT', path: '/api/clusters/a%2Fb%3F/feishu-alert', body: value },
      { method: 'POST', path: '/api/clusters/a%2Fb%3F/feishu-alert/test', body: {} },
    ])
    const controller = new AbortController()
    controller.abort()
    await expect(feishuAlertsApi.get('a', controller.signal)).rejects.toMatchObject({ name: 'AbortError' })
  })
})
