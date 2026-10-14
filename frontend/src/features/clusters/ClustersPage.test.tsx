import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { http, HttpResponse } from 'msw'
import { ClustersPage } from './ClustersPage'
import type { ClusterDto, ClusterSnapshot, LagRuleSet } from '../../api/types'
import { clusterFixture, server, snapshotFixture } from '../../test/handlers'

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

function renderPage(snapshot: ClusterSnapshot | null = snapshotFixture) {
  return render(<QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
    <ClustersPage snapshot={snapshot} />
  </QueryClientProvider>)
}

describe('cluster settings', () => {
  it('adds, edits with blank secrets preserved, tests connection, and deletes a cluster', async () => {
    const user = userEvent.setup()
    let clusters = [clusterFixture]
    let saved: Record<string, unknown> | undefined
    server.use(
      http.get('/api/clusters', () => HttpResponse.json(clusters)),
      http.post('/api/clusters', async ({ request }) => {
        saved = await request.json() as Record<string, unknown>
        clusters = [...clusters, { ...clusterFixture, ...saved, id: 'new' } as typeof clusterFixture]
        return HttpResponse.json(clusters[1], { status: 201 })
      }),
      http.put('/api/clusters/:id', async ({ request }) => {
        saved = await request.json() as Record<string, unknown>
        clusters = clusters.map(cluster => cluster.id === 'new' ? { ...cluster, ...saved } as typeof clusterFixture : cluster)
        return HttpResponse.json(clusters[1])
      }),
      http.post('/api/clusters/:id/test', () => HttpResponse.json({ brokerCount: 3, topicCount: 12, elapsedMs: 20 })),
      http.delete('/api/clusters/:id', ({ params }) => {
        clusters = clusters.filter(cluster => cluster.id !== params.id)
        return new HttpResponse(null, { status: 204 })
      }),
    )
    renderPage()
    await screen.findByText('生产 Kafka')
    await user.click(screen.getByRole('button', { name: '新增集群' }))
    await user.type(screen.getByLabelText('集群名称'), '测试集群')
    await user.type(screen.getByLabelText('Bootstrap Servers'), 'test:9092')
    await user.click(screen.getByRole('button', { name: '保存集群' }))
    const row = await screen.findByRole('group', { name: '测试集群' })
    expect(saved).toMatchObject({ name: '测试集群', bootstrapServers: 'test:9092', defaultNormalLagMax: 999, defaultHighLagMin: 5000, defaultOverLimitSeconds: 30 })
    await user.click(within(row).getByRole('button', { name: '编辑' }))
    expect(screen.getByLabelText('密码')).toHaveValue('')
    expect(screen.getByLabelText('客户端私钥 PEM')).toHaveValue('')
    await user.clear(screen.getByLabelText('集群名称'))
    await user.type(screen.getByLabelText('集群名称'), '已编辑集群')
    await user.click(screen.getByRole('button', { name: '保存集群' }))
    expect(saved).not.toHaveProperty('password')
    expect(saved).not.toHaveProperty('clientKeyPem')
    const edited = await screen.findByRole('group', { name: '已编辑集群' })
    await user.click(within(edited).getByRole('button', { name: '测试连接' }))
    await screen.findByText(/3.*Broker.*12.*Topic/)
    await user.click(within(edited).getByRole('button', { name: '删除' }))
    await waitFor(() => expect(screen.queryByRole('group', { name: '已编辑集群' })).not.toBeInTheDocument())
  })

  it('displays server field errors and request ID without discarding input', async () => {
    const user = userEvent.setup()
    server.use(http.post('/api/clusters', () => HttpResponse.json({ code: 'VALIDATION_ERROR', message: 'The request contains invalid fields',
      details: { bootstrapServers: '地址无效' }, requestId: 'req-ui' }, { status: 422 })))
    renderPage()
    await user.click(screen.getByRole('button', { name: '新增集群' }))
    await user.type(screen.getByLabelText('集群名称'), '保留输入')
    await user.type(screen.getByLabelText('Bootstrap Servers'), 'invalid:9092')
    await user.click(screen.getByRole('button', { name: '保存集群' }))
    await screen.findByText('地址无效')
    expect(screen.getByLabelText('Bootstrap Servers')).toHaveAccessibleDescription('地址无效')
    expect(screen.getByText(/req-ui/)).toBeInTheDocument()
    expect(screen.getByLabelText('集群名称')).toHaveValue('保留输入')
  })

  it('reuses an active snapshot and atomically saves defaults and group/topic rules', async () => {
    const user = userEvent.setup()
    let saved: unknown
    server.use(http.put('/api/clusters/:id/lag-rules', async ({ request }) => {
      const body = await request.json() as Record<string, unknown>
      saved = body
      return HttpResponse.json(body)
    }))
    renderPage()
    await user.click(await screen.findByRole('button', { name: '配置 Lag 规则' }))
    await user.click(await screen.findByRole('button', { name: '新增规则' }))
    await user.selectOptions(screen.getByLabelText('Consumer Group'), 'flink-order-enrich')
    await user.selectOptions(screen.getByLabelText('Topic'), 'raw-orders')
    await user.click(screen.getByRole('button', { name: '保存规则' }))
    await screen.findByText('规则已保存')
    expect(saved).toEqual({ defaults: { normalMax: 999, highMin: 5000, overLimitSeconds: 30 },
      overrides: [{ groupId: 'flink-order-enrich', topic: 'raw-orders', normalMax: 999, highMin: 5000, overLimitSeconds: 30 }] })
  })

  it('associates rule API field errors with the numeric input and topic selector', async () => {
    const user = userEvent.setup()
    server.use(http.put('/api/clusters/:id/lag-rules', () => HttpResponse.json({ code: 'VALIDATION_ERROR', message: 'Invalid rule fields',
      details: { 'defaults.normalMax': '正常上限无效', 'overrides.0.topic': 'Topic 目前不可选' }, requestId: 'req-rules' }, { status: 422 })))
    renderPage()
    await user.click(await screen.findByRole('button', { name: '配置 Lag 规则' }))
    await user.click(await screen.findByRole('button', { name: '新增规则' }))
    await user.selectOptions(screen.getByLabelText('Consumer Group'), 'flink-order-enrich')
    await user.selectOptions(screen.getByLabelText('Topic'), 'raw-orders')
    await user.click(screen.getByRole('button', { name: '保存规则' }))
    await screen.findByText(/req-rules/)
    expect(within(screen.getByRole('group', { name: '集群默认规则' })).getByLabelText('正常上限')).toHaveAccessibleDescription('正常上限无效')
    expect(screen.getByLabelText('Topic')).toHaveAccessibleDescription('Topic 目前不可选')
    expect(screen.getByLabelText('Topic')).toHaveAttribute('aria-invalid', 'true')
  })

  it('opens and releases a temporary configuration session when no active snapshot exists', async () => {
    const user = userEvent.setup()
    let closed = false
    server.use(
      http.post('/api/clusters/:id/sessions', () => HttpResponse.json({ sessionId: 'settings-session', leaseSeconds: 30 }, { status: 201 })),
      http.get('/api/sessions/settings-session/snapshot', () => HttpResponse.json(snapshotFixture)),
      http.delete('/api/sessions/settings-session', () => { closed = true; return new HttpResponse(null, { status: 204 }) }),
    )
    renderPage(null)
    await user.click(await screen.findByRole('button', { name: '配置 Lag 规则' }))
    await waitFor(() => expect(screen.getByRole('button', { name: '新增规则' })).toBeEnabled())
    await user.click(screen.getByRole('button', { name: '关闭规则设置' }))
    await waitFor(() => expect(closed).toBe(true))
  })

  it('retains old rules read-only after a connection failure and supports reconnecting', async () => {
    const user = userEvent.setup()
    let reachable = false
    server.use(
      http.post('/api/clusters/:id/sessions', () => reachable
        ? HttpResponse.json({ sessionId: 'reconnected', leaseSeconds: 30 }, { status: 201 })
        : HttpResponse.json({ code: 'KAFKA_TIMEOUT', message: '连接超时', requestId: 'req-connect' }, { status: 502 })),
      http.get('/api/sessions/reconnected/snapshot', () => HttpResponse.json(snapshotFixture)),
      http.delete('/api/sessions/reconnected', () => new HttpResponse(null, { status: 204 })),
    )
    renderPage(null)
    await user.click(await screen.findByRole('button', { name: '配置 Lag 规则' }))
    await screen.findByText(/req-connect/)
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
    reachable = true
    await user.click(screen.getByRole('button', { name: '重新连接' }))
    await waitFor(() => expect(screen.getByRole('button', { name: '新增规则' })).toBeEnabled())
  })

  it('offers reconnect for a total-failure HTTP 200 snapshot and releases that session', async () => {
    const user = userEvent.setup()
    let collections = 0
    let closed = 0
    server.use(
      http.post('/api/clusters/:id/sessions', () => HttpResponse.json({ sessionId: 'full-failure', leaseSeconds: 30 }, { status: 201 })),
      http.get('/api/sessions/full-failure/snapshot', () => HttpResponse.json(++collections === 1
        ? { ...snapshotFixture, consumerGroupIds: [], consumerGroups: [], partialFailures: [
          { scope: 'topics', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'All topics failed' },
          { scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'All groups failed' },
        ] } : snapshotFixture)),
      http.delete('/api/sessions/full-failure', () => { closed++; return new HttpResponse(null, { status: 204 }) }),
    )
    renderPage(null)
    await user.click(await screen.findByRole('button', { name: '配置 Lag 规则' }))
    await screen.findByText(/All topics failed/)
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
    await waitFor(() => expect(closed).toBe(1))
    await user.click(screen.getByRole('button', { name: '重新连接' }))
    await waitFor(() => expect(screen.getByRole('button', { name: '新增规则' })).toBeEnabled())
    await user.click(screen.getByRole('button', { name: '关闭规则设置' }))
    await waitFor(() => expect(closed).toBe(2))
  })

  it('offers reconnect when the reused monitoring snapshot has failed all discovery', async () => {
    const user = userEvent.setup()
    server.use(
      http.post('/api/clusters/:id/sessions', () => HttpResponse.json({ sessionId: 'shared-retry', leaseSeconds: 30 }, { status: 201 })),
      http.get('/api/sessions/shared-retry/snapshot', () => HttpResponse.json(snapshotFixture)),
      http.delete('/api/sessions/shared-retry', () => new HttpResponse(null, { status: 204 })),
    )
    renderPage({ ...snapshotFixture, consumerGroupIds: [], consumerGroups: [], partialFailures: [
      { scope: 'topics', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'All topics failed' },
      { scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'All groups failed' },
    ] })
    await user.click(await screen.findByRole('button', { name: '配置 Lag 规则' }))
    await screen.findByRole('button', { name: '新增规则' })
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
    await user.click(screen.getByRole('button', { name: '重新连接' }))
    await waitFor(() => expect(screen.getByRole('button', { name: '新增规则' })).toBeEnabled())
  })

  it('keeps cluster and Lag editors exclusive and preserves latest defaults in both directions', async () => {
    const user = userEvent.setup()
    let cluster: ClusterDto = { ...clusterFixture }
    let rules: LagRuleSet = { defaults: { normalMax: 999, highMin: 5000, overLimitSeconds: 30 }, overrides: [] }
    server.use(
      http.get('/api/clusters', () => HttpResponse.json([cluster])),
      http.get('/api/clusters/:id/lag-rules', () => HttpResponse.json(rules)),
      http.put('/api/clusters/:id', async ({ request }) => {
        cluster = { ...cluster, ...await request.json() as ClusterDto }
        rules = { ...rules, defaults: { normalMax: cluster.defaultNormalLagMax, highMin: cluster.defaultHighLagMin, overLimitSeconds: cluster.defaultOverLimitSeconds } }
        return HttpResponse.json(cluster)
      }),
      http.put('/api/clusters/:id/lag-rules', async ({ request }) => {
        rules = await request.json() as LagRuleSet
        cluster = { ...cluster, defaultNormalLagMax: rules.defaults.normalMax, defaultHighLagMin: rules.defaults.highMin, defaultOverLimitSeconds: rules.defaults.overLimitSeconds }
        return HttpResponse.json(rules)
      }),
    )
    renderPage()
    await user.click(await screen.findByRole('button', { name: '编辑' }))
    await user.click(screen.getByRole('button', { name: '配置 Lag 规则' }))
    await screen.findByRole('button', { name: '保存规则' })
    expect(screen.queryByRole('button', { name: '保存集群' })).not.toBeInTheDocument()
    await user.clear(screen.getByLabelText('正常上限'))
    await user.type(screen.getByLabelText('正常上限'), '123')
    await user.click(screen.getByRole('button', { name: '保存规则' }))
    await screen.findByText('规则已保存')
    await user.click(screen.getByRole('button', { name: '编辑' }))
    expect(screen.queryByRole('button', { name: '保存规则' })).not.toBeInTheDocument()
    expect(screen.getByLabelText('默认正常上限')).toHaveValue(123)
    await user.clear(screen.getByLabelText('默认持续超限时间（秒）'))
    await user.type(screen.getByLabelText('默认持续超限时间（秒）'), '45')
    await user.click(screen.getByRole('button', { name: '保存集群' }))
    await waitFor(() => expect(screen.queryByRole('button', { name: '保存集群' })).not.toBeInTheDocument())
    await user.click(screen.getByRole('button', { name: '配置 Lag 规则' }))
    expect(await screen.findByLabelText('正常上限')).toHaveValue(123)
    expect(screen.getByLabelText('持续超限时间（秒）')).toHaveValue(45)
    await user.click(screen.getByRole('button', { name: '保存规则' }))
    await screen.findByText('规则已保存')
    expect(cluster.defaultNormalLagMax).toBe(123)
    expect(cluster.defaultOverLimitSeconds).toBe(45)
  })

  it('blocks editor target changes while a cluster save is pending', async () => {
    const user = userEvent.setup()
    let finish!: () => void
    const pending = new Promise<void>(resolve => { finish = resolve })
    server.use(
      http.get('/api/clusters', () => HttpResponse.json([clusterFixture, { ...clusterFixture, id: 'cluster-b', name: '集群 B' }])),
      http.put('/api/clusters/:id', async () => { await pending; return HttpResponse.json(clusterFixture) }),
    )
    renderPage()
    await user.click((await screen.findAllByRole('button', { name: '编辑' }))[0])
    await user.clear(screen.getByLabelText('集群名称'))
    await user.type(screen.getByLabelText('集群名称'), '正在保存 A')
    await user.click(screen.getByRole('button', { name: '保存集群' }))
    try {
      expect(screen.getByRole('button', { name: '新增集群' })).toBeDisabled()
      expect(screen.getAllByRole('button', { name: '编辑' })[1]).toBeDisabled()
      expect(screen.getAllByRole('button', { name: '配置 Lag 规则' })[0]).toBeDisabled()
      await user.click(screen.getByRole('button', { name: '新增集群' }))
      expect(screen.getByLabelText('集群名称')).toHaveValue('正在保存 A')
    } finally { finish() }
    await waitFor(() => expect(screen.queryByLabelText('集群名称')).not.toBeInTheDocument())
    await user.click(screen.getByRole('button', { name: '新增集群' }))
    await user.type(screen.getByLabelText('集群名称'), '保存后的新草稿')
    expect(screen.getByLabelText('集群名称')).toHaveValue('保存后的新草稿')
  })
})
