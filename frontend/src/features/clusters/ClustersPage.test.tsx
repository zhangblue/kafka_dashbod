import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { http, HttpResponse } from 'msw'
import { ClustersPage } from './ClustersPage'
import { clusterFixture, server, snapshotFixture } from '../../test/handlers'

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

function renderPage(snapshot: typeof snapshotFixture | null = snapshotFixture) {
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
})
