import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { http, HttpResponse } from 'msw'
import { apiFetch, ApiError, clustersApi, topicsApi } from './client'
import { clusterFixture, server } from '../test/handlers'

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

describe('API boundary', () => {
  it('encodes each topic management path segment and sends exact mutation bodies', async () => {
    const calls: { method: string; path: string; body: unknown }[] = []
    const configs = { configs: [{ key: 'retention.ms', value: null, source: 'unknown', readOnly: true }] }
    server.use(http.all('*', async ({ request }) => {
      calls.push({ method: request.method, path: new URL(request.url).pathname,
        body: request.method === 'GET' ? null : await request.json() })
      if (request.method === 'DELETE') return new HttpResponse(null, { status: 204 })
      return HttpResponse.json(request.method === 'POST' ? { status: 'created' } : configs,
        { status: request.method === 'POST' ? 201 : 200 })
    }))
    const input = { name: 'orders', partitions: 1, replicationFactor: null, configs: {} }
    const update = { changes: { 'retention.ms': null }, expectedValues: { 'retention.ms': '60000' } }
    await expect(topicsApi.create('session/a?#%', input)).resolves.toEqual({ status: 'created' })
    await expect(topicsApi.configs('session/a?#%', 'orders/a?#%')).resolves.toEqual(configs)
    await expect(topicsApi.update('session/a?#%', 'orders/a?#%', update)).resolves.toEqual(configs)
    await expect(topicsApi.delete('session/a?#%', 'orders/a?#%', 'orders/a?#%')).resolves.toBeUndefined()
    expect(calls).toEqual([
      { method: 'POST', path: '/api/sessions/session%2Fa%3F%23%25/topics', body: input },
      { method: 'GET', path: '/api/sessions/session%2Fa%3F%23%25/topics/orders%2Fa%3F%23%25/configs', body: null },
      { method: 'PUT', path: '/api/sessions/session%2Fa%3F%23%25/topics/orders%2Fa%3F%23%25/configs', body: update },
      { method: 'DELETE', path: '/api/sessions/session%2Fa%3F%23%25/topics/orders%2Fa%3F%23%25', body: { confirmationName: 'orders/a?#%' } },
    ])
  })

  it('preserves applied-but-unconfirmed results and supports cancellation of config reads', async () => {
    server.use(http.put('/api/sessions/:id/topics/:topic/configs', () =>
      HttpResponse.json({ status: 'appliedButUnconfirmed' }, { status: 202 })))
    await expect(topicsApi.update('session', 'orders', {
      changes: { 'retention.ms': null }, expectedValues: { 'retention.ms': null },
    })).resolves.toEqual({ status: 'appliedButUnconfirmed' })
    const controller = new AbortController()
    controller.abort()
    await expect(topicsApi.configs('session', 'orders', controller.signal)).rejects.toMatchObject({ name: 'AbortError' })
  })

  it('sends JSON and retains structured validation fields and request ID', async () => {
    server.use(http.post('/api/invalid', async ({ request }) => {
      expect(request.headers.get('content-type')).toBe('application/json')
      expect(await request.json()).toEqual({ name: '' })
      return HttpResponse.json({ code: 'VALIDATION_ERROR', message: 'Invalid fields',
        details: { name: 'Name is required' }, requestId: 'req-42' }, { status: 422 })
    }))
    await expect(apiFetch('/api/invalid', { method: 'POST', body: JSON.stringify({ name: '' }) }))
      .rejects.toMatchObject({ code: 'VALIDATION_ERROR', fields: { name: 'Name is required' }, requestId: 'req-42', status: 422 })
  })

  it('handles no-content deletion and URL-encodes cluster identities', async () => {
    let deleted = ''
    server.use(http.delete('/api/clusters/:id', ({ params }) => {
      deleted = params.id as string
      return new HttpResponse(null, { status: 204 })
    }))
    await expect(clustersApi.delete('cluster/a')).resolves.toBeUndefined()
    expect(deleted).toBe('cluster/a')
  })

  it('supports an AbortSignal and does not expose non-JSON response contents', async () => {
    server.use(http.get('/api/broken', () => new HttpResponse('private backend failure', { status: 502, headers: { 'x-request-id': 'req-html' } })))
    const error = await apiFetch('/api/broken').catch(error => error)
    expect(error).toBeInstanceOf(ApiError)
    expect(error).toMatchObject({ requestId: 'req-html', status: 502 })
    expect((error as ApiError).message).not.toContain('private backend failure')
    const controller = new AbortController()
    controller.abort()
    await expect(apiFetch('/api/clusters', { signal: controller.signal })).rejects.toMatchObject({ name: 'AbortError' })
  })

  it('reads the public cluster DTO without any password fields', async () => {
    expect(await clustersApi.list()).toEqual([clusterFixture])
    expect(await clustersApi.list()).not.toHaveProperty('0.password')
  })
})
