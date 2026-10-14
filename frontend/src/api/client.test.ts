import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest'
import { http, HttpResponse } from 'msw'
import { apiFetch, ApiError, clustersApi } from './client'
import { clusterFixture, server } from '../test/handlers'

beforeAll(() => server.listen({ onUnhandledFrame: 'error' }))
afterEach(() => server.resetHandlers())
afterAll(() => server.close())

describe('API boundary', () => {
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
