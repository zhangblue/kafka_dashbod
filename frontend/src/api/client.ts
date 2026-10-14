import type { ClusterDto, ClusterInput, ClusterSnapshot, ConnectionTestResult, CreatedSession, LagRuleSet } from './types'

export class ApiError extends Error {
  readonly status: number
  readonly code: string
  readonly fields: Record<string, string>
  readonly requestId?: string

  constructor(status: number, body: { code?: string; message?: string; fields?: Record<string, string>; details?: Record<string, string>; requestId?: string }) {
    super(body.message ?? '请求失败，请稍后重试')
    this.name = 'ApiError'
    this.status = status
    this.code = body.code ?? 'HTTP_ERROR'
    this.fields = body.fields ?? body.details ?? {}
    this.requestId = body.requestId
  }
}

export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('Accept', 'application/json')
  if (init.body != null) headers.set('Content-Type', 'application/json')
  const response = await fetch(new URL(path, window.location.origin), { ...init, headers })
  if (!response.ok) {
    const body = await response.json().catch(() => ({}))
    throw new ApiError(response.status, { ...body, requestId: body.requestId ?? response.headers.get('x-request-id') ?? undefined })
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

const clusterPath = (id: string) => `/api/clusters/${encodeURIComponent(id)}`
const sessionPath = (id: string) => `/api/sessions/${encodeURIComponent(id)}`

export const clustersApi = {
  list: (signal?: AbortSignal) => apiFetch<ClusterDto[]>('/api/clusters', { signal }),
  get: (id: string, signal?: AbortSignal) => apiFetch<ClusterDto>(clusterPath(id), { signal }),
  create: (value: ClusterInput) => apiFetch<ClusterDto>('/api/clusters', { method: 'POST', body: JSON.stringify(value) }),
  update: (id: string, value: ClusterInput) => apiFetch<ClusterDto>(clusterPath(id), { method: 'PUT', body: JSON.stringify(value) }),
  delete: (id: string) => apiFetch<void>(clusterPath(id), { method: 'DELETE' }),
  test: (id: string, signal?: AbortSignal) => apiFetch<ConnectionTestResult>(`${clusterPath(id)}/test`, { method: 'POST', signal }),
  testDraft: (value: ClusterInput, id?: string) => apiFetch<ConnectionTestResult>(id ? `${clusterPath(id)}/test` : '/api/clusters/test',
    { method: 'POST', body: JSON.stringify(value) }),
}
export const lagRulesApi = {
  get: (id: string, signal?: AbortSignal) => apiFetch<LagRuleSet>(`${clusterPath(id)}/lag-rules`, { signal }),
  save: (id: string, value: LagRuleSet) => apiFetch<LagRuleSet>(`${clusterPath(id)}/lag-rules`, { method: 'PUT', body: JSON.stringify(value) }),
}
export const sessionsApi = {
  create: (clusterId: string, signal?: AbortSignal) => apiFetch<CreatedSession>(`${clusterPath(clusterId)}/sessions`, { method: 'POST', signal }),
  snapshot: (id: string, signal?: AbortSignal) => apiFetch<ClusterSnapshot>(`${sessionPath(id)}/snapshot`, { signal }),
  close: (id: string) => apiFetch<void>(sessionPath(id), { method: 'DELETE', keepalive: true }),
}
