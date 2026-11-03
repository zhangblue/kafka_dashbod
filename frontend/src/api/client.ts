import type { ClusterDto, ClusterInput, ClusterSnapshot, ConnectionTestResult, CreatedSession, CreateTopicInput,
  FeishuAlertSettings, SaveFeishuAlertSettings, LagRuleSet, TopicConfigsResponse, TopicMutationResult, UpdateTopicConfigsInput } from './types'

export class ApiError extends Error {
  readonly status: number
  readonly code: string
  readonly fields: Record<string, string>
  readonly requestId?: string

  // 在 HTTP 失败响应转换时保存结构化字段与请求标识，显示端须经过安全错误翻译。
  constructor(status: number, body: { code?: string; message?: string; fields?: Record<string, string>; details?: Record<string, string>; requestId?: string }) {
    // 错误对象只保留内部占位；页面必须按 code 翻译，避免后端 message 被其他入口直接展示。
    super('API_REQUEST_FAILED')
    this.name = 'ApiError'
    this.status = status
    this.code = body.code ?? 'HTTP_ERROR'
    this.fields = body.fields ?? body.details ?? {}
    this.requestId = body.requestId
  }
}

// 各 API 方法调用时统一发送 JSON 请求、传递取消信号，并将失败响应转换为安全错误。
export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('Accept', 'application/json')
  if (init.body != null) headers.set('Content-Type', 'application/json')
  // 保留调用方的 signal，使页面切换或隐藏时可以取消在途 HTTP 请求。
  const response = await fetch(new URL(path, window.location.origin), { ...init, headers })
  if (!response.ok) {
    // 解析失败时丢弃非 JSON 内容，避免将原始服务端错误正文暴露给页面。
    const body = await response.json().catch(() => ({}))
    throw new ApiError(response.status, { ...body, requestId: body.requestId ?? response.headers.get('x-request-id') ?? undefined })
  }
  if (response.status === 204) return undefined as T
  return response.json() as Promise<T>
}

// 发起单集群请求前编码集群标识，防止标识中的斜杠改变路由层级。
const clusterPath = (id: string) => `/api/clusters/${encodeURIComponent(id)}`
// 发起会话请求前编码会话标识，保持单一路径段边界。
const sessionPath = (id: string) => `/api/sessions/${encodeURIComponent(id)}`
// 执行 Topic 管理前生成当前会话下的集合地址，确保管理绑定监控会话。
const topicsPath = (sessionId: string) => `${sessionPath(sessionId)}/topics`
// 读取或变更指定 Topic 前单独编码名称，避免特殊字符改变管理目标。
const topicPath = (sessionId: string, topic: string) => `${topicsPath(sessionId)}/${encodeURIComponent(topic)}`

export const clustersApi = {
  // 集群列表加载时读取已保存配置，可随页面请求取消。
  list: (signal?: AbortSignal) => apiFetch<ClusterDto[]>('/api/clusters', { signal }),
  // 配置编辑前读取指定集群的公开配置，可随页面请求取消。
  get: (id: string, signal?: AbortSignal) => apiFetch<ClusterDto>(clusterPath(id), { signal }),
  // 新增表单提交时持久化集群配置，不在此创建监控会话。
  create: (value: ClusterInput) => apiFetch<ClusterDto>('/api/clusters', { method: 'POST', body: JSON.stringify(value) }),
  // 编辑表单提交时更新集群配置。
  update: (id: string, value: ClusterInput) => apiFetch<ClusterDto>(clusterPath(id), { method: 'PUT', body: JSON.stringify(value) }),
  // 确认删除时移除指定集群，由服务端协调会话释放。
  delete: (id: string) => apiFetch<void>(clusterPath(id), { method: 'DELETE' }),
  // 连接测试按钮触发时验证已保存配置，并传递可选取消信号。
  test: (id: string, signal?: AbortSignal) => apiFetch<ConnectionTestResult>(`${clusterPath(id)}/test`, { method: 'POST', signal }),
  // 草稿测试时提交当前表单，编辑场景携带集群标识以处理保留凭据。
  testDraft: (value: ClusterInput, id?: string) => apiFetch<ConnectionTestResult>(id ? `${clusterPath(id)}/test` : '/api/clusters/test',
    { method: 'POST', body: JSON.stringify(value) }),
}
export const lagRulesApi = {
  // 规则页面加载时读取默认规则与覆盖规则。
  get: (id: string, signal?: AbortSignal) => apiFetch<LagRuleSet>(`${clusterPath(id)}/lag-rules`, { signal }),
  // 规则表单保存时提交整套 Lag 判定规则。
  save: (id: string, value: LagRuleSet) => apiFetch<LagRuleSet>(`${clusterPath(id)}/lag-rules`, { method: 'PUT', body: JSON.stringify(value) }),
}
export const feishuAlertsApi = {
  // 告警配置加载时读取规则及令牌配置标志。
  get: (id: string, signal?: AbortSignal) => apiFetch<FeishuAlertSettings>(`${clusterPath(id)}/feishu-alert`, { signal }),
  // 告警保存时提交规则及令牌变更意图。
  save: (id: string, value: SaveFeishuAlertSettings) => apiFetch<FeishuAlertSettings>(`${clusterPath(id)}/feishu-alert`, { method: 'PUT', body: JSON.stringify(value) }),
  // 用户测试飞书通知时提交可选草稿令牌。
  test: (id: string, accessToken?: string) => apiFetch<{ ok: boolean }>(`${clusterPath(id)}/feishu-alert/test`, { method: 'POST', body: JSON.stringify({ accessToken }) }),
}
export const sessionsApi = {
  // 可见监控页面按需连接时创建短租约，会话关闭顺序由调用方协调。
  create: (clusterId: string, signal?: AbortSignal) => apiFetch<CreatedSession>(`${clusterPath(clusterId)}/sessions`, { method: 'POST', signal }),
  // 轮询或手动刷新时采集快照并续租，信号用于取消 HTTP 等待。
  snapshot: (id: string, signal?: AbortSignal) => apiFetch<ClusterSnapshot>(`${sessionPath(id)}/snapshot`, { signal }),
  // 离开或重建会话时释放服务端资源，使用 keepalive 支持页面离开阶段。
  close: (id: string) => apiFetch<void>(sessionPath(id), { method: 'DELETE', keepalive: true }),
}
export const topicsApi = {
  // 确认创建时在当前有效会话中提交 Topic 参数。
  create: (sessionId: string, value: CreateTopicInput) => apiFetch<Extract<TopicMutationResult, { status: 'created' }>>(
    topicsPath(sessionId), { method: 'POST', body: JSON.stringify(value) }),
  // 配置弹窗打开时实时读取当前 Topic 配置，可取消过期请求。
  configs: (sessionId: string, topic: string, signal?: AbortSignal) => apiFetch<TopicConfigsResponse>(
    `${topicPath(sessionId, topic)}/configs`, { signal }),
  // 确认保存时提交增量变更与期望值，保留已应用但未确认的响应语义。
  update: (sessionId: string, topic: string, value: UpdateTopicConfigsInput) => apiFetch<TopicConfigsResponse | Extract<TopicMutationResult, { status: 'appliedButUnconfirmed' }>>(
    `${topicPath(sessionId, topic)}/configs`, { method: 'PUT', body: JSON.stringify(value) }),
  // 确认删除时发送完整确认名称，供服务端严格匹配目标。
  delete: (sessionId: string, topic: string, confirmationName: string) => apiFetch<void>(
    topicPath(sessionId, topic), { method: 'DELETE', body: JSON.stringify({ confirmationName }) }),
}
