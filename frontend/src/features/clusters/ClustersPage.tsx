import { useEffect, useId, useRef, useState } from 'react'
import type { ReactNode } from 'react'
import { useIsMutating, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError, clustersApi, feishuAlertsApi, lagRulesApi, sessionsApi } from '../../api/client'
import type { ClusterDto, ClusterInput, ClusterSnapshot, FeishuAlertSettings, LagRuleSet } from '../../api/types'
import { ClusterForm, ApiErrorNotice } from './ClusterForm'
import { LagRulesForm } from '../lag-rules/LagRulesForm'
import { snapshotUnavailable } from '../lag-rules/snapshotAvailability'
import { sessionTransition } from '../dashboard/sessionTransitions'
import { Modal } from '../../components/Modal'
import { FeishuAlertForm } from '../feishu-alerts/FeishuAlertForm'
import { useSnapshotStale } from '../dashboard/useSnapshotStale'

// 在摘要和编辑器发起读取前生成按集群隔离的查询配置，并传递取消信号。
function feishuSettingsQuery(clusterId: string) {
  return { queryKey: ['feishu-alerts', clusterId], queryFn: /* 在当前资源查询启动时传递取消信号读取绑定集群的数据，使查询取消能传递到 HTTP 请求。 */ ({ signal }: { signal: AbortSignal }) => feishuAlertsApi.get(clusterId, signal) }
}

// 在集群设置卡片渲染时展示已配置标志与规则数量，避免读取或显示原始 Token。
function FeishuAlertSummary({ clusterId }: { clusterId: string }) {
  const settings = useQuery(feishuSettingsQuery(clusterId))
  return <div className="threshold-summary">飞书告警：{settings.isError ? '摘要加载失败' : !settings.data ? '正在加载…'
    : `${settings.data.accessTokenConfigured ? 'Token 已配置' : '未配置'}${settings.data.accessTokenConfigured || settings.data.rules.length ? ` · ${settings.data.rules.length} 条 Topic + Group 规则` : ''}`}</div>
}

// 在飞书设置打开时维护当前编辑状态、反馈及可用快照，隔离不同集群的草稿。
function FeishuAlertEditor({ cluster, snapshot, onClose }: { cluster: ClusterDto; snapshot: ClusterSnapshot | null; onClose: () => void }) {
  const client = useQueryClient()
  const settings = useQuery({ ...feishuSettingsQuery(cluster.id), refetchOnMount: 'always' })
  // Keep the loaded source stable while onSaved refreshes the summary cache, so the
  // form retains the server result and its save/test feedback until it is closed.
  const [source, setSource] = useState<FeishuAlertSettings | null>(null)
  const [attempt, setAttempt] = useState(0)
  const expired = useSnapshotStale({ snapshot, status: 'connected' }, 10)
  // Cached success can coexist with an offline/paused fetch. Wait for this
  // editor's observer to receive a completed success before restoring the form.
  if (settings.isFetchedAfterMount && settings.isSuccess && settings.fetchStatus === 'idle' && !source) setSource(settings.data)
  useEffect(/* 在飞书编辑器卸载或切换集群时取消旧配置读取，避免旧响应恢复已放弃的草稿。 */ () => () => { void client.cancelQueries({ queryKey: ['feishu-alerts', cluster.id], exact: true }) }, [client, cluster.id])
  // 在配置会话提供可用或空快照时渲染当前飞书表单，保持已加载配置源稳定，避免摘要刷新抹掉草稿。
  const renderForm = (currentSnapshot: ClusterSnapshot | null) => source && <FeishuAlertForm clusterId={cluster.id}
    value={source} snapshot={currentSnapshot} onClose={onClose} onSaved={/* 在飞书配置保存成功时同步摘要缓存并重新校验，不重置正在编辑的表单。 */ value => {
      client.setQueryData(['feishu-alerts', cluster.id], value)
      void client.invalidateQueries({ queryKey: ['feishu-alerts', cluster.id], exact: true })
    }} />
  // 编辑器只复用新鲜的同集群有效采样，否则由临时会话重新发现选项，避免跨集群规则选择。
  const reusable = snapshot?.clusterId === cluster.id && !expired && Number.isFinite(Date.parse(snapshot.collectedAt)) && !snapshotUnavailable(snapshot)
  return <section aria-label={`${cluster.name} 飞书告警`}>
    <p className="rules-description">{cluster.name} · 独立配置 Token 与 Consumer Group + Topic 告警规则。</p>
    {!source && !settings.isError && <p role="status">正在加载飞书告警配置…</p>}
    {settings.isError && !source && <><p role="alert">加载飞书告警配置失败，请重试。</p>
      <button type="button" onClick={/* 在读取失败后点击重试时重新请求当前集群飞书设置。 */ () => { void settings.refetch() }}>重新加载</button></>}
    {source && <ConfigurationSession clusterId={cluster.id} snapshot={reusable ? snapshot : null} attempt={attempt}
      onReconnect={/* 在用户请求重连时递增尝试标识，触发旧会话清理与新采样。 */ () => setAttempt(value => value + 1)}>{renderForm}</ConfigurationSession>}
  </section>
}

// 在配置弹窗缺少可复用快照时按需创建短租约会话，随可见性和卸载停止采样并释放连接。
function ConfigurationSession({ clusterId, snapshot = null, attempt = 0, onReconnect, children }: {
  clusterId: string
  snapshot?: ClusterSnapshot | null
  attempt?: number
  onReconnect: () => void
  children: (snapshot: ClusterSnapshot | null) => ReactNode
}) {
  const [ownSnapshot, setOwnSnapshot] = useState<ClusterSnapshot | null>(null)
  const [connectionError, setConnectionError] = useState<unknown>(null)
  const [connecting, setConnecting] = useState(true)
  const [visible, setVisible] = useState(() => document.visibilityState === 'visible')
  const [pageActive, setPageActive] = useState(true)
  const [sharedSnapshot, setSharedSnapshot] = useState(snapshot)
  if (snapshot !== sharedSnapshot) {
    setSharedSnapshot(snapshot)
    setOwnSnapshot(null)
    setConnectionError(null)
    setConnecting(true)
  }
  const stopSession = useRef(() => {})
  useEffect(/* 在配置区域挂载时订阅可见性和页面离开事件，卸载时移除监听避免重复建连。 */ () => {
    // 在页面隐藏时清空仅用于下拉选择的快照并重置连接反馈。
    const pause = () => { setOwnSnapshot(null); setConnectionError(null); setConnecting(true) }
    // 在页面可见性变化时暂停配置采样，恢复后由会话副作用重新连接。
    const visibility = () => {
      const next = document.visibilityState === 'visible'
      if (!next) pause()
      setVisible(next)
    }
    // 在页面离开或进入缓存时立即停止会话并撤销旧快照选项。
    const hide = () => { stopSession.current(); pause(); setPageActive(false) }
    // 在页面重新展示时激活配置会话生命周期。
    const show = () => setPageActive(true)
    document.addEventListener('visibilitychange', visibility)
    window.addEventListener('pagehide', hide)
    window.addEventListener('pageshow', show)
    return () => {
      document.removeEventListener('visibilitychange', visibility)
      window.removeEventListener('pagehide', hide)
      window.removeEventListener('pageshow', show)
    }
  }, [])
  useEffect(/* 在可见且缺少共享快照时串行创建与采样，依赖变化时先取消请求、定时器并释放旧会话。 */ () => {
    if (!visible || !pageActive || snapshot) return
    let disposed = false
    let sessionId: string | undefined
    let timer: ReturnType<typeof setTimeout> | undefined
    const controller = new AbortController()
    // 在采样失败、停止或卸载时串行释放已创建会话，避免遗留 Kafka 客户端。
    const close = () => {
      if (sessionId) {
        const id = sessionId
        sessionId = undefined
        void sessionTransition(() => sessionsApi.close(id)).catch(() => {})
      }
    }
    // 在可见配置页缺少共享快照时串行创建会话，晚到的创建结果也必须释放。
    async function connect() {
      try {
        // Let creation finish so a session returned after unmount can be released.
        // 创建与释放共用串行边界；不取消创建响应，才能接住卸载后返回的会话并显式关闭。
        const session = await sessionTransition(async () => {
          if (disposed) return null
          const created = await sessionsApi.create(clusterId)
          if (disposed) { await sessionsApi.close(created.sessionId).catch(() => {}); return null }
          sessionId = created.sessionId
          return created
        })
        if (!session) return
        if (disposed) { close(); return }
        // 在临时会话建立后读取快照并按租约续采，发现完全失败时关闭会话。
        const refresh = async () => {
          try {
            const next = await sessionsApi.snapshot(session.sessionId, controller.signal)
            if (disposed) return
            setOwnSnapshot(next)
            setConnecting(false)
            if (snapshotUnavailable(next)) { close(); return }
            timer = setTimeout(refresh, Math.max(1000, Math.min(10000, session.leaseSeconds * 500)))
          } catch (error) {
            if (disposed) return
            setOwnSnapshot(null)
            setConnectionError(error)
            setConnecting(false)
            close()
          }
        }
        await refresh()
      } catch (error) {
        if (!disposed) { setConnectionError(error); setConnecting(false) }
      }
    }
    // 在隐藏、切换或卸载时先标记失效，再取消请求与定时器并释放会话。
    // 先使异步回调失效，再中断采样与续租，最后排队释放，避免关闭后继续使用旧会话。
    const stop = () => { disposed = true; controller.abort(); clearTimeout(timer); close() }
    stopSession.current = stop
    void connect()
    return () => { stop(); if (stopSession.current === stop) stopSession.current = () => {} }
  }, [clusterId, visible, pageActive, snapshot, attempt])
  return <>
    <ApiErrorNotice error={connectionError} />
    {(!visible || !pageActive) ? <p role="status">已暂停（页面不可见）</p> : !snapshot && connecting && <p role="status">正在连接集群…</p>}
    {!snapshot && (!ownSnapshot || snapshotUnavailable(ownSnapshot)) && <button type="button" disabled={connecting} onClick={/* 在配置重连按钮点击时先丢弃失效快照和旧错误，再触发新的会话尝试。 */ () => {
      setOwnSnapshot(null); setConnectionError(null); setConnecting(true); onReconnect()
    }}>重新连接</button>}
    {children(visible && pageActive ? snapshot ?? ownSnapshot : null)}
  </>
}

// 在 Lag 规则弹窗打开时读取规则、选择监控或临时快照并保存规则草稿。
function RulesEditor({ cluster, snapshot, onClose }: { cluster: ClusterDto; snapshot: ClusterSnapshot | null; onClose: () => void }) {
  const fieldErrorIdPrefix = useId()
  const client = useQueryClient()
  const rules = useQuery({ queryKey: ['lag-rules', cluster.id], queryFn: /* 在当前资源查询启动时传递取消信号读取绑定集群的数据，使查询取消能传递到 HTTP 请求。 */ ({ signal }) => lagRulesApi.get(cluster.id, signal) })
  const [draft, setDraft] = useState<LagRuleSet | null>(null)
  const [attempt, setAttempt] = useState(0)
  const save = useMutation({ mutationKey: ['cluster-settings'], mutationFn: /* 在用户提交当前配置或测试操作时调用对应集群 API，让查询层统一管理在途状态。 */ (value: LagRuleSet) => lagRulesApi.save(cluster.id, value), onSuccess: /* 在当前写入成功后先同步关联缓存，再使摘要查询失效，保证后续编辑使用最新配置。 */ async data => {
    setDraft(data)
    client.setQueryData(['lag-rules', cluster.id], data)
    client.setQueryData<ClusterDto[]>(['clusters'], previous => previous?.map(item => item.id === cluster.id ? {
      ...item, defaultNormalLagMax: data.defaults.normalMax, defaultHighLagMin: data.defaults.highMin,
      defaultOverLimitSeconds: data.defaults.overLimitSeconds,
    } : item))
    await client.invalidateQueries({ queryKey: ['clusters'] })
  } })
  // 在配置会话提供可用或空快照时渲染当前规则表单，保持数据来源与会话一致。
  const renderForm = (currentSnapshot: ClusterSnapshot | null) => rules.data && <LagRulesForm clusterId={cluster.id}
    snapshot={currentSnapshot} value={draft ?? rules.data} onChange={setDraft}
    fieldErrors={save.error instanceof ApiError ? save.error.fields : {}} fieldErrorIdPrefix={fieldErrorIdPrefix}
    onSave={/* 在请求保存时，提交当前受控草稿，由父级统一管理保存与错误反馈。 */ value => save.mutate(value)} saving={save.isPending} />
  return <section aria-label={`${cluster.name} Lag 规则`}>
    <p className="rules-description">{cluster.name} · 自定义规则精确匹配 Consumer Group + Topic，覆盖集群默认区间与持续时间。</p>
    {rules.isPending && <p role="status">正在加载规则…</p>}
    <ApiErrorNotice error={save.error ?? rules.error} fieldIdPrefix={fieldErrorIdPrefix} />
    {snapshot && (attempt === 0 || !snapshotUnavailable(snapshot)) ? <>
      {renderForm(snapshot)}
      {snapshotUnavailable(snapshot) && <button type="button" onClick={/* 在用户请求重连时递增尝试标识，触发旧会话清理与新采样。 */ () => setAttempt(value => value + 1)}>重新连接</button>}
    </> : <ConfigurationSession key={attempt} clusterId={cluster.id}
      onReconnect={/* 在用户请求重连时递增尝试标识，触发旧会话清理与新采样。 */ () => setAttempt(value => value + 1)}>{renderForm}</ConfigurationSession>}
    {save.isSuccess && <p role="status">规则已保存</p>}
    <button type="button" disabled={save.isPending} onClick={onClose}>关闭规则设置</button>
  </section>
}

export interface ClustersPageProps { snapshot?: ClusterSnapshot | null }

// 在集群设置路由渲染时协调集群增删改查、连接测试及互斥的配置弹窗。
export function ClustersPage({ snapshot = null }: ClustersPageProps) {
  const client = useQueryClient()
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: /* 在当前资源查询启动时传递取消信号读取绑定集群的数据，使查询取消能传递到 HTTP 请求。 */ ({ signal }) => clustersApi.list(signal) })
  const savingSettings = useIsMutating({ mutationKey: ['cluster-settings'] }) > 0
  const [editing, setEditing] = useState<ClusterDto | 'new' | null>(null)
  const [rulesClusterId, setRulesClusterId] = useState<string | null>(null)
  const rulesCluster = clusters.data?.find(cluster => cluster.id === rulesClusterId)
  const [feishuClusterId, setFeishuClusterId] = useState<string | null>(null)
  const feishuCluster = clusters.data?.find(cluster => cluster.id === feishuClusterId)
  const [testedId, setTestedId] = useState('')
  const save = useMutation({ mutationKey: ['cluster-settings'], mutationFn: /* 在用户提交当前配置或测试操作时调用对应集群 API，让查询层统一管理在途状态。 */ ({ id, value }: { id?: string; value: ClusterInput }) => id ? clustersApi.update(id, value) : clustersApi.create(value),
    onSuccess: /* 在当前写入成功后先同步关联缓存，再使摘要查询失效，保证后续编辑使用最新配置。 */ async saved => {
      client.setQueryData<ClusterDto[]>(['clusters'], previous => previous?.some(cluster => cluster.id === saved.id)
        ? previous.map(cluster => cluster.id === saved.id ? saved : cluster) : [...previous ?? [], saved])
      client.setQueryData<LagRuleSet>(['lag-rules', saved.id], previous => previous ? { ...previous, defaults: {
        normalMax: saved.defaultNormalLagMax, highMin: saved.defaultHighLagMin, overLimitSeconds: saved.defaultOverLimitSeconds,
      } } : undefined)
      await Promise.all([
        client.invalidateQueries({ queryKey: ['clusters'] }),
        client.invalidateQueries({ queryKey: ['lag-rules', saved.id] }),
      ])
      setEditing(null)
    } })
  const remove = useMutation({ mutationFn: clustersApi.delete, onSuccess: /* 在集群删除成功后关闭指向它的编辑器并刷新集群列表，触发临时配置会话卸载释放。 */ async (_, id) => {
    if (rulesClusterId === id) setRulesClusterId(null)
    if (feishuClusterId === id) setFeishuClusterId(null)
    if (editing !== 'new' && editing?.id === id) setEditing(null)
    await client.invalidateQueries({ queryKey: ['clusters'] })
  } })
  const test = useMutation({ mutationFn: /* 在用户提交当前配置或测试操作时调用对应集群 API，让查询层统一管理在途状态。 */ (id: string) => clustersApi.test(id) })

  // 在点击新增集群时清除旧保存反馈并关闭其他编辑器。
  const addCluster = () => { save.reset(); setRulesClusterId(null); setFeishuClusterId(null); setEditing('new') }
  return <><header className="topbar settings-topbar"><span>Kafka Lens <span className="breadcrumb-separator">/</span> 集群设置</span><span className="muted">连接与 Lag 状态规则</span></header>
    <section className="page-content settings-page" aria-label="集群设置">
    <div className="page-head"><div><h1>集群设置</h1><p>管理 Kafka 连接与各集群的 Lag 状态规则</p></div>
    <button className="primary" type="button" disabled={savingSettings} onClick={addCluster}>新增集群</button></div>
    {clusters.isPending && <p role="status">正在加载集群…</p>}
    <ApiErrorNotice error={clusters.error ?? remove.error ?? test.error} />
    {clusters.data?.length === 0 && <p>尚未配置集群，请新增集群。</p>}
    <div className="settings-grid">{clusters.data?.map(cluster => <fieldset className="cluster-card" key={cluster.id} aria-label={cluster.name}>
      <div className="cluster-card-top"><h3>{cluster.name}</h3><span className="protocol-badge">{cluster.securityProtocol}</span></div>
      <div className="cluster-meta"><p>{cluster.bootstrapServers}</p><p>{cluster.saslMechanism || 'Kafka 集群连接'}</p></div>
      <div className="threshold-summary">默认 Lag 阈值：正常 ≤ {cluster.defaultNormalLagMax.toLocaleString('en-US')} · 高 Lag ≥ {cluster.defaultHighLagMin.toLocaleString('en-US')}<br />持续超限 {cluster.defaultOverLimitSeconds} 秒</div>
      <FeishuAlertSummary clusterId={cluster.id} />
      <div className="cluster-card-actions">
      <button type="button" disabled={savingSettings} onClick={/* 在编辑按钮点击时清除旧保存反馈并关闭其他配置弹窗，隔离不同草稿。 */ () => { save.reset(); setRulesClusterId(null); setFeishuClusterId(null); setEditing(cluster) }}>编辑</button>
      <button type="button" disabled={test.isPending} onClick={/* 在测试按钮点击时记录集群标识并提交连接测试，以便反馈关联到正确卡片。 */ () => { setTestedId(cluster.id); test.mutate(cluster.id) }}>测试连接</button>
      <button type="button" disabled={savingSettings} onClick={/* 在按钮点击时，清除当前编辑目标，触发表单与会话的卸载清理。 */ () => { setEditing(null); setFeishuClusterId(null); setRulesClusterId(cluster.id) }}>配置 Lag 规则</button>
      <button type="button" disabled={savingSettings} onClick={/* 在按钮点击时，清除当前编辑目标，触发表单与会话的卸载清理。 */ () => { setEditing(null); setRulesClusterId(null); setFeishuClusterId(cluster.id) }}>配置飞书告警</button>
      <button className="danger-button" type="button" disabled={remove.isPending || savingSettings} onClick={/* 在删除集群按钮点击时提交当前标识，由成功回调清理关联缓存和编辑器。 */ () => remove.mutate(cluster.id)}>删除</button></div>
      {test.isSuccess && testedId === cluster.id && <p role="status">连接成功：{test.data.brokerCount} Broker，{test.data.topicCount} Topic，耗时 {test.data.elapsedMs} ms</p>}
    </fieldset>)}<button className="empty-add" type="button" disabled={savingSettings} onClick={addCluster}><span aria-hidden="true">＋</span>添加 Kafka 集群</button></div>
    {editing && <Modal title="Kafka 集群配置" description="配置连接信息与默认 Lag 状态判断区间" busy={savingSettings} onClose={/* 在关闭弹窗时，清除当前编辑目标，触发表单与会话的卸载清理。 */ () => setEditing(null)}><ClusterForm key={editing === 'new' ? 'new' : editing.id} cluster={editing === 'new' ? undefined : editing}
      saving={save.isPending} error={save.error} onCancel={/* 在取消编辑时，清除当前编辑目标，触发表单与会话的卸载清理。 */ () => setEditing(null)}
      onTest={/* 在测试草稿时提交当前输入并关联原集群，留空凭据由服务端按已有配置处理。 */ value => clustersApi.testDraft(value, editing === 'new' ? undefined : editing.id)}
      onSave={/* 在请求保存时，提交当前受控草稿，由父级统一管理保存与错误反馈。 */ value => save.mutate({ id: editing === 'new' ? undefined : editing.id, value })} /></Modal>}
    {rulesCluster && <Modal title="Lag 状态判断区间" wide busy={savingSettings} onClose={/* 在规则弹窗关闭时清除目标并卸载临时会话，避免配置页持续连接。 */ () => setRulesClusterId(null)}><RulesEditor key={rulesCluster.id} cluster={rulesCluster}
      snapshot={snapshot?.clusterId === rulesCluster.id ? snapshot : null} onClose={/* 在规则弹窗关闭时清除目标并卸载临时会话，避免配置页持续连接。 */ () => setRulesClusterId(null)} /></Modal>}
    {feishuCluster && <Modal title="配置飞书告警" wide onClose={/* 在飞书弹窗关闭时卸载编辑器，丢弃 Token 草稿并取消配置读取。 */ () => setFeishuClusterId(null)}><FeishuAlertEditor key={feishuCluster.id} cluster={feishuCluster}
      snapshot={snapshot?.clusterId === feishuCluster.id ? snapshot : null} onClose={/* 在飞书弹窗关闭时卸载编辑器，丢弃 Token 草稿并取消配置读取。 */ () => setFeishuClusterId(null)} /></Modal>}
  </section></>
}
