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

function feishuSettingsQuery(clusterId: string) {
  return { queryKey: ['feishu-alerts', clusterId], queryFn: ({ signal }: { signal: AbortSignal }) => feishuAlertsApi.get(clusterId, signal) }
}

function FeishuAlertSummary({ clusterId }: { clusterId: string }) {
  const settings = useQuery(feishuSettingsQuery(clusterId))
  return <div className="threshold-summary">飞书告警：{settings.isError ? '摘要加载失败' : !settings.data ? '正在加载…'
    : `${settings.data.accessTokenConfigured ? 'Token 已配置' : '未配置'}${settings.data.accessTokenConfigured || settings.data.rules.length ? ` · ${settings.data.rules.length} 条 Topic + Group 规则` : ''}`}</div>
}

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
  useEffect(() => () => { void client.cancelQueries({ queryKey: ['feishu-alerts', cluster.id], exact: true }) }, [client, cluster.id])
  const renderForm = (currentSnapshot: ClusterSnapshot | null) => source && <FeishuAlertForm clusterId={cluster.id}
    value={source} snapshot={currentSnapshot} onClose={onClose} onSaved={value => {
      client.setQueryData(['feishu-alerts', cluster.id], value)
      void client.invalidateQueries({ queryKey: ['feishu-alerts', cluster.id], exact: true })
    }} />
  const reusable = snapshot?.clusterId === cluster.id && !expired && Number.isFinite(Date.parse(snapshot.collectedAt)) && !snapshotUnavailable(snapshot)
  return <section aria-label={`${cluster.name} 飞书告警`}>
    <p className="rules-description">{cluster.name} · 独立配置 Token 与 Consumer Group + Topic 告警规则。</p>
    {!source && !settings.isError && <p role="status">正在加载飞书告警配置…</p>}
    {settings.isError && !source && <><p role="alert">加载飞书告警配置失败，请重试。</p>
      <button type="button" onClick={() => { void settings.refetch() }}>重新加载</button></>}
    {source && <ConfigurationSession clusterId={cluster.id} snapshot={reusable ? snapshot : null} attempt={attempt}
      onReconnect={() => setAttempt(value => value + 1)}>{renderForm}</ConfigurationSession>}
  </section>
}

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
  useEffect(() => {
    const pause = () => { setOwnSnapshot(null); setConnectionError(null); setConnecting(true) }
    const visibility = () => {
      const next = document.visibilityState === 'visible'
      if (!next) pause()
      setVisible(next)
    }
    const hide = () => { stopSession.current(); pause(); setPageActive(false) }
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
  useEffect(() => {
    if (!visible || !pageActive || snapshot) return
    let disposed = false
    let sessionId: string | undefined
    let timer: ReturnType<typeof setTimeout> | undefined
    const controller = new AbortController()
    const close = () => {
      if (sessionId) {
        const id = sessionId
        sessionId = undefined
        void sessionTransition(() => sessionsApi.close(id)).catch(() => {})
      }
    }
    async function connect() {
      try {
        // Let creation finish so a session returned after unmount can be released.
        const session = await sessionTransition(async () => {
          if (disposed) return null
          const created = await sessionsApi.create(clusterId)
          if (disposed) { await sessionsApi.close(created.sessionId).catch(() => {}); return null }
          sessionId = created.sessionId
          return created
        })
        if (!session) return
        if (disposed) { close(); return }
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
    const stop = () => { disposed = true; controller.abort(); clearTimeout(timer); close() }
    stopSession.current = stop
    void connect()
    return () => { stop(); if (stopSession.current === stop) stopSession.current = () => {} }
  }, [clusterId, visible, pageActive, snapshot, attempt])
  return <>
    <ApiErrorNotice error={connectionError} />
    {(!visible || !pageActive) ? <p role="status">已暂停（页面不可见）</p> : !snapshot && connecting && <p role="status">正在连接集群…</p>}
    {!snapshot && (!ownSnapshot || snapshotUnavailable(ownSnapshot)) && <button type="button" disabled={connecting} onClick={() => {
      setOwnSnapshot(null); setConnectionError(null); setConnecting(true); onReconnect()
    }}>重新连接</button>}
    {children(visible && pageActive ? snapshot ?? ownSnapshot : null)}
  </>
}

function RulesEditor({ cluster, snapshot, onClose }: { cluster: ClusterDto; snapshot: ClusterSnapshot | null; onClose: () => void }) {
  const fieldErrorIdPrefix = useId()
  const client = useQueryClient()
  const rules = useQuery({ queryKey: ['lag-rules', cluster.id], queryFn: ({ signal }) => lagRulesApi.get(cluster.id, signal) })
  const [draft, setDraft] = useState<LagRuleSet | null>(null)
  const [attempt, setAttempt] = useState(0)
  const save = useMutation({ mutationKey: ['cluster-settings'], mutationFn: (value: LagRuleSet) => lagRulesApi.save(cluster.id, value), onSuccess: async data => {
    setDraft(data)
    client.setQueryData(['lag-rules', cluster.id], data)
    client.setQueryData<ClusterDto[]>(['clusters'], previous => previous?.map(item => item.id === cluster.id ? {
      ...item, defaultNormalLagMax: data.defaults.normalMax, defaultHighLagMin: data.defaults.highMin,
      defaultOverLimitSeconds: data.defaults.overLimitSeconds,
    } : item))
    await client.invalidateQueries({ queryKey: ['clusters'] })
  } })
  const renderForm = (currentSnapshot: ClusterSnapshot | null) => rules.data && <LagRulesForm clusterId={cluster.id}
    snapshot={currentSnapshot} value={draft ?? rules.data} onChange={setDraft}
    fieldErrors={save.error instanceof ApiError ? save.error.fields : {}} fieldErrorIdPrefix={fieldErrorIdPrefix}
    onSave={value => save.mutate(value)} saving={save.isPending} />
  return <section aria-label={`${cluster.name} Lag 规则`}>
    <p className="rules-description">{cluster.name} · 自定义规则精确匹配 Consumer Group + Topic，覆盖集群默认区间与持续时间。</p>
    {rules.isPending && <p role="status">正在加载规则…</p>}
    <ApiErrorNotice error={save.error ?? rules.error} fieldIdPrefix={fieldErrorIdPrefix} />
    {snapshot && (attempt === 0 || !snapshotUnavailable(snapshot)) ? <>
      {renderForm(snapshot)}
      {snapshotUnavailable(snapshot) && <button type="button" onClick={() => setAttempt(value => value + 1)}>重新连接</button>}
    </> : <ConfigurationSession key={attempt} clusterId={cluster.id}
      onReconnect={() => setAttempt(value => value + 1)}>{renderForm}</ConfigurationSession>}
    {save.isSuccess && <p role="status">规则已保存</p>}
    <button type="button" disabled={save.isPending} onClick={onClose}>关闭规则设置</button>
  </section>
}

export interface ClustersPageProps { snapshot?: ClusterSnapshot | null }

export function ClustersPage({ snapshot = null }: ClustersPageProps) {
  const client = useQueryClient()
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => clustersApi.list(signal) })
  const savingSettings = useIsMutating({ mutationKey: ['cluster-settings'] }) > 0
  const [editing, setEditing] = useState<ClusterDto | 'new' | null>(null)
  const [rulesClusterId, setRulesClusterId] = useState<string | null>(null)
  const rulesCluster = clusters.data?.find(cluster => cluster.id === rulesClusterId)
  const [feishuClusterId, setFeishuClusterId] = useState<string | null>(null)
  const feishuCluster = clusters.data?.find(cluster => cluster.id === feishuClusterId)
  const [testedId, setTestedId] = useState('')
  const save = useMutation({ mutationKey: ['cluster-settings'], mutationFn: ({ id, value }: { id?: string; value: ClusterInput }) => id ? clustersApi.update(id, value) : clustersApi.create(value),
    onSuccess: async saved => {
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
  const remove = useMutation({ mutationFn: clustersApi.delete, onSuccess: async (_, id) => {
    if (rulesClusterId === id) setRulesClusterId(null)
    if (feishuClusterId === id) setFeishuClusterId(null)
    if (editing !== 'new' && editing?.id === id) setEditing(null)
    await client.invalidateQueries({ queryKey: ['clusters'] })
  } })
  const test = useMutation({ mutationFn: (id: string) => clustersApi.test(id) })

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
      <button type="button" disabled={savingSettings} onClick={() => { save.reset(); setRulesClusterId(null); setFeishuClusterId(null); setEditing(cluster) }}>编辑</button>
      <button type="button" disabled={test.isPending} onClick={() => { setTestedId(cluster.id); test.mutate(cluster.id) }}>测试连接</button>
      <button type="button" disabled={savingSettings} onClick={() => { setEditing(null); setFeishuClusterId(null); setRulesClusterId(cluster.id) }}>配置 Lag 规则</button>
      <button type="button" disabled={savingSettings} onClick={() => { setEditing(null); setRulesClusterId(null); setFeishuClusterId(cluster.id) }}>配置飞书告警</button>
      <button className="danger-button" type="button" disabled={remove.isPending || savingSettings} onClick={() => remove.mutate(cluster.id)}>删除</button></div>
      {test.isSuccess && testedId === cluster.id && <p role="status">连接成功：{test.data.brokerCount} Broker，{test.data.topicCount} Topic，耗时 {test.data.elapsedMs} ms</p>}
    </fieldset>)}<button className="empty-add" type="button" disabled={savingSettings} onClick={addCluster}><span aria-hidden="true">＋</span>添加 Kafka 集群</button></div>
    {editing && <Modal title="Kafka 集群配置" description="配置连接信息与默认 Lag 状态判断区间" busy={savingSettings} onClose={() => setEditing(null)}><ClusterForm key={editing === 'new' ? 'new' : editing.id} cluster={editing === 'new' ? undefined : editing}
      saving={save.isPending} error={save.error} onCancel={() => setEditing(null)}
      onTest={value => clustersApi.testDraft(value, editing === 'new' ? undefined : editing.id)}
      onSave={value => save.mutate({ id: editing === 'new' ? undefined : editing.id, value })} /></Modal>}
    {rulesCluster && <Modal title="Lag 状态判断区间" wide busy={savingSettings} onClose={() => setRulesClusterId(null)}><RulesEditor key={rulesCluster.id} cluster={rulesCluster}
      snapshot={snapshot?.clusterId === rulesCluster.id ? snapshot : null} onClose={() => setRulesClusterId(null)} /></Modal>}
    {feishuCluster && <Modal title="配置飞书告警" wide onClose={() => setFeishuClusterId(null)}><FeishuAlertEditor key={feishuCluster.id} cluster={feishuCluster}
      snapshot={snapshot?.clusterId === feishuCluster.id ? snapshot : null} onClose={() => setFeishuClusterId(null)} /></Modal>}
  </section></>
}
