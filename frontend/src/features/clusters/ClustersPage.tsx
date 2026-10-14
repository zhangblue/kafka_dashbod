import { useEffect, useState } from 'react'
import type { ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { clustersApi, lagRulesApi, sessionsApi } from '../../api/client'
import type { ClusterDto, ClusterInput, ClusterSnapshot, LagRuleSet } from '../../api/types'
import { ClusterForm, ApiErrorNotice } from './ClusterForm'
import { LagRulesForm } from '../lag-rules/LagRulesForm'

function ConfigurationSession({ clusterId, onReconnect, children }: {
  clusterId: string
  onReconnect: () => void
  children: (snapshot: ClusterSnapshot | null) => ReactNode
}) {
  const [ownSnapshot, setOwnSnapshot] = useState<ClusterSnapshot | null>(null)
  const [connectionError, setConnectionError] = useState<unknown>(null)
  const [connecting, setConnecting] = useState(true)
  useEffect(() => {
    let disposed = false
    let sessionId: string | undefined
    let timer: ReturnType<typeof setTimeout> | undefined
    const controller = new AbortController()
    const close = () => {
      if (sessionId) {
        const id = sessionId
        sessionId = undefined
        void sessionsApi.close(id).catch(() => {})
      }
    }
    async function connect() {
      try {
        // Let creation finish so a session returned after unmount can be released.
        const session = await sessionsApi.create(clusterId)
        sessionId = session.sessionId
        if (disposed) { close(); return }
        const refresh = async () => {
          try {
            const next = await sessionsApi.snapshot(session.sessionId, controller.signal)
            if (disposed) return
            setOwnSnapshot(next)
            setConnecting(false)
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
    void connect()
    return () => { disposed = true; controller.abort(); clearTimeout(timer); close() }
  }, [clusterId])
  return <>
    <ApiErrorNotice error={connectionError} />
    {connecting && <p role="status">正在连接集群…</p>}
    {!ownSnapshot && <button type="button" disabled={connecting} onClick={onReconnect}>重新连接</button>}
    {children(ownSnapshot)}
  </>
}

function RulesEditor({ cluster, snapshot, onClose }: { cluster: ClusterDto; snapshot: ClusterSnapshot | null; onClose: () => void }) {
  const client = useQueryClient()
  const rules = useQuery({ queryKey: ['lag-rules', cluster.id], queryFn: ({ signal }) => lagRulesApi.get(cluster.id, signal) })
  const [draft, setDraft] = useState<LagRuleSet | null>(null)
  const [attempt, setAttempt] = useState(0)
  const save = useMutation({ mutationFn: (value: LagRuleSet) => lagRulesApi.save(cluster.id, value), onSuccess: async data => {
    setDraft(data)
    client.setQueryData(['lag-rules', cluster.id], data)
    await client.invalidateQueries({ queryKey: ['clusters'] })
  } })
  const renderForm = (currentSnapshot: ClusterSnapshot | null) => rules.data && <LagRulesForm clusterId={cluster.id}
    snapshot={currentSnapshot} value={draft ?? rules.data} onChange={setDraft}
    onSave={value => save.mutate(value)} saving={save.isPending} />
  return <section aria-label={`${cluster.name} Lag 规则`}>
    <h2>{cluster.name} · Lag 状态判断区间</h2>
    {rules.isPending && <p role="status">正在加载规则…</p>}
    <ApiErrorNotice error={rules.error ?? save.error} />
    {snapshot ? renderForm(snapshot) : <ConfigurationSession key={attempt} clusterId={cluster.id}
      onReconnect={() => setAttempt(value => value + 1)}>{renderForm}</ConfigurationSession>}
    {save.isSuccess && <p role="status">规则已保存</p>}
    <button type="button" onClick={onClose}>关闭规则设置</button>
  </section>
}

export interface ClustersPageProps { snapshot?: ClusterSnapshot | null }

export function ClustersPage({ snapshot = null }: ClustersPageProps) {
  const client = useQueryClient()
  const clusters = useQuery({ queryKey: ['clusters'], queryFn: ({ signal }) => clustersApi.list(signal) })
  const [editing, setEditing] = useState<ClusterDto | 'new' | null>(null)
  const [rulesCluster, setRulesCluster] = useState<ClusterDto | null>(null)
  const [testedId, setTestedId] = useState('')
  const save = useMutation({ mutationFn: ({ id, value }: { id?: string; value: ClusterInput }) => id ? clustersApi.update(id, value) : clustersApi.create(value),
    onSuccess: async () => { setEditing(null); await client.invalidateQueries({ queryKey: ['clusters'] }) } })
  const remove = useMutation({ mutationFn: clustersApi.delete, onSuccess: async (_, id) => {
    if (rulesCluster?.id === id) setRulesCluster(null)
    if (editing !== 'new' && editing?.id === id) setEditing(null)
    await client.invalidateQueries({ queryKey: ['clusters'] })
  } })
  const test = useMutation({ mutationFn: (id: string) => clustersApi.test(id) })

  return <section aria-label="集群设置">
    <h1>集群设置</h1>
    <button type="button" onClick={() => { save.reset(); setEditing('new') }}>新增集群</button>
    {clusters.isPending && <p role="status">正在加载集群…</p>}
    <ApiErrorNotice error={clusters.error ?? remove.error ?? test.error} />
    {clusters.data?.length === 0 && <p>尚未配置集群，请新增集群。</p>}
    {clusters.data?.map(cluster => <fieldset key={cluster.id} aria-label={cluster.name}>
      <legend>{cluster.name}</legend><p>{cluster.bootstrapServers}</p><p>{cluster.securityProtocol}</p>
      <button type="button" onClick={() => { save.reset(); setEditing(cluster) }}>编辑</button>
      <button type="button" disabled={test.isPending} onClick={() => { setTestedId(cluster.id); test.mutate(cluster.id) }}>测试连接</button>
      <button type="button" onClick={() => setRulesCluster(cluster)}>配置 Lag 规则</button>
      <button type="button" disabled={remove.isPending} onClick={() => remove.mutate(cluster.id)}>删除</button>
      {test.isSuccess && testedId === cluster.id && <p role="status">连接成功：{test.data.brokerCount} Broker，{test.data.topicCount} Topic，耗时 {test.data.elapsedMs} ms</p>}
    </fieldset>)}
    {editing && <ClusterForm key={editing === 'new' ? 'new' : editing.id} cluster={editing === 'new' ? undefined : editing}
      saving={save.isPending} error={save.error} onCancel={() => setEditing(null)}
      onSave={value => save.mutate({ id: editing === 'new' ? undefined : editing.id, value })} />}
    {rulesCluster && <RulesEditor key={rulesCluster.id} cluster={rulesCluster}
      snapshot={snapshot?.clusterId === rulesCluster.id ? snapshot : null} onClose={() => setRulesCluster(null)} />}
  </section>
}
