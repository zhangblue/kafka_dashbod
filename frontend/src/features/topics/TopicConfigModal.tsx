import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { ApiError, topicsApi } from '../../api/client'
import type { TopicConfigSource, TopicConfigValue, TopicMutationResult, UpdateTopicConfigsInput } from '../../api/types'
import { Modal } from '../../components/Modal'
import { topicConfigDefinitions, validateTopicConfigValue, type TopicConfigDefinition } from './topicConfig'

const errorMessages: Record<string, string> = {
  TOPIC_ALREADY_EXISTS: '同名 Topic 已存在。',
  TOPIC_NOT_FOUND: 'Topic 已不存在，请刷新列表。',
  TOPIC_AUTHORIZATION_FAILED: 'Kafka 账户无权执行此 Topic 操作。',
  TOPIC_OPERATION_TIMEOUT: 'Topic 操作超时，请重新读取集群状态后确认。',
  INVALID_TOPIC_CONFIG: 'Kafka 拒绝了此配置，请检查参数与集群限制。',
  TOPIC_CONFIG_CHANGED: '配置已被其他客户端修改，请重新读取。',
  INTERNAL_TOPIC_PROTECTED: '禁止删除 Kafka 内部 Topic。',
  SESSION_EXPIRED: '会话已过期，请重新连接集群。',
  SNAPSHOT_IN_PROGRESS: '当前会话正在采集或执行管理操作，请稍后重试。',
  SHUTTING_DOWN: '服务正在关闭，请稍后重试。',
  KAFKA_UNAVAILABLE: 'Kafka 暂时不可用，请检查连接后重试。',
  VALIDATION_ERROR: '请检查表单中的配置参数。',
}

export function TopicAdminErrorNotice({ error }: { error: unknown }) {
  if (!error) return null
  return <div role="alert"><p>{error instanceof ApiError
    ? errorMessages[error.code] ?? error.message : '请求失败，请稍后重试。'}</p>
    {error instanceof ApiError && error.requestId && <p>Request ID: {error.requestId}</p>}
    {error instanceof ApiError && Object.entries(error.fields).filter(([key]) =>
      !topicConfigDefinitions.some(item => key === item.key || key === `configs.${item.key}` || key === `changes.${item.key}`)
      && !['name', 'partitions', 'replicationFactor'].includes(key)).map(([key, message]) => <p key={key}>{message}</p>)}
  </div>
}

export function TopicConfigField({ definition, value, onChange, disabled, error, source, reset }: {
  definition: TopicConfigDefinition; value: string; onChange: (value: string) => void
  disabled: boolean; error?: string; source?: TopicConfigSource
  reset?: { pending: boolean; disabled: boolean; onClick: () => void }
}) {
  const id = useId()
  const sourceLabels = { topicOverride: 'Topic 覆盖值', brokerDefault: 'Broker 默认值', unknown: '来源未知' }
  return <article className="topic-config-card" aria-label={`${definition.label} 配置项`}>
    <div className="topic-config-top"><div><div className="topic-config-title">
      <h3 id={`${id}-title`}><label htmlFor={id}>{definition.label}</label></h3>
      {source && <span className={`topic-source topic-source-${source}`}>{sourceLabels[source]}</span>}
    </div><div className="topic-config-key">{definition.key}</div></div>
      {reset && <button type="button" className="topic-reset" disabled={reset.disabled}
        aria-label={`${reset.pending ? '撤销恢复' : '恢复'} ${definition.label} 为 Broker 默认值`} onClick={reset.onClick}>
        {reset.pending ? '撤销恢复' : source === 'brokerDefault' ? '已使用默认' : '恢复默认'}</button>}
    </div>
    <p className="topic-config-description" id={`${id}-description`}>{definition.description}</p>
    <div className="topic-config-control">
      {definition.inputType === 'select' ? <select id={id} value={value} disabled={disabled}
        aria-invalid={!!error} aria-describedby={`${id}-description ${id}-risk${error ? ` ${id}-error` : ''}`}
        onChange={event => onChange(event.target.value)}>
        <option value="">{reset ? '当前值不可用' : '使用集群默认值'}</option>
        {value && !definition.options?.some(option => option.value === value) && <option value={value}>{value}（当前值）</option>}
        {definition.options?.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
      </select> : <input id={id} type="number" step="1" value={value} disabled={disabled}
        placeholder={reset ? '当前值不可用' : '留空使用集群默认值'}
        aria-invalid={!!error} aria-describedby={`${id}-description ${id}-risk${error ? ` ${id}-error` : ''}`}
        onChange={event => onChange(event.target.value)} />}
      {definition.unit && <span>{definition.unit}</span>}
    </div>
    <p className="topic-config-risk" id={`${id}-risk`}>{definition.risk}</p>
    {reset?.pending && <p className="topic-reset-pending">保存后继承 Broker 默认值；当前默认值将在保存后重新读取。</p>}
    {error && <p className="topic-field-error" role="alert" id={`${id}-error`}>{error}</p>}
  </article>
}

export interface TopicConfigModalProps {
  sessionId: string
  topic: string
  onClose: () => void
  onApplied: (result?: TopicMutationResult) => void | Promise<void>
}

export function TopicConfigModal(props: TopicConfigModalProps) {
  return <TopicConfigEditor key={JSON.stringify([props.sessionId, props.topic])} {...props} />
}

function TopicConfigEditor({ sessionId, topic, onClose, onApplied }: TopicConfigModalProps) {
  const [configs, setConfigs] = useState<TopicConfigValue[] | null>(null)
  const [draft, setDraft] = useState<Record<string, string | null>>({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [loadError, setLoadError] = useState<unknown>(null)
  const [saveError, setSaveError] = useState<unknown>(null)
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({})
  const [notice, setNotice] = useState('')
  const request = useRef<AbortController | null>(null)
  const active = useRef(true)
  const submitting = useRef(false)
  const read = useCallback(() => {
    request.current?.abort()
    const controller = new AbortController()
    request.current = controller
    return topicsApi.configs(sessionId, topic, controller.signal).then(result => {
      if (controller.signal.aborted) return false
      setConfigs(result.configs)
      return true
    }).catch(error => {
      if (!controller.signal.aborted) setLoadError(error)
      return false
    }).finally(() => {
      if (!controller.signal.aborted) setLoading(false)
    })
  }, [sessionId, topic])
  const prepareRead = () => {
    setLoading(true); setConfigs(null); setDraft({}); setLoadError(null); setSaveError(null); setFieldErrors({})
  }
  useEffect(() => {
    active.current = true
    void read()
    return () => { active.current = false; request.current?.abort() }
  }, [read])
  const changed = Object.keys(draft).length > 0
  const close = () => {
    if (!saving && (!changed || window.confirm('有未保存的 Topic 配置修改，确定放弃吗？'))) onClose()
  }
  const edit = (entry: TopicConfigValue, value: string | null) => {
    setSaveError(null); setFieldErrors({}); setNotice('')
    setDraft(previous => {
      const next = { ...previous }
      if (value === entry.value) delete next[entry.key]
      else next[entry.key] = value
      return next
    })
  }
  const save = async () => {
    if (submitting.current || !configs || !changed || loading) return
    const errors: Record<string, string> = {}
    for (const [key, value] of Object.entries(draft)) {
      const error = value === null ? undefined : validateTopicConfigValue(key, value)
      if (error) errors[key] = error
    }
    setFieldErrors(errors)
    if (Object.keys(errors).length) return
    const input: UpdateTopicConfigsInput = { changes: { ...draft }, expectedValues: {} }
    for (const key of Object.keys(draft)) input.expectedValues[key] = configs.find(item => item.key === key)!.value
    submitting.current = true; setSaving(true); setSaveError(null); setNotice('')
    try {
      let result
      try { result = await topicsApi.update(sessionId, topic, input) }
      catch (error) { if (active.current) setSaveError(error); return }
      if (!active.current) return
      let unconfirmed = 'status' in result
      if (unconfirmed) { setConfigs(null); setDraft({}) }
      else { prepareRead(); unconfirmed = !await read() }
      if (!active.current) return
      setNotice(unconfirmed ? '已提交但暂时无法确认当前值' : '配置已保存，当前值已从 Kafka 重新读取。')
      try { await onApplied(unconfirmed ? { status: 'appliedButUnconfirmed' } : undefined) }
      catch { if (active.current) setNotice('配置已提交，列表刷新失败，请稍后手动刷新。') }
    } finally {
      submitting.current = false
      if (active.current) setSaving(false)
    }
  }
  return <Modal title="Topic 参数配置" description={`${topic} · 修改后直接应用到 Kafka，无需重启 Broker`}
    onClose={close} busy={saving} wide>
    <form className="topic-admin-form" noValidate onSubmit={event => { event.preventDefault(); void save() }}>
      <div className="topic-config-notice"><b>修改前请确认影响。</b> 缩短保留时间或降低保留空间，可能使已有日志段在下一次清理时被删除；可靠性参数可能影响生产者写入。</div>
      <div className="topic-config-legend"><span>参数来源：</span><span className="topic-source topic-source-topicOverride">Topic 覆盖值</span><span className="topic-source topic-source-brokerDefault">Broker 默认值</span></div>
      {loading && <div className="topic-config-loading" role="status"><span>正在读取 Kafka 当前配置…</span>
        <div className="topic-config-grid" aria-hidden="true">{Array.from({ length: 8 }, (_, index) => <div key={index} />)}</div></div>}
      <TopicAdminErrorNotice error={loadError ?? saveError} />
      {notice && <p className="topic-config-feedback" role="status">{notice}</p>}
      {!loading && (!configs || !!loadError || (saveError instanceof ApiError && saveError.code === 'TOPIC_CONFIG_CHANGED')) &&
        <button type="button" disabled={saving} onClick={() => {
          if (!changed || window.confirm('重新读取会放弃未保存的修改，确定继续吗？')) {
            setNotice(''); prepareRead(); void read()
          }
        }}>重新读取</button>}
      {configs && <div className="topic-config-grid">{topicConfigDefinitions.map(definition => {
        const entry = configs.find(item => item.key === definition.key)
        const pending = draft[definition.key] === null
        const unavailable = !entry || entry.value === null
        const error = fieldErrors[definition.key] ?? (saveError instanceof ApiError
          ? saveError.fields[`changes.${definition.key}`] ?? saveError.fields[definition.key] : undefined)
        return <TopicConfigField key={definition.key} definition={definition}
          source={entry?.source ?? 'unknown'} value={pending ? '' : draft[definition.key] ?? entry?.value ?? ''}
          disabled={saving || unavailable || !!entry?.readOnly || pending} error={error}
          onChange={value => { if (entry) edit(entry, value) }}
          reset={{ pending, disabled: saving || unavailable || !!entry?.readOnly || entry?.source !== 'topicOverride',
            onClick: () => { if (entry) {
              if (pending) setDraft(previous => { const next = { ...previous }; delete next[entry.key]; return next })
              else edit(entry, null)
            } } }} />
      })}</div>}
      {configs?.some(entry => entry.readOnly) && <p className="form-note">只读参数受集群策略限制，无法修改或恢复默认；未返回的参数显示为不可用。</p>}
      <div className="modal-actions topic-admin-actions"><small>保存时仅提交已修改的参数；恢复默认会删除 Topic 级覆盖值。</small>
        <div><button type="button" disabled={saving} onClick={close}>取消</button>
          <button type="submit" disabled={saving || loading || !configs || !changed}>{saving ? '保存中…' : '保存配置'}</button></div>
      </div>
    </form>
  </Modal>
}
