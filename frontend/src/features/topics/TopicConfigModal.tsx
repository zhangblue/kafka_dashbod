import type { ParseKeys } from 'i18next'
import { useCallback, useEffect, useId, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { localizeApiError } from '../../api/errorMessages'
import { ApiError, topicsApi } from '../../api/client'
import type { TopicConfigSource, TopicConfigValue, TopicMutationResult, UpdateTopicConfigsInput } from '../../api/types'
import { Modal } from '../../components/Modal'
import { topicConfigDefinitions, validateTopicConfigValue, type TopicConfigDefinition } from './topicConfig'

const errorKeys = {
  TOPIC_ALREADY_EXISTS: 'topicAdmin.errors.TOPIC_ALREADY_EXISTS',
  TOPIC_NOT_FOUND: 'topicAdmin.errors.TOPIC_NOT_FOUND',
  TOPIC_AUTHORIZATION_FAILED: 'topicAdmin.errors.TOPIC_AUTHORIZATION_FAILED',
  TOPIC_OPERATION_TIMEOUT: 'topicAdmin.errors.TOPIC_OPERATION_TIMEOUT',
  INVALID_TOPIC_CONFIG: 'topicAdmin.errors.INVALID_TOPIC_CONFIG',
  TOPIC_CONFIG_CHANGED: 'topicAdmin.errors.TOPIC_CONFIG_CHANGED',
  INTERNAL_TOPIC_PROTECTED: 'topicAdmin.errors.INTERNAL_TOPIC_PROTECTED',
  SESSION_EXPIRED: 'topicAdmin.errors.SESSION_EXPIRED',
  SNAPSHOT_IN_PROGRESS: 'topicAdmin.errors.SNAPSHOT_IN_PROGRESS',
  SHUTTING_DOWN: 'topicAdmin.errors.SHUTTING_DOWN',
  KAFKA_UNAVAILABLE: 'topicAdmin.errors.KAFKA_UNAVAILABLE',
  VALIDATION_ERROR: 'topicAdmin.errors.VALIDATION_ERROR',
} as const

// 在 Topic 管理请求失败时展示统一安全错误和未由字段控件承接的提示。
export function TopicAdminErrorNotice({ error }: { error: unknown }) {
  const { t } = useTranslation()
  if (!error) return null
  // 保留 Topic 专属提示；其余错误按当前语言安全翻译，避免内部占位或后端原文进入页面。
  const message = error instanceof ApiError && Object.hasOwn(errorKeys, error.code)
    ? t(errorKeys[error.code as keyof typeof errorKeys]) : localizeApiError(error, t).message
  return <div role="alert"><p>{message}</p>
    {error instanceof ApiError && error.requestId && <p>{t('topicAdmin.requestId', { id: error.requestId })}</p>}
    {error instanceof ApiError && Object.entries(localizeApiError(error, t).fields).filter(([key]) =>
      !topicConfigDefinitions.some(item => key === item.key || key === `configs.${item.key}` || key === `changes.${item.key}`)
      && !['name', 'partitions', 'replicationFactor'].includes(key)).map(([key, message]) => <p key={key}>{message}</p>)}
  </div>
}

// 在创建或编辑 Topic 参数时渲染白名单字段、来源、风险及恢复默认操作。
export function TopicConfigField({ definition, value, onChange, disabled, error, source, reset }: {
  definition: TopicConfigDefinition; value: string; onChange: (value: string) => void
  disabled: boolean; error?: string; source?: TopicConfigSource
  reset?: { pending: boolean; disabled: boolean; onClick: () => void }
}) {
  const id = useId()
  const { t } = useTranslation()
  const label = t(definition.labelKey)
  return <article className="topic-config-card" aria-label={t('topicAdmin.card', { label })}>
    <div className="topic-config-top"><div><div className="topic-config-title">
      <h3 id={`${id}-title`}><label htmlFor={id}>{label}</label></h3>
      {source && <span className={`topic-source topic-source-${source}`}>{t(`topicAdmin.source.${source}`)}</span>}
    </div><div className="topic-config-key">{definition.key}</div></div>
      {reset && <button type="button" className="topic-reset" disabled={reset.disabled}
        aria-label={t(reset.pending ? 'topicAdmin.undoResetLabel' : 'topicAdmin.resetLabel', { label })} onClick={reset.onClick}>
        {reset.pending ? t('topicAdmin.undoReset') : source === 'brokerDefault' ? t('topicAdmin.usingDefault') : t('topicAdmin.reset')}</button>}
    </div>
    <p className="topic-config-description" id={`${id}-description`}>{t(definition.descriptionKey)}</p>
    <div className="topic-config-control">
      {definition.inputType === 'select' ? <select id={id} value={value} disabled={disabled}
        aria-invalid={!!error} aria-describedby={`${id}-description ${id}-risk${error ? ` ${id}-error` : ''}`}
        onChange={/* 在控件变更时将当前值交回受控表单，由统一校验决定是否可保存。 */ event => onChange(event.target.value)}>
        <option value="">{reset ? t('topicAdmin.unavailable') : t('topicAdmin.clusterDefault')}</option>
        {value && !definition.options?.some(option => option.value === value) && <option value={value}>{t('topicAdmin.currentValue', { value })}</option>}
        {definition.options?.map(option => <option key={option.value} value={option.value}>{t(option.labelKey)}</option>)}
      </select> : <input id={id} type="number" step="1" value={value} disabled={disabled}
        placeholder={reset ? t('topicAdmin.unavailable') : t('topicAdmin.blankDefault')}
        aria-invalid={!!error} aria-describedby={`${id}-description ${id}-risk${error ? ` ${id}-error` : ''}`}
        onChange={/* 在控件变更时将当前值交回受控表单，由统一校验决定是否可保存。 */ event => onChange(event.target.value)} />}
      {definition.unitKey && <span>{t(definition.unitKey)}</span>}
    </div>
    <p className="topic-config-risk" id={`${id}-risk`}>{t(definition.riskKey)}</p>
    {reset?.pending && <p className="topic-reset-pending">{t('topicAdmin.resetHint')}</p>}
    {error && <p className="topic-field-error" role="alert" id={`${id}-error`}>{error}</p>}
  </article>
}

export interface TopicConfigModalProps {
  sessionId: string
  topic: string
  onClose: () => void
  onApplied: (result?: TopicMutationResult) => void | Promise<void>
}

// 在打开参数弹窗或切换会话与 Topic 时重建编辑器，避免跨目标复用配置草稿。
export function TopicConfigModal(props: TopicConfigModalProps) {
  return <TopicConfigEditor key={JSON.stringify([props.sessionId, props.topic])} {...props} />
}

// 在参数编辑阶段实时读取 Kafka 当前值，维护增量草稿并处理冲突与已应用未确认结果。
function TopicConfigEditor({ sessionId, topic, onClose, onApplied }: TopicConfigModalProps) {
  const { t } = useTranslation()
  const [configs, setConfigs] = useState<TopicConfigValue[] | null>(null)
  const [draft, setDraft] = useState<Record<string, string | null>>({})
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [loadError, setLoadError] = useState<unknown>(null)
  const [saveError, setSaveError] = useState<unknown>(null)
  const [fieldErrors, setFieldErrors] = useState<Record<string, ParseKeys>>({})
  const [notice, setNotice] = useState<ParseKeys | ''>('')
  const [refreshError, setRefreshError] = useState<ParseKeys | ''>('')
  const request = useRef<AbortController | null>(null)
  const active = useRef(true)
  const submitting = useRef(false)
  const read = useCallback(/* 在弹窗打开、冲突重读或保存确认时取消旧读取并获取 Kafka 实时值，忽略已取消响应。 */ () => {
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
  // 在重新读取前清空旧配置、草稿和错误，防止把旧值作为新的比较基线。
  const prepareRead = () => {
    setLoading(true); setConfigs(null); setDraft({}); setLoadError(null); setSaveError(null); setFieldErrors({}); setRefreshError('')
  }
  useEffect(/* 在目标配置编辑器挂载时读取实时值，卸载或换目标时取消旧读取并拒绝晚到响应。 */ () => {
    active.current = true
    void read()
    return () => { active.current = false; request.current?.abort() }
  }, [read])
  const changed = Object.keys(draft).length > 0
  // 在关闭参数弹窗时阻止保存中退出，并确认是否放弃未保存修改。
  const close = () => {
    if (!saving && (!changed || window.confirm(t('topicAdmin.discardConfig')))) onClose()
  }
  // 在字段编辑或恢复默认时维护增量差异，值回到基线时移除冗余变更。
  const edit = (entry: TopicConfigValue, value: string | null) => {
    setSaveError(null); setFieldErrors({}); setNotice(''); setRefreshError('')
    setDraft(previous => {
      const next = { ...previous }
      if (value === entry.value) delete next[entry.key]
      else next[entry.key] = value
      return next
    })
  }
  // 在保存时提交修改字段与读取基线，随后重新读值并保留已应用但未确认的结果。
  const save = async () => {
    if (submitting.current || !configs || !changed || loading) return
    const errors: Record<string, ParseKeys> = {}
    for (const [key, value] of Object.entries(draft)) {
      const error = value === null ? undefined : validateTopicConfigValue(key, value)
      if (error) errors[key] = error
    }
    setFieldErrors(errors)
    if (Object.keys(errors).length) return
    const input: UpdateTopicConfigsInput = { changes: { ...draft }, expectedValues: {} }
    // 仅携带变更键的读取基线用于服务端冲突检查；Kafka 不提供本流程的原子比较写入。
    for (const key of Object.keys(draft)) input.expectedValues[key] = configs.find(item => item.key === key)!.value
    submitting.current = true; setSaving(true); setSaveError(null); setNotice(''); setRefreshError('')
    try {
      let result
      // 保持配置 key 和提交值原样，服务端先比较读取基线再执行增量修改。
      try { result = await topicsApi.update(sessionId, topic, input) }
      catch (error) { if (active.current) setSaveError(error); return }
      if (!active.current) return
      // 202 表示已应用但无法确认，不能当作写入失败重试；确认成功也须重读真实值与来源。
      let unconfirmed = 'status' in result
      if (unconfirmed) { setConfigs(null); setDraft({}) }
      else { prepareRead(); unconfirmed = !await read() }
      if (!active.current) return
      setNotice(unconfirmed ? 'topicAdmin.unconfirmed' : 'topicAdmin.saved')
      try { await onApplied(unconfirmed ? { status: 'appliedButUnconfirmed' } : undefined) }
      catch { if (active.current) setRefreshError('topicAdmin.refreshFailed') }
    } finally {
      submitting.current = false
      if (active.current) setSaving(false)
    }
  }
  return <Modal title={t('topicAdmin.configTitle')} description={t('topicAdmin.configDescription', { topic })}
    onClose={close} busy={saving} wide>
    <form className="topic-admin-form" noValidate onSubmit={/* 在表单提交时，阻止默认提交并交给当前表单校验及请求锁，避免重复写入。 */ event => { event.preventDefault(); void save() }}>
      <div className="topic-config-notice"><b>{t('topicAdmin.confirmImpact')}</b> {t('topicAdmin.configWarning')}</div>
      <div className="topic-config-legend"><span>{t('topicAdmin.sources')}</span><span className="topic-source topic-source-topicOverride">{t('topicAdmin.source.topicOverride')}</span><span className="topic-source topic-source-brokerDefault">{t('topicAdmin.source.brokerDefault')}</span></div>
      {loading && <div className="topic-config-loading" role="status"><span>{t('topicAdmin.loading')}</span>
        <div className="topic-config-grid" aria-hidden="true">{Array.from({ length: 8 }, (_, index) => <div key={index} />)}</div></div>}
      <TopicAdminErrorNotice error={loadError ?? saveError} />
      {notice && <p className="topic-config-feedback" role="status">{t(notice)}</p>}
      {refreshError && <p role="alert">{t(refreshError)}</p>}
      {!loading && (!configs || !!loadError || (saveError instanceof ApiError && saveError.code === 'TOPIC_CONFIG_CHANGED')) &&
        <button type="button" disabled={saving} onClick={/* 在按钮点击时，先确认放弃未保存修改，再清空旧比较基线并重读 Kafka，避免覆盖外部配置。 */ () => {
          if (!changed || window.confirm(t('topicAdmin.discardReload'))) {
            setNotice(''); prepareRead(); void read()
          }
        }}>{t('topicAdmin.reload')}</button>}
      {configs && <div className="topic-config-grid">{topicConfigDefinitions.map(definition => {
        const entry = configs.find(item => item.key === definition.key)
        const pending = draft[definition.key] === null
        const unavailable = !entry || entry.value === null
        const error = fieldErrors[definition.key] ? t(fieldErrors[definition.key]) : (saveError instanceof ApiError
          ? localizeApiError(saveError, t).fields[`changes.${definition.key}`] ?? localizeApiError(saveError, t).fields[definition.key] : undefined)
        return <TopicConfigField key={definition.key} definition={definition}
          source={entry?.source ?? 'unknown'} value={pending ? '' : draft[definition.key] ?? entry?.value ?? ''}
          disabled={saving || unavailable || !!entry?.readOnly || pending} error={error}
          onChange={/* 在参数字段变更时仅更新该项增量草稿，不立即写入 Kafka。 */ value => { if (entry) edit(entry, value) }}
          reset={{ pending, disabled: saving || unavailable || !!entry?.readOnly || entry?.source !== 'topicOverride',
            onClick: /* 在点击恢复默认时切换增量删除草稿，撤销操作则移除待恢复项而不立即写入 Kafka。 */ () => { if (entry) {
              if (pending) setDraft(previous => { const next = { ...previous }; delete next[entry.key]; return next })
              else edit(entry, null)
            } } }} />
      })}</div>}
      {configs?.some(entry => entry.readOnly) && <p className="form-note">{t('topicAdmin.readOnly')}</p>}
      <div className="modal-actions topic-admin-actions"><small>{t('topicAdmin.saveHint')}</small>
        <div><button type="button" disabled={saving} onClick={close}>{t('topicAdmin.cancel')}</button>
          <button type="submit" disabled={saving || loading || !configs || !changed}>{saving ? t('topicAdmin.saving') : t('topicAdmin.save')}</button></div>
      </div>
    </form>
  </Modal>
}
