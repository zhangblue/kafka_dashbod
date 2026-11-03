import type { ParseKeys } from 'i18next'
import { useTranslation } from 'react-i18next'
import { localizeApiError } from '../../api/errorMessages'
import { useEffect, useId, useRef, useState } from 'react'
import { ApiError, topicsApi } from '../../api/client'
import type { CreateTopicInput, TopicMutationResult } from '../../api/types'
import { Modal } from '../../components/Modal'
import { topicConfigDefinitions, validateTopicConfigValue } from './topicConfig'
import { TopicAdminErrorNotice, TopicConfigField } from './TopicConfigModal'

export interface CreateTopicModalProps {
  sessionId: string; onClose: () => void; onApplied: (result?: TopicMutationResult) => void | Promise<void>
}

// 在打开创建弹窗或切换会话时重建表单，确保草稿绑定当前有效会话。
export function CreateTopicModal(props: CreateTopicModalProps) {
  return <CreateTopicForm key={props.sessionId} {...props} />
}

// 在 Topic 创建阶段校验名称、数量和白名单配置，防止重复提交并等待列表刷新。
function CreateTopicForm({ sessionId, onClose, onApplied }: CreateTopicModalProps) {
  const { t } = useTranslation()
  const id = useId()
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState('1')
  const [replicationFactor, setReplicationFactor] = useState('')
  const [configs, setConfigs] = useState<Record<string, string>>({})
  const [fieldErrors, setFieldErrors] = useState<Record<string, ParseKeys>>({})
  const [error, setError] = useState<unknown>(null)
  const [saving, setSaving] = useState(false)
  const [created, setCreated] = useState(false)
  const [refreshFailed, setRefreshFailed] = useState(false)
  const submitting = useRef(false)
  const active = useRef(true)
  useEffect(/* 在创建表单挂载时允许处理响应，卸载后忽略晚到结果避免操作旧页面。 */ () => {
    active.current = true
    return () => { active.current = false }
  }, [])
  const dirty = name !== '' || partitions !== '1' || replicationFactor !== '' || Object.values(configs).some(Boolean)
  // 在退出创建弹窗时阻止提交中关闭，并在有未保存草稿时确认放弃。
  const close = () => {
    if (!saving && (created || !dirty || window.confirm(t('topicAdmin.discardCreate')))) onClose()
  }
  // 在创建字段重新编辑时清除旧校验与请求错误。
  const clearErrors = () => { setFieldErrors({}); setError(null) }
  // 在创建字段渲染时合并本地与服务端对应参数错误。
  const fieldError = (key: string) => fieldErrors[key] ? t(fieldErrors[key]) : (error instanceof ApiError
    ? localizeApiError(error, t).fields[key] ?? localizeApiError(error, t).fields[`configs.${key}`] : undefined)
  // 在创建表单渲染时生成字段错误的可访问关联。
  const attributes = (key: string) => ({ 'aria-invalid': !!fieldError(key),
    'aria-describedby': fieldError(key) ? `${id}-${key}-error` : undefined })
  // 在创建表单提交时校验并锁定请求，成功后先刷新列表再关闭弹窗。
  const submit = async () => {
    if (submitting.current || created) return
    const errors: Record<string, ParseKeys> = {}
    if (!/^[a-zA-Z0-9._-]{1,249}$/.test(name) || name === '.' || name === '..') errors.name = 'topicAdmin.invalidName'
    if (!/^[0-9]+$/.test(partitions) || !Number.isSafeInteger(Number(partitions)) || Number(partitions) < 1 || Number(partitions) > 100000) errors.partitions = 'topicAdmin.invalidPartitions'
    if (replicationFactor !== '' && (!/^[0-9]+$/.test(replicationFactor) || Number(replicationFactor) < 1 || Number(replicationFactor) > 10000)) errors.replicationFactor = 'topicAdmin.invalidReplicas'
    const suppliedConfigs = Object.fromEntries(Object.entries(configs).filter(([, value]) => value !== ''))
    for (const [key, value] of Object.entries(suppliedConfigs)) {
      const error = validateTopicConfigValue(key, value)
      if (error) errors[key] = error
    }
    setFieldErrors(errors)
    if (Object.keys(errors).length) return
    const input: CreateTopicInput = { name, partitions: Number(partitions), replicationFactor: replicationFactor === '' ? null : Number(replicationFactor),
      ...(Object.keys(suppliedConfigs).length ? { configs: suppliedConfigs } : {}) }
    submitting.current = true; setSaving(true); setError(null)
    try {
      let result
      // 先提交原始名称和配置值，再等待列表刷新；翻译仅用于展示。
      try { result = await topicsApi.create(sessionId, input) }
      catch (error) { if (active.current) setError(error); return }
      if (!active.current) return
      setCreated(true)
      try { await onApplied(result) }
      catch { if (active.current) setRefreshFailed(true); return }
      if (active.current) onClose()
    } finally { submitting.current = false; if (active.current) setSaving(false) }
  }
  return <Modal title={t('topicAdmin.createTitle')} description={t('topicAdmin.createDescription')} onClose={close} busy={saving} wide>
    <form className="topic-admin-form" noValidate onSubmit={/* 在表单提交时，阻止默认提交并交给当前表单校验及请求锁，避免重复写入。 */ event => { event.preventDefault(); void submit() }}>
      <fieldset className="form-grid topic-basic-fields" disabled={saving || created}><legend>{t('topicAdmin.basic')}</legend>
        {([
          ['name', t('topicAdmin.name'), name, setName, 'text'],
          ['partitions', t('topicAdmin.partitions'), partitions, setPartitions, 'number'],
          ['replicationFactor', t('topicAdmin.replicas'), replicationFactor, setReplicationFactor, 'number'],
        ] as const).map(([key, label, value, update, type]) => <div key={key} className={key === 'name' ? 'full' : undefined}>
          <label htmlFor={`${id}-${key}`}>{label}<input id={`${id}-${key}`} type={type} step={type === 'number' ? '1' : undefined}
            value={value} placeholder={key === 'replicationFactor' ? t('topicAdmin.clusterDefault') : undefined}
            {...attributes(key)} onChange={/* 在创建字段变更时更新草稿并撤销过时的字段或请求错误。 */ event => { clearErrors(); update(event.target.value) }} /></label>
          {fieldError(key) && <p className="topic-field-error" role="alert" id={`${id}-${key}-error`}>{fieldError(key)}</p>}
        </div>)}
      </fieldset>
      <h3 className="topic-optional-title">{t('topicAdmin.optional')}</h3><p className="form-note">{t('topicAdmin.optionalHint')}</p>
      <div className="topic-config-grid">{topicConfigDefinitions.map(definition => <TopicConfigField key={definition.key}
        definition={definition} value={configs[definition.key] ?? ''} disabled={saving || created} error={fieldError(definition.key)}
        onChange={/* 在创建字段变更时更新草稿并撤销过时的字段或请求错误。 */ value => { clearErrors(); setConfigs(previous => ({ ...previous, [definition.key]: value })) }} />)}</div>
      <TopicAdminErrorNotice error={error} />
      {refreshFailed && <p role="status">{t('topicAdmin.createdRefreshFailed')}</p>}
      <div className="modal-actions topic-admin-actions"><small>{t('topicAdmin.createHint')}</small><div>
        <button type="button" disabled={saving} onClick={close}>{created ? t('topicAdmin.close') : t('topicAdmin.cancel')}</button><button type="submit" disabled={saving || created}>{created ? t('topicAdmin.created') : saving ? t('topicAdmin.creating') : t('topicAdmin.create')}</button>
      </div></div>
    </form>
  </Modal>
}
