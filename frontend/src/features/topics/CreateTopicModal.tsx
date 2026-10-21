import { useEffect, useId, useRef, useState } from 'react'
import { ApiError, topicsApi } from '../../api/client'
import type { CreateTopicInput, TopicMutationResult } from '../../api/types'
import { Modal } from '../../components/Modal'
import { topicConfigDefinitions, validateTopicConfigValue } from './topicConfig'
import { TopicAdminErrorNotice, TopicConfigField } from './TopicConfigModal'

export interface CreateTopicModalProps {
  sessionId: string; onClose: () => void; onApplied: (result?: TopicMutationResult) => void | Promise<void>
}

export function CreateTopicModal(props: CreateTopicModalProps) {
  return <CreateTopicForm key={props.sessionId} {...props} />
}

function CreateTopicForm({ sessionId, onClose, onApplied }: CreateTopicModalProps) {
  const id = useId()
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState('1')
  const [replicationFactor, setReplicationFactor] = useState('')
  const [configs, setConfigs] = useState<Record<string, string>>({})
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({})
  const [error, setError] = useState<unknown>(null)
  const [saving, setSaving] = useState(false)
  const [created, setCreated] = useState(false)
  const [refreshFailed, setRefreshFailed] = useState(false)
  const submitting = useRef(false)
  const active = useRef(true)
  useEffect(() => {
    active.current = true
    return () => { active.current = false }
  }, [])
  const dirty = name !== '' || partitions !== '1' || replicationFactor !== '' || Object.values(configs).some(Boolean)
  const close = () => {
    if (!saving && (created || !dirty || window.confirm('有未保存的 Topic 创建内容，确定放弃吗？'))) onClose()
  }
  const clearErrors = () => { setFieldErrors({}); setError(null) }
  const fieldError = (key: string) => fieldErrors[key] ?? (error instanceof ApiError
    ? error.fields[key] ?? error.fields[`configs.${key}`] : undefined)
  const attributes = (key: string) => ({ 'aria-invalid': !!fieldError(key),
    'aria-describedby': fieldError(key) ? `${id}-${key}-error` : undefined })
  const submit = async () => {
    if (submitting.current || created) return
    const errors: Record<string, string> = {}
    if (!/^[a-zA-Z0-9._-]{1,249}$/.test(name) || name === '.' || name === '..') errors.name = '请输入 1–249 个字母、数字、点、下划线或短横线；名称不能是 . 或 ..。'
    if (!/^[0-9]+$/.test(partitions) || !Number.isSafeInteger(Number(partitions)) || Number(partitions) < 1 || Number(partitions) > 100000) errors.partitions = '分区数必须为 1–100000 的整数。'
    if (replicationFactor !== '' && (!/^[0-9]+$/.test(replicationFactor) || Number(replicationFactor) < 1 || Number(replicationFactor) > 10000)) errors.replicationFactor = '副本因子必须为 1–10000 的整数，或留空使用集群默认值。'
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
      try { result = await topicsApi.create(sessionId, input) }
      catch (error) { if (active.current) setError(error); return }
      if (!active.current) return
      setCreated(true)
      try { await onApplied(result) }
      catch { if (active.current) setRefreshFailed(true); return }
      if (active.current) onClose()
    } finally { submitting.current = false; if (active.current) setSaving(false) }
  }
  return <Modal title="新建 Topic" description="创建会直接修改 Kafka 集群；未设置的可选配置使用集群默认值。" onClose={close} busy={saving} wide>
    <form className="topic-admin-form" noValidate onSubmit={event => { event.preventDefault(); void submit() }}>
      <fieldset className="form-grid topic-basic-fields" disabled={saving || created}><legend>基础信息</legend>
        {([
          ['name', 'Topic 名称', name, setName, 'text'],
          ['partitions', '分区数', partitions, setPartitions, 'number'],
          ['replicationFactor', '副本因子', replicationFactor, setReplicationFactor, 'number'],
        ] as const).map(([key, label, value, update, type]) => <div key={key} className={key === 'name' ? 'full' : undefined}>
          <label htmlFor={`${id}-${key}`}>{label}<input id={`${id}-${key}`} type={type} step={type === 'number' ? '1' : undefined}
            value={value} placeholder={key === 'replicationFactor' ? '使用集群默认值' : undefined}
            {...attributes(key)} onChange={event => { clearErrors(); update(event.target.value) }} /></label>
          {fieldError(key) && <p className="topic-field-error" role="alert" id={`${id}-${key}-error`}>{fieldError(key)}</p>}
        </div>)}
      </fieldset>
      <h3 className="topic-optional-title">可选配置</h3><p className="form-note">以下参数均可选；留空表示使用 Kafka 集群默认值。</p>
      <div className="topic-config-grid">{topicConfigDefinitions.map(definition => <TopicConfigField key={definition.key}
        definition={definition} value={configs[definition.key] ?? ''} disabled={saving || created} error={fieldError(definition.key)}
        onChange={value => { clearErrors(); setConfigs(previous => ({ ...previous, [definition.key]: value })) }} />)}</div>
      <TopicAdminErrorNotice error={error} />
      {refreshFailed && <p role="status">Topic 已创建，但列表刷新失败。请关闭弹窗后手动刷新。</p>}
      <div className="modal-actions topic-admin-actions"><small>创建前请确认分区数、副本因子与集群策略。</small><div>
        <button type="button" disabled={saving} onClick={close}>{created ? '关闭' : '取消'}</button><button type="submit" disabled={saving || created}>{created ? 'Topic 已创建' : saving ? '创建中…' : '创建 Topic'}</button>
      </div></div>
    </form>
  </Modal>
}
