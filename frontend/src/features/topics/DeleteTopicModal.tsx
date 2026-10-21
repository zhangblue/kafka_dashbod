import { useEffect, useId, useRef, useState } from 'react'
import { topicsApi } from '../../api/client'
import { Modal } from '../../components/Modal'
import { TopicAdminErrorNotice } from './TopicConfigModal'

export interface DeleteTopicModalProps {
  sessionId: string; topic: string; onClose: () => void; onApplied: () => void | Promise<void>
}

export function DeleteTopicModal(props: DeleteTopicModalProps) {
  return <DeleteTopicForm key={JSON.stringify([props.sessionId, props.topic])} {...props} />
}

function DeleteTopicForm({ sessionId, topic, onClose, onApplied }: DeleteTopicModalProps) {
  const id = useId()
  const [confirmationName, setConfirmationName] = useState('')
  const [error, setError] = useState<unknown>(null)
  const [saving, setSaving] = useState(false)
  const [deleted, setDeleted] = useState(false)
  const [refreshFailed, setRefreshFailed] = useState(false)
  const submitting = useRef(false)
  const active = useRef(true)
  useEffect(() => {
    active.current = true
    return () => { active.current = false }
  }, [])
  const close = () => { if (!saving) onClose() }
  const submit = async () => {
    if (submitting.current || deleted || confirmationName !== topic || topic.startsWith('__')) return
    submitting.current = true; setSaving(true); setError(null)
    try {
      try { await topicsApi.delete(sessionId, topic, confirmationName) }
      catch (error) { if (active.current) setError(error); return }
      if (!active.current) return
      setDeleted(true)
      try { await onApplied() }
      catch { if (active.current) setRefreshFailed(true); return }
      if (active.current) onClose()
    } finally { submitting.current = false; if (active.current) setSaving(false) }
  }
  return <Modal title="删除 Topic" description="删除会直接修改 Kafka 集群，请仔细确认目标。" onClose={close} busy={saving}>
    <form className="topic-delete-form" onSubmit={event => { event.preventDefault(); void submit() }}>
      <p className="topic-delete-warning"><b>此操作不可恢复。</b> Topic 中的数据将被删除，依赖它的生产者和消费者可能受到影响。</p>
      <p>待删除 Topic：<strong className="topic-delete-name">{topic}</strong></p>
      <label htmlFor={id}>确认 Topic 名称<input id={id} value={confirmationName} autoComplete="off" spellCheck={false}
        aria-describedby={`${id}-hint`} disabled={saving || deleted}
        onChange={event => { setConfirmationName(event.target.value); setError(null) }} /></label>
      <p id={`${id}-hint`} className="form-note">请输入完整 Topic 名称，大小写必须完全一致。</p>
      <TopicAdminErrorNotice error={error} />
      {refreshFailed && <p role="status">删除请求已成功，但列表刷新失败。请关闭弹窗后手动刷新。</p>}
      <div className="modal-actions"><small>集群状态更新可能稍有延迟。</small><div>
        <button type="button" disabled={saving} onClick={close}>{deleted ? '关闭' : '取消'}</button>
        <button type="submit" className="danger-button" disabled={saving || deleted || confirmationName !== topic || topic.startsWith('__')}>
          {deleted ? '删除请求已成功' : saving ? '删除中…' : '确认删除'}</button>
      </div></div>
    </form>
  </Modal>
}
