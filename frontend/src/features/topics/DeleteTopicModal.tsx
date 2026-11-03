import { useEffect, useId, useRef, useState } from 'react'
import { topicsApi } from '../../api/client'
import { Modal } from '../../components/Modal'
import { TopicAdminErrorNotice } from './TopicConfigModal'

export interface DeleteTopicModalProps {
  sessionId: string; topic: string; onClose: () => void; onApplied: () => void | Promise<void>
}

// 在删除弹窗打开或目标变化时重建确认表单，避免复用旧名称确认。
export function DeleteTopicModal(props: DeleteTopicModalProps) {
  return <DeleteTopicForm key={JSON.stringify([props.sessionId, props.topic])} {...props} />
}

// 在 Topic 删除阶段要求完整名称严格匹配，保护内部 Topic 并锁定已成功请求。
function DeleteTopicForm({ sessionId, topic, onClose, onApplied }: DeleteTopicModalProps) {
  const id = useId()
  const [confirmationName, setConfirmationName] = useState('')
  const [error, setError] = useState<unknown>(null)
  const [saving, setSaving] = useState(false)
  const [deleted, setDeleted] = useState(false)
  const [refreshFailed, setRefreshFailed] = useState(false)
  const submitting = useRef(false)
  const active = useRef(true)
  useEffect(/* 在删除表单挂载时允许更新反馈，卸载后忽略晚到删除结果避免重复关闭新目标。 */ () => {
    active.current = true
    return () => { active.current = false }
  }, [])
  // 在关闭删除弹窗时阻止在途请求期间退出。
  const close = () => { if (!saving) onClose() }
  // 在确认删除时严格校验完整名称与内部前缀；成功后先锁定删除，再等待刷新以免重复删除。
  const submit = async () => {
    // 完整名称严格相等且不能为内部 Topic；先通过确认边界才允许请求，避免误删或重复删除。
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
    <form className="topic-delete-form" onSubmit={/* 在表单提交时，阻止默认提交并交给当前表单校验及请求锁，避免重复写入。 */ event => { event.preventDefault(); void submit() }}>
      <p className="topic-delete-warning"><b>此操作不可恢复。</b> Topic 中的数据将被删除，依赖它的生产者和消费者可能受到影响。</p>
      <p>待删除 Topic：<strong className="topic-delete-name">{topic}</strong></p>
      <label htmlFor={id}>确认 Topic 名称<input id={id} value={confirmationName} autoComplete="off" spellCheck={false}
        aria-describedby={`${id}-hint`} disabled={saving || deleted}
        onChange={/* 在确认名称变更时保留完整原始输入并清除旧错误，严格比较由提交边界执行。 */ event => { setConfirmationName(event.target.value); setError(null) }} /></label>
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
