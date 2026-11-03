import { useEffect, useId, useRef, useState } from 'react'
import { ApiError, feishuAlertsApi } from '../../api/client'
import type { ClusterSnapshot, FeishuAlertRule, FeishuAlertSettings } from '../../api/types'
import { useSnapshotStale } from '../dashboard/useSnapshotStale'
import { snapshotUnavailable } from '../lag-rules/snapshotAvailability'

export interface FeishuAlertFormProps {
  clusterId: string
  value: FeishuAlertSettings
  snapshot: ClusterSnapshot | null
  snapshotStale?: boolean
  onSaved?: (value: FeishuAlertSettings) => void
  onClose?: () => void
}

// 在 Token 编辑或提交校验时检查控制字符、字节长度和非法路径片段，不把输入写入错误。
function tokenInvalid(value: string) {
  const token = value.trim()
  return [...value].some(character => {
    const code = character.codePointAt(0)!
    return code < 32 || (code >= 127 && code <= 159)
  })
    || new TextEncoder().encode(token).length > 512 || token === '.' || token === '..'
}

// 在飞书保存或测试失败时将错误映射成固定中文提示，阻止远端消息或凭据回显。
function safeFailure(error: unknown, action: 'save' | 'test') {
  if (error instanceof ApiError) {
    if (error.code === 'CLUSTER_NOT_FOUND' || error.status === 404) return '集群不存在，请关闭后重新选择集群。'
    if (error.status === 422 || error.code === 'VALIDATION_ERROR') return '配置校验失败，请检查 Token 格式、规则组合、非负整数阈值及至少 10 秒的持续时间。'
    if (error.status === 413) return '配置内容过大，请减少规则或缩短 Token 后重试。'
  }
  return action === 'test' ? '测试通知发送失败，请检查 Token 和飞书机器人设置后重试。' : '保存飞书告警配置失败，请稍后重试。'
}

// Reloaded settings and cluster changes discard the entire draft, including credentials.
// 在配置来源或集群变化时重新挂载编辑器，清除未保存的 Token 和规则草稿。
export function FeishuAlertForm(props: FeishuAlertFormProps) {
  const [source, setSource] = useState({ clusterId: props.clusterId, value: props.value, revision: 0 })
  if (source.clusterId !== props.clusterId || source.value !== props.value) {
    setSource({ clusterId: props.clusterId, value: props.value, revision: source.revision + 1 })
  }
  return <FeishuAlertEditor key={source.revision} {...props} />
}

// 在飞书设置打开时维护当前编辑状态、反馈及可用快照，隔离不同集群的草稿。
function FeishuAlertEditor({ clusterId, value, snapshot, snapshotStale = false, onSaved, onClose }: FeishuAlertFormProps) {
  const id = useId()
  const [saved, setSaved] = useState(value)
  const [rules, setRules] = useState(() => value.rules.map(rule => ({ ...rule })))
  const [accessToken, setAccessToken] = useState('')
  const [clearAccessToken, setClearAccessToken] = useState(false)
  const [showToken, setShowToken] = useState(false)
  const [busy, setBusy] = useState<'save' | 'test' | null>(null)
  const [feedback, setFeedback] = useState<{ error: boolean; message: string } | null>(null)
  const active = useRef(true)
  useEffect(/* 在编辑器挂载时开启响应更新，卸载后拒绝晚到保存或测试结果修改状态。 */ () => {
    active.current = true
    return () => { active.current = false }
  }, [])

  // The parent may invalidate a retained snapshot immediately; age also expires it here.
  // 仅接受同集群、时间有效且未陈旧的快照；下拉失效时仍允许停用或调整旧规则以恢复配置。
  const expired = useSnapshotStale({ snapshot, status: snapshotStale ? 'error' : 'connected' }, 10)
  const available = !!snapshot && snapshot.clusterId === clusterId && !expired
    && Number.isFinite(Date.parse(snapshot.collectedAt)) && !snapshotUnavailable(snapshot)
  const groups = available ? [...new Set(snapshot.consumerGroupIds)].sort() : []
  // 在规则渲染或切换 Group 时只从同集群新鲜快照选择存在有效提交位置的 Topic。
  const topicsFor = (groupId: string) => available ? [...new Set(snapshot.consumerGroups
    .filter(group => group.groupId === groupId && group.partitions.some(partition => partition.committedOffset !== null && partition.committedOffset >= 0))
    .map(group => group.topic))].sort() : []
  // 在编辑规则字段时清除旧反馈并只更新指定规则。
  const updateRule = (index: number, patch: Partial<FeishuAlertRule>) => {
    setFeedback(null)
    setRules(current => current.map((rule, i) => i === index ? { ...rule, ...patch } : rule))
  }
  const errors: string[] = []
  const invalidToken = tokenInvalid(accessToken)
  if (invalidToken) errors.push('Token 格式无效：最多 512 字节，不能包含控制字符或使用 .、..。')
  const pairs = new Set<string>()
  for (const rule of rules) {
    if (!rule.groupId.trim() || !rule.topic.trim()) errors.push('请选择完整的 Consumer Group 和 Topic。')
    const pair = JSON.stringify([rule.groupId.trim(), rule.topic.trim()])
    if (pairs.has(pair)) errors.push('Consumer Group + Topic 组合不能重复。')
    pairs.add(pair)
    if (!Number.isSafeInteger(rule.lagThreshold) || rule.lagThreshold < 0) errors.push('Lag 阈值必须是非负安全整数。')
    if (!Number.isSafeInteger(rule.growthSeconds) || rule.growthSeconds < 10) errors.push('持续时间必须是整数且至少为 10 秒。')
  }
  const valid = errors.length === 0
  const canTest = !busy && !clearAccessToken && !invalidToken && !!(accessToken.trim() || saved.accessTokenConfigured)
  const validationId = errors.length ? `${id}-validation` : undefined

  // 在保存或测试通知时校验并提交当前草稿；保存成功后立即清空新 Token 和显隐状态。
  async function submit(action: 'save' | 'test') {
    if (busy || (action === 'save' ? !valid : !canTest)) return
    setBusy(action)
    setFeedback(null)
    try {
      const token = accessToken.trim() || undefined
      if (action === 'test') {
        await feishuAlertsApi.test(clusterId, token)
        if (active.current) setFeedback({ error: false, message: '测试通知已发送' })
      } else {
        const result = await feishuAlertsApi.save(clusterId, {
          ...(token ? { accessToken: token } : {}), clearAccessToken, rules,
        })
        if (!active.current) return
        setSaved(result)
        setRules(result.rules.map(rule => ({ ...rule })))
        // 成功后立即丢弃本次明文草稿并恢复隐藏，仅保留服务端返回的配置标志与掩码。
        setAccessToken('')
        setClearAccessToken(false)
        setShowToken(false)
        setFeedback({ error: false, message: '飞书告警配置已保存' })
        onSaved?.(result)
      }
    } catch (error) {
      if (active.current) setFeedback({ error: true, message: safeFailure(error, action) })
    } finally {
      if (active.current) setBusy(null)
    }
  }

  return <form className="feishu-alert-form" aria-label="飞书告警配置" noValidate onSubmit={/* 在表单提交时，阻止默认提交并交给当前表单校验及请求锁，避免重复写入。 */ event => { event.preventDefault(); void submit('save') }}>
    <div className="feishu-token-field">
      <label htmlFor={`${id}-token`}>更新 Token</label>
      <div className="feishu-token-input">
        <input id={`${id}-token`} className="feishu-control" type={showToken ? 'text' : 'password'} autoComplete="new-password"
          value={accessToken} disabled={!!busy || clearAccessToken} aria-invalid={invalidToken} aria-describedby={`${id}-token-note`}
          placeholder={saved.accessTokenConfigured ? '留空保留已保存 Token' : '输入飞书机器人 Token'}
          onChange={/* 在字段变更时，更新本次 Token 草稿并清除过时反馈。 */ event => { setAccessToken(event.target.value); setFeedback(null) }} />
        <button type="button" className="feishu-token-eye" disabled={!!busy || clearAccessToken || !accessToken}
          aria-label={showToken ? '隐藏 Token' : '显示 Token'} title={showToken ? '隐藏 Token' : '显示 Token'}
          onClick={/* 在按钮点击时，切换本次草稿 Token 的显隐，不读取已保存凭据。 */ () => setShowToken(current => !current)}>
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7" aria-hidden="true">
            <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" /><circle cx="12" cy="12" r="3" />
            {showToken && <path d="m3 3 18 18" />}
          </svg>
        </button>
      </div>
      <p className="form-note" id={`${id}-token-note`}>已保存 Token：{saved.accessTokenConfigured ? <span>{saved.maskedAccessToken ?? '已配置'}</span> : '未配置'}。只显示掩码；留空保留，更新值保存后以明文存入数据库。</p>
      <label className="feishu-checkbox"><input type="checkbox" checked={clearAccessToken} disabled={!!busy || !saved.accessTokenConfigured}
        onChange={/* 在清除复选框变更时设置显式清除草稿、清空本次输入并恢复隐藏；勾选只修改草稿，保存时才删除已保存 Token。 */ event => { setClearAccessToken(event.target.checked); setAccessToken(''); setShowToken(false); setFeedback(null) }} />清除已保存 Token</label>
    </div>
    <p className="form-note">Lag 超过阈值并持续增长达到设定时间后发送通知；启用规则且配置 Token 后，后台会持续监控。</p>
    <section aria-labelledby={`${id}-rules-title`} className="feishu-rules-box">
      <div className="feishu-rules-heading"><h3 id={`${id}-rules-title`}>Consumer Group + Topic 告警规则</h3>
        <button type="button" disabled={!available || !!busy} onClick={/* 在按钮点击时，更新消费组并清空旧 Topic，避免沿用跨组无效组合。 */ () => { setRules(current => [...current, { groupId: '', topic: '', lagThreshold: 0, growthSeconds: 10, enabled: true }]); setFeedback(null) }}>新增规则</button>
      </div>
      {!available && <p role="status" className="form-note">暂无新鲜的同集群快照，请重新连接后新增或更换 Consumer Group / Topic；仍可修改阈值、持续时间或停用规则。</p>}
      {available && !!snapshot.partialFailures.length && <p role="status" className="form-note">快照部分采集失败，下拉选项仅包含已发现且具有提交位置的 Topic。</p>}
      <div className="feishu-rules-scroll" role="region" aria-label="飞书告警规则表" tabIndex={0}>
        {rules.map((rule, index) => {
          const topics = topicsFor(rule.groupId)
          return <fieldset key={rule.id ?? index} className="feishu-rule-row">
            <legend>告警规则 {index + 1}</legend>
            <label>Consumer Group<select className="feishu-control" aria-label="Consumer Group" value={rule.groupId} disabled={!available || !!busy}
              aria-describedby={validationId} onChange={/* 在字段变更时，更新消费组并清空旧 Topic，避免沿用跨组无效组合。 */ event => updateRule(index, { groupId: event.target.value, topic: '' })}>
              <option value="">请选择 Consumer Group</option>
              {rule.groupId && !groups.includes(rule.groupId) && <option value={rule.groupId} disabled>{rule.groupId}（当前不可用）</option>}
              {groups.map(group => <option key={group} value={group}>{group}</option>)}
            </select></label>
            <label>Topic<select className="feishu-control" aria-label="Topic" value={rule.topic} disabled={!available || !rule.groupId || !!busy}
              aria-describedby={validationId} onChange={/* 在Topic 选择变化时更新指定规则的组合草稿。 */ event => updateRule(index, { topic: event.target.value })}>
              <option value="">请选择 Topic</option>
              {rule.topic && !topics.includes(rule.topic) && <option value={rule.topic} disabled>{rule.topic}（当前不可用）</option>}
              {topics.map(topic => <option key={topic} value={topic}>{topic}</option>)}
            </select></label>
            <label>Lag 阈值<input className="feishu-control feishu-threshold" type="number" min="0" step="1" value={Number.isNaN(rule.lagThreshold) ? '' : rule.lagThreshold}
              disabled={!!busy} aria-describedby={validationId} onChange={/* 在Lag 阈值编辑时保留空输入为无效数值，交由表单阻止非法保存。 */ event => updateRule(index, { lagThreshold: event.target.value === '' ? NaN : Number(event.target.value) })} /></label>
            <label>持续时间（秒）<input className="feishu-control feishu-duration" type="number" min="10" step="1" value={Number.isNaN(rule.growthSeconds) ? '' : rule.growthSeconds}
              disabled={!!busy} aria-describedby={validationId} onChange={/* 在持续时间编辑时保留空输入为无效数值，并由至少十秒的校验限制保存。 */ event => updateRule(index, { growthSeconds: event.target.value === '' ? NaN : Number(event.target.value) })} /></label>
            <label className="feishu-checkbox"><input type="checkbox" checked={rule.enabled} disabled={!!busy} onChange={/* 在启用复选框变化时只修改当前规则开关，允许停用不可用资源的旧规则。 */ event => updateRule(index, { enabled: event.target.checked })} />启用规则</label>
            <button className="danger-button" type="button" disabled={!!busy} onClick={/* 在删除规则按钮点击时移除当前草稿项并清除旧反馈，等待保存才持久化。 */ () => { setRules(current => current.filter((_, i) => i !== index)); setFeedback(null) }}>删除规则</button>
          </fieldset>
        })}
        {!rules.length && <p className="form-note">暂无飞书告警规则。</p>}
      </div>
    </section>
    {!!errors.length && <p role="alert" id={validationId}>{[...new Set(errors)].join(' ')}</p>}
    {feedback && <p role={feedback.error ? 'alert' : 'status'}>{feedback.message}</p>}
    <div className="modal-actions feishu-form-actions">
      <button type="button" disabled={!canTest} onClick={/* 在点击测试通知时只测试当前新 Token 或已保存配置，不把新草稿持久化。 */ () => { void submit('test') }}>{busy === 'test' ? '发送中…' : '测试通知'}</button>
      <div>{onClose && <button type="button" disabled={!!busy} onClick={onClose}>关闭</button>}
        <button type="submit" disabled={!valid || !!busy}>{busy === 'save' ? '保存中…' : '保存配置'}</button></div>
    </div>
  </form>
}
