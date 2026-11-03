import { localizeApiError } from '../../api/errorMessages'
import { useTranslation } from 'react-i18next'
import type { zhCN } from '../../i18n/resources/zh-CN'
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

type FeedbackKey = `feishuConfig.${keyof typeof zhCN.feishuConfig}`

// 保存或测试失败时按稳定错误码选取受控键，未知错误只使用当前操作的安全提示。
function safeFailure(error: unknown, action: 'save' | 'test'): FeedbackKey {
  if (error instanceof ApiError) {
    if (error.code === 'FEISHU_SEND_FAILED') return 'feishuConfig.testFailed'
    if (error.code === 'CLUSTER_NOT_FOUND') return 'feishuConfig.clusterMissing'
    if (error.code === 'VALIDATION_ERROR') return 'feishuConfig.invalidSettings'
    if (error.code === 'PAYLOAD_TOO_LARGE') return 'feishuConfig.tooLarge'
    // 已知业务码优先；没有适用业务码时才按 HTTP 状态提供安全降级。
    if (error.status === 404) return 'feishuConfig.clusterMissing'
    if (error.status === 422) return 'feishuConfig.invalidSettings'
    if (error.status === 413) return 'feishuConfig.tooLarge'
  }
  return action === 'test' ? 'feishuConfig.testFailed' : 'feishuConfig.saveFailed'
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
  const { t } = useTranslation()
  const id = useId()
  const [saved, setSaved] = useState(value)
  const [rules, setRules] = useState(() => value.rules.map(rule => ({ ...rule })))
  const [accessToken, setAccessToken] = useState('')
  const [clearAccessToken, setClearAccessToken] = useState(false)
  const [showToken, setShowToken] = useState(false)
  const [busy, setBusy] = useState<'save' | 'test' | null>(null)
  const [feedback, setFeedback] = useState<{ error: boolean; message: FeedbackKey; sourceError?: unknown } | null>(null)
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
  const errors: FeedbackKey[] = []
  const invalidToken = tokenInvalid(accessToken)
  if (invalidToken) errors.push('feishuConfig.invalidToken')
  const pairs = new Set<string>()
  for (const rule of rules) {
    if (!rule.groupId.trim() || !rule.topic.trim()) errors.push('feishuConfig.incompletePair')
    const pair = JSON.stringify([rule.groupId.trim(), rule.topic.trim()])
    if (pairs.has(pair)) errors.push('feishuConfig.duplicatePair')
    pairs.add(pair)
    if (!Number.isSafeInteger(rule.lagThreshold) || rule.lagThreshold < 0) errors.push('feishuConfig.invalidThreshold')
    if (!Number.isSafeInteger(rule.growthSeconds) || rule.growthSeconds < 10) errors.push('feishuConfig.invalidDuration')
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
        // 凭据只交给专用 API，不传入翻译、日志或错误诊断。
        await feishuAlertsApi.test(clusterId, token)
        if (active.current) setFeedback({ error: false, message: 'feishuConfig.testSent' })
      } else {
        // 校验完成后提交规则和本次凭据，响应只保留掩码与配置标志。
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
        setFeedback({ error: false, message: 'feishuConfig.saved' })
        onSaved?.(result)
      }
    } catch (error) {
      if (active.current) setFeedback({ error: true, message: safeFailure(error, action), sourceError: error })
    } finally {
      if (active.current) setBusy(null)
    }
  }

  // 每次渲染重新翻译白名单字段错误，不渲染远端原文或未知字段名。
  const serverFields = feedback?.sourceError ? localizeApiError(feedback.sourceError, t).fields : {}

  return <form className="feishu-alert-form" aria-label={t('feishuConfig.form')} noValidate onSubmit={/* 在表单提交时，阻止默认提交并交给当前表单校验及请求锁，避免重复写入。 */ event => { event.preventDefault(); void submit('save') }}>
    <div className="feishu-token-field">
      <label htmlFor={`${id}-token`}>{t('feishuConfig.updateToken')}</label>
      <div className="feishu-token-input">
        <input id={`${id}-token`} className="feishu-control" type={showToken ? 'text' : 'password'} autoComplete="new-password"
          value={accessToken} disabled={!!busy || clearAccessToken} aria-invalid={invalidToken} aria-describedby={`${id}-token-note`}
          placeholder={saved.accessTokenConfigured ? t('feishuConfig.keepToken') : t('feishuConfig.enterToken')}
          onChange={/* 在字段变更时，更新本次 Token 草稿并清除过时反馈。 */ event => { setAccessToken(event.target.value); setFeedback(null) }} />
        <button type="button" className="feishu-token-eye" disabled={!!busy || clearAccessToken || !accessToken}
          aria-label={showToken ? t('feishuConfig.hideToken') : t('feishuConfig.showToken')} title={showToken ? t('feishuConfig.hideToken') : t('feishuConfig.showToken')}
          onClick={/* 在按钮点击时，切换本次草稿 Token 的显隐，不读取已保存凭据。 */ () => setShowToken(current => !current)}>
          <svg viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" strokeWidth="1.7" aria-hidden="true">
            <path d="M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12Z" /><circle cx="12" cy="12" r="3" />
            {showToken && <path d="m3 3 18 18" />}
          </svg>
        </button>
      </div>
      <p className="form-note" id={`${id}-token-note`}>{t('feishuConfig.savedToken')}{saved.accessTokenConfigured ? <span>{saved.maskedAccessToken ?? t('feishuConfig.configured')}</span> : t('feishuConfig.notConfigured')}{t('feishuConfig.tokenNote')}</p>
      <label className="feishu-checkbox"><input type="checkbox" checked={clearAccessToken} disabled={!!busy || !saved.accessTokenConfigured}
        onChange={/* 在清除复选框变更时设置显式清除草稿、清空本次输入并恢复隐藏；勾选只修改草稿，保存时才删除已保存 Token。 */ event => { setClearAccessToken(event.target.checked); setAccessToken(''); setShowToken(false); setFeedback(null) }} />{t('feishuConfig.clearToken')}</label>
    </div>
    <p className="form-note">{t('feishuConfig.growthNote')}</p>
    <section aria-labelledby={`${id}-rules-title`} className="feishu-rules-box">
      <div className="feishu-rules-heading"><h3 id={`${id}-rules-title`}>{t('feishuConfig.rulesTitle')}</h3>
        <button type="button" disabled={!available || !!busy} onClick={/* 点击新增规则时追加待选择 Group / Topic 的草稿，初始化阈值、持续时间和启用状态并清除旧反馈。 */ () => { setRules(current => [...current, { groupId: '', topic: '', lagThreshold: 0, growthSeconds: 10, enabled: true }]); setFeedback(null) }}>{t('feishuConfig.add')}</button>
      </div>
      {!available && <p role="status" className="form-note">{t('feishuConfig.unavailable')}</p>}
      {available && !!snapshot.partialFailures.length && <p role="status" className="form-note">{t('feishuConfig.partial')}</p>}
      <div className="feishu-rules-scroll" role="region" aria-label={t('feishuConfig.rulesTable')} tabIndex={0}>
        {rules.map((rule, index) => {
          const topics = topicsFor(rule.groupId)
          return <fieldset key={rule.id ?? index} className="feishu-rule-row">
            <legend>{t('feishuConfig.rule', { number: index + 1 })}</legend>
            <label>Consumer Group<select className="feishu-control" aria-label="Consumer Group" value={rule.groupId} disabled={!available || !!busy}
              aria-describedby={validationId} onChange={/* 在字段变更时，更新消费组并清空旧 Topic，避免沿用跨组无效组合。 */ event => updateRule(index, { groupId: event.target.value, topic: '' })}>
              <option value="">{t('feishuConfig.chooseGroup')}</option>
              {rule.groupId && !groups.includes(rule.groupId) && <option value={rule.groupId} disabled>{t('lagConfig.missing', { name: rule.groupId })}</option>}
              {groups.map(group => <option key={group} value={group}>{group}</option>)}
            </select></label>
            <label>Topic<select className="feishu-control" aria-label="Topic" value={rule.topic} disabled={!available || !rule.groupId || !!busy}
              aria-describedby={validationId} onChange={/* 在Topic 选择变化时更新指定规则的组合草稿。 */ event => updateRule(index, { topic: event.target.value })}>
              <option value="">{t('feishuConfig.chooseTopic')}</option>
              {rule.topic && !topics.includes(rule.topic) && <option value={rule.topic} disabled>{t('lagConfig.missing', { name: rule.topic })}</option>}
              {topics.map(topic => <option key={topic} value={topic}>{topic}</option>)}
            </select></label>
            <label>{t('feishuConfig.threshold')}<input className="feishu-control feishu-threshold" type="number" min="0" step="1" value={Number.isNaN(rule.lagThreshold) ? '' : rule.lagThreshold}
              disabled={!!busy} aria-describedby={validationId} onChange={/* 在Lag 阈值编辑时保留空输入为无效数值，交由表单阻止非法保存。 */ event => updateRule(index, { lagThreshold: event.target.value === '' ? NaN : Number(event.target.value) })} /></label>
            <label>{t('feishuConfig.duration')}<input className="feishu-control feishu-duration" type="number" min="10" step="1" value={Number.isNaN(rule.growthSeconds) ? '' : rule.growthSeconds}
              disabled={!!busy} aria-describedby={validationId} onChange={/* 在持续时间编辑时保留空输入为无效数值，并由至少十秒的校验限制保存。 */ event => updateRule(index, { growthSeconds: event.target.value === '' ? NaN : Number(event.target.value) })} /></label>
            <label className="feishu-checkbox"><input type="checkbox" checked={rule.enabled} disabled={!!busy} onChange={/* 在启用复选框变化时只修改当前规则开关，允许停用不可用资源的旧规则。 */ event => updateRule(index, { enabled: event.target.checked })} />{t('feishuConfig.enabled')}</label>
            <button className="danger-button" type="button" disabled={!!busy} onClick={/* 在删除规则按钮点击时移除当前草稿项并清除旧反馈，等待保存才持久化。 */ () => { setRules(current => current.filter((_, i) => i !== index)); setFeedback(null) }}>{t('feishuConfig.delete')}</button>
          </fieldset>
        })}
        {!rules.length && <p className="form-note">{t('feishuConfig.empty')}</p>}
      </div>
    </section>
    {!!errors.length && <p role="alert" id={validationId}>{[...new Set(errors)].map(key => t(key)).join(' ')}</p>}
    {feedback && <div role={feedback.error ? 'alert' : 'status'}><p>{t(feedback.message)}</p>
      {Object.entries(serverFields).map(([field, message]) => <p key={field}>{message}</p>)}
    </div>}
    <div className="modal-actions feishu-form-actions">
      <button type="button" disabled={!canTest} onClick={/* 在点击测试通知时只测试当前新 Token 或已保存配置，不把新草稿持久化。 */ () => { void submit('test') }}>{busy === 'test' ? t('feishuConfig.sending') : t('feishuConfig.test')}</button>
      <div>{onClose && <button type="button" disabled={!!busy} onClick={onClose}>{t('feishuConfig.close')}</button>}
        <button type="submit" disabled={!valid || !!busy}>{busy === 'save' ? t('feishuConfig.saving') : t('feishuConfig.save')}</button></div>
    </div>
  </form>
}
