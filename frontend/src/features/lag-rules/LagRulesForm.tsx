import { useId } from 'react'
import type { ClusterSnapshot, LagRule, LagRuleSet, LagThresholds } from '../../api/types'
import { snapshotUnavailable } from './snapshotAvailability'

function validThresholds(value: LagThresholds) {
  return Number.isSafeInteger(value.normalMax) && value.normalMax >= 0
    && Number.isSafeInteger(value.highMin) && value.highMin > value.normalMax
    && Number.isSafeInteger(value.overLimitSeconds) && value.overLimitSeconds >= 0
}

function ThresholdFields({ value, onChange, disabled = false, getAttributes }: {
  value: LagThresholds; onChange: (value: LagThresholds) => void; disabled?: boolean
  getAttributes: (key: keyof LagThresholds) => { 'aria-describedby': string | undefined; 'aria-invalid': boolean }
}) {
  const id = useId()
  return <>{([
    ['normalMax', '正常上限'], ['highMin', '高 Lag 起始值'], ['overLimitSeconds', '持续超限时间（秒）'],
  ] as const).map(([key, label]) => <label key={key} htmlFor={`${id}-${key}`}>{label}
    <input id={`${id}-${key}`} type="number" min="0" step="1" disabled={disabled}
      {...getAttributes(key)}
      value={Number.isNaN(value[key]) ? '' : value[key]}
      onChange={event => onChange({ ...value, [key]: event.target.value === '' ? NaN : Number(event.target.value) })} />
  </label>)}</>
}

export interface LagRulesFormProps {
  clusterId?: string
  snapshot: ClusterSnapshot | null
  value: LagRuleSet
  onChange: (value: LagRuleSet) => void
  onSave?: (value: LagRuleSet) => void
  saving?: boolean
  fieldErrors?: Record<string, string>
  fieldErrorIdPrefix?: string
}

export function LagRulesForm({ clusterId, snapshot, value, onChange, onSave, saving = false, fieldErrors = {}, fieldErrorIdPrefix }: LagRulesFormProps) {
  const id = useId()
  const available = !!snapshot && (!clusterId || snapshot.clusterId === clusterId) && !snapshotUnavailable(snapshot)
  const groups = available ? snapshot.consumerGroupIds : []
  const topicsFor = (groupId: string) => available ? [...new Set(snapshot.consumerGroups
    .filter(group => group.groupId === groupId && group.partitions.some(partition => partition.committedOffset !== null && partition.committedOffset >= 0))
    .map(group => group.topic))] : []
  const updateRule = (index: number, patch: Partial<LagRule>) => onChange({ ...value,
    overrides: value.overrides.map((rule, i) => i === index ? { ...rule, ...patch } : rule) })
  const pairs = new Set(value.overrides.map(rule => JSON.stringify([rule.groupId, rule.topic])))
  const valid = validThresholds(value.defaults) && pairs.size === value.overrides.length
    && value.overrides.every(rule => rule.groupId && rule.topic && validThresholds(rule))
  const fieldAttributes = (path: string) => ({
    'aria-describedby': [!valid ? `${id}-validation` : '', fieldErrors[path] && fieldErrorIdPrefix ? `${fieldErrorIdPrefix}-${path}` : ''].filter(Boolean).join(' ') || undefined,
    'aria-invalid': !!fieldErrors[path],
  })

  return <form className="lag-rules-form" onSubmit={event => { event.preventDefault(); if (valid && !saving) onSave?.(value) }}>
    <fieldset className="default-rules" disabled={saving || !available}>
      <legend>集群默认规则</legend>
      <ThresholdFields value={value.defaults} getAttributes={key => fieldAttributes(`defaults.${key}`)} onChange={defaults => onChange({ ...value, defaults })} />
    </fieldset>
    {validThresholds(value.defaults) && <div className="range-summary">
      <div className="range-bar" aria-hidden="true"><i /><i /><i /></div>
      <div className="range-labels"><span>正常 · 0–{value.defaults.normalMax.toLocaleString('en-US')}</span><span>积压 · {value.defaults.highMin === value.defaults.normalMax + 1 ? '无整数区间' : `${(value.defaults.normalMax + 1).toLocaleString('en-US')}–${(value.defaults.highMin - 1).toLocaleString('en-US')}`}</span><span>高 Lag · ≥{value.defaults.highMin.toLocaleString('en-US')}</span></div>
    </div>}
    <p className="form-note">持续超限只延迟“积压”和“高 Lag”的状态切换。Lag 恢复正常后计时清零；采集异常与未提交立即生效。</p>
    <section aria-labelledby={`${id}-title`}>
      <h3 id={`${id}-title`}>自定义Lag状态判定区间</h3>
      {!available && <p role="status">集群不可达，请重新连接后新增或更换 Consumer Group / Topic。</p>}
      {available && !!snapshot?.partialFailures.length && <p role="status">采集数据不完整，选项仅包含已发现的数据。</p>}
      {!!snapshot?.partialFailures.length && <ul aria-label="采集失败原因">{snapshot.partialFailures.map((failure, index) =>
        <li key={index}>{failure.scope} · {failure.resource} · {failure.code}: {failure.message}</li>)}</ul>}
      {value.overrides.map((rule, index) => {
        const topics = topicsFor(rule.groupId)
        const groupMissing = !!rule.groupId && !groups.includes(rule.groupId)
        const topicMissing = !!rule.topic && !topics.includes(rule.topic)
        const unavailable = !available || groupMissing || topicMissing
        return <fieldset className="rule-row" key={rule.id ?? index}>
          <legend>规则 {index + 1}</legend>
          <label htmlFor={`${id}-group-${index}`}>Consumer Group
          <select id={`${id}-group-${index}`} aria-label="Consumer Group" value={rule.groupId} disabled={unavailable || saving}
            {...fieldAttributes(`overrides.${index}.groupId`)}
            onChange={event => updateRule(index, { groupId: event.target.value, topic: '' })}>
            <option value="">请选择 Consumer Group</option>
            {groupMissing && <option value={rule.groupId} disabled>{rule.groupId}（当前不可用）</option>}
            {groups.map(group => <option key={group} value={group}>{group}</option>)}
          </select></label>
          <label htmlFor={`${id}-topic-${index}`}>Topic
          <select id={`${id}-topic-${index}`} aria-label="Topic" value={rule.topic} disabled={unavailable || !rule.groupId || saving}
            {...fieldAttributes(`overrides.${index}.topic`)}
            onChange={event => updateRule(index, { topic: event.target.value })}>
            <option value="">请选择 Topic</option>
            {topicMissing && <option value={rule.topic} disabled>{rule.topic}（当前不可用）</option>}
            {topics.map(topic => <option key={topic} value={topic}>{topic}</option>)}
          </select></label>
          <ThresholdFields value={rule} getAttributes={key => fieldAttributes(`overrides.${index}.${key}`)} disabled={unavailable || saving} onChange={thresholds => updateRule(index, thresholds)} />
          <button type="button" className="danger-button" disabled={saving} onClick={() => onChange({ ...value, overrides: value.overrides.filter((_, i) => i !== index) })}>删除规则</button>
        </fieldset>
      })}
      <button type="button" disabled={!available || saving} onClick={() => onChange({ ...value,
        overrides: [...value.overrides, { groupId: '', topic: '', ...value.defaults }] })}>新增规则</button>
    </section>
    {!valid && <p id={`${id}-validation`} role="alert">请填写完整规则，确保组合唯一、数值非负且高 Lag 起始值大于正常上限。</p>}
    <button type="submit" disabled={!valid || saving}>保存规则</button>
  </form>
}
