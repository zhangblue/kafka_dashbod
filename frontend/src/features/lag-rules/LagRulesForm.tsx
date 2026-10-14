import { useId } from 'react'
import type { ClusterSnapshot, LagRule, LagRuleSet, LagThresholds } from '../../api/types'

function validThresholds(value: LagThresholds) {
  return Number.isSafeInteger(value.normalMax) && value.normalMax >= 0
    && Number.isSafeInteger(value.highMin) && value.highMin > value.normalMax
    && Number.isSafeInteger(value.overLimitSeconds) && value.overLimitSeconds >= 0
}

function ThresholdFields({ value, onChange, disabled = false }: {
  value: LagThresholds; onChange: (value: LagThresholds) => void; disabled?: boolean
}) {
  const id = useId()
  return <>{([
    ['normalMax', '正常上限'], ['highMin', '高 Lag 起始值'], ['overLimitSeconds', '持续超限时间（秒）'],
  ] as const).map(([key, label]) => <label key={key} htmlFor={`${id}-${key}`}>{label}
    <input id={`${id}-${key}`} type="number" min="0" step="1" disabled={disabled}
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
}

export function LagRulesForm({ clusterId, snapshot, value, onChange, onSave, saving = false }: LagRulesFormProps) {
  const id = useId()
  const available = !!snapshot && (!clusterId || snapshot.clusterId === clusterId)
  const groups = available ? snapshot.consumerGroupIds : []
  const topicsFor = (groupId: string) => available ? [...new Set(snapshot.consumerGroups
    .filter(group => group.groupId === groupId && group.partitions.some(partition => partition.committedOffset !== null && partition.committedOffset >= 0))
    .map(group => group.topic))] : []
  const updateRule = (index: number, patch: Partial<LagRule>) => onChange({ ...value,
    overrides: value.overrides.map((rule, i) => i === index ? { ...rule, ...patch } : rule) })
  const pairs = new Set(value.overrides.map(rule => JSON.stringify([rule.groupId, rule.topic])))
  const valid = validThresholds(value.defaults) && pairs.size === value.overrides.length
    && value.overrides.every(rule => rule.groupId && rule.topic && validThresholds(rule))

  return <form onSubmit={event => { event.preventDefault(); if (valid && !saving) onSave?.(value) }}>
    <fieldset disabled={saving || !available}>
      <legend>集群默认规则</legend>
      <ThresholdFields value={value.defaults} onChange={defaults => onChange({ ...value, defaults })} />
    </fieldset>
    <section aria-labelledby={`${id}-title`}>
      <h3 id={`${id}-title`}>自定义Lag状态判定区间</h3>
      {!available && <p role="status">集群不可达，请重新连接后新增或更换 Consumer Group / Topic。</p>}
      {value.overrides.map((rule, index) => {
        const topics = topicsFor(rule.groupId)
        const groupMissing = !!rule.groupId && !groups.includes(rule.groupId)
        const topicMissing = !!rule.topic && !topics.includes(rule.topic)
        const unavailable = !available || groupMissing || topicMissing
        return <fieldset key={rule.id ?? index}>
          <legend>规则 {index + 1}</legend>
          <label htmlFor={`${id}-group-${index}`}>Consumer Group</label>
          <select id={`${id}-group-${index}`} value={rule.groupId} disabled={unavailable || saving}
            onChange={event => updateRule(index, { groupId: event.target.value, topic: '' })}>
            <option value="">请选择 Consumer Group</option>
            {groupMissing && <option value={rule.groupId} disabled>{rule.groupId}（当前不可用）</option>}
            {groups.map(group => <option key={group} value={group}>{group}</option>)}
          </select>
          <label htmlFor={`${id}-topic-${index}`}>Topic</label>
          <select id={`${id}-topic-${index}`} value={rule.topic} disabled={unavailable || !rule.groupId || saving}
            onChange={event => updateRule(index, { topic: event.target.value })}>
            <option value="">请选择 Topic</option>
            {topicMissing && <option value={rule.topic} disabled>{rule.topic}（当前不可用）</option>}
            {topics.map(topic => <option key={topic} value={topic}>{topic}</option>)}
          </select>
          <ThresholdFields value={rule} disabled={unavailable || saving} onChange={thresholds => updateRule(index, thresholds)} />
          <button type="button" disabled={saving} onClick={() => onChange({ ...value, overrides: value.overrides.filter((_, i) => i !== index) })}>删除规则</button>
        </fieldset>
      })}
      <button type="button" disabled={!available || saving} onClick={() => onChange({ ...value,
        overrides: [...value.overrides, { groupId: '', topic: '', ...value.defaults }] })}>新增规则</button>
    </section>
    {!valid && <p role="alert">请填写完整规则，确保组合唯一、数值非负且高 Lag 起始值大于正常上限。</p>}
    <button type="submit" disabled={!valid || saving}>保存规则</button>
  </form>
}
