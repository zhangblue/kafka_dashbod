import { ApiError } from '../../api/client'
import { localizeApiError } from '../../api/errorMessages'
import { useTranslation } from 'react-i18next'
import { useId } from 'react'
import type { ClusterSnapshot, LagRule, LagRuleSet, LagThresholds } from '../../api/types'
import { snapshotUnavailable } from './snapshotAvailability'

// 在规则表单渲染和提交前检查安全整数、阈值顺序与非负持续时间。
function validThresholds(value: LagThresholds) {
  return Number.isSafeInteger(value.normalMax) && value.normalMax >= 0
    && Number.isSafeInteger(value.highMin) && value.highMin > value.normalMax
    && Number.isSafeInteger(value.overLimitSeconds) && value.overLimitSeconds >= 0
}

// 在默认或自定义规则行渲染时复用三项数值输入及其可访问错误关联。
function ThresholdFields({ value, onChange, disabled = false, getAttributes }: {
  value: LagThresholds; onChange: (value: LagThresholds) => void; disabled?: boolean
  getAttributes: (key: keyof LagThresholds) => { 'aria-describedby': string | undefined; 'aria-invalid': boolean }
}) {
  const { t } = useTranslation()
  const id = useId()
  return <>{([
    ['normalMax', t('lagConfig.normal')], ['highMin', t('lagConfig.high')], ['overLimitSeconds', t('lagConfig.duration')],
  ] as const).map(([key, label]) => <label key={key} htmlFor={`${id}-${key}`}>{label}
    <input id={`${id}-${key}`} type="number" min="0" step="1" disabled={disabled}
      {...getAttributes(key)}
      value={Number.isNaN(value[key]) ? '' : value[key]}
      onChange={/* 在控件变更时将当前值交回受控表单，由统一校验决定是否可保存。 */ event => onChange({ ...value, [key]: event.target.value === '' ? NaN : Number(event.target.value) })} />
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

// 在规则编辑阶段维护默认阈值与精确 Group 加 Topic 覆盖规则，按快照限制可选资源。
export function LagRulesForm({ clusterId, snapshot, value, onChange, onSave, saving = false, fieldErrors = {}, fieldErrorIdPrefix }: LagRulesFormProps) {
  const { t } = useTranslation()
  const id = useId()
  const available = !!snapshot && (!clusterId || snapshot.clusterId === clusterId) && !snapshotUnavailable(snapshot)
  const groups = available ? snapshot.consumerGroupIds : []
  // 在规则下拉渲染时筛选当前 Group 已有有效提交位置的 Topic，避免无效组合。
  const topicsFor = (groupId: string) => available ? [...new Set(snapshot.consumerGroups
    .filter(group => group.groupId === groupId && group.partitions.some(partition => partition.committedOffset !== null && partition.committedOffset >= 0))
    .map(group => group.topic))] : []
  // 在用户修改规则字段时只合并指定覆盖项并通知父级保存草稿。
  const updateRule = (index: number, patch: Partial<LagRule>) => onChange({ ...value,
    overrides: value.overrides.map((rule, i) => i === index ? { ...rule, ...patch } : rule) })
  const pairs = new Set(value.overrides.map(rule => JSON.stringify([rule.groupId, rule.topic])))
  const valid = validThresholds(value.defaults) && pairs.size === value.overrides.length
    && value.overrides.every(rule => rule.groupId && rule.topic && validThresholds(rule))
  // 在字段渲染时把本地和服务端错误关联到对应控件。
  const fieldAttributes = (path: string) => ({
    'aria-describedby': [!valid ? `${id}-validation` : '', fieldErrors[path] && fieldErrorIdPrefix ? `${fieldErrorIdPrefix}-${path}` : ''].filter(Boolean).join(' ') || undefined,
    'aria-invalid': !!fieldErrors[path],
  })

  return <form className="lag-rules-form" onSubmit={/* 在表单提交时，提交当前受控草稿，由父级统一管理保存与错误反馈。 */ event => { event.preventDefault(); if (valid && !saving) onSave?.(value) }}>
    <fieldset className="default-rules" disabled={saving || !available}>
      <legend>{t('lagConfig.defaults')}</legend>
      <ThresholdFields value={value.defaults} getAttributes={/* 在规则控件渲染时计算当前字段错误关联，便于定位服务端校验问题。 */ key => fieldAttributes(`defaults.${key}`)} onChange={/* 在控件变更时将当前值交回受控表单，由统一校验决定是否可保存。 */ defaults => onChange({ ...value, defaults })} />
    </fieldset>
    {validThresholds(value.defaults) && <div className="range-summary">
      <div className="range-bar" aria-hidden="true"><i /><i /><i /></div>
      <div className="range-labels"><span>{t('lagConfig.normalRange', { max: value.defaults.normalMax.toLocaleString('en-US') })}</span><span>{t('lagConfig.backlogRange', { range: value.defaults.highMin === value.defaults.normalMax + 1 ? t('lagConfig.emptyRange') : `${(value.defaults.normalMax + 1).toLocaleString('en-US')}–${(value.defaults.highMin - 1).toLocaleString('en-US')}` })}</span><span>{t('lagConfig.highRange', { min: value.defaults.highMin.toLocaleString('en-US') })}</span></div>
    </div>}
    <p className="form-note">{t('lagConfig.durationNote')}</p>
    <section aria-labelledby={`${id}-title`}>
      <h3 id={`${id}-title`}>{t('lagConfig.custom')}</h3>
      {!available && <p role="status">{t('lagConfig.unavailable')}</p>}
      {available && !!snapshot?.partialFailures.length && <p role="status">{t('lagConfig.partial')}</p>}
      {!!snapshot?.partialFailures.length && <ul aria-label={t('lagConfig.failures')}>{snapshot.partialFailures.map((failure, index) =>
        <li key={index}>{failure.scope} · {failure.resource} · {failure.code}: {localizeApiError(new ApiError(0, { code: failure.code }), t).message}</li>)}</ul>}
      {value.overrides.map((rule, index) => {
        const topics = topicsFor(rule.groupId)
        const groupMissing = !!rule.groupId && !groups.includes(rule.groupId)
        const topicMissing = !!rule.topic && !topics.includes(rule.topic)
        const unavailable = !available || groupMissing || topicMissing
        return <fieldset className="rule-row" key={rule.id ?? index}>
          <legend>{t('lagConfig.rule', { number: index + 1 })}</legend>
          <label htmlFor={`${id}-group-${index}`}>Consumer Group
          <select id={`${id}-group-${index}`} aria-label="Consumer Group" value={rule.groupId} disabled={unavailable || saving}
            {...fieldAttributes(`overrides.${index}.groupId`)}
            onChange={/* 在字段变更时，更新消费组并清空旧 Topic，避免沿用跨组无效组合。 */ event => updateRule(index, { groupId: event.target.value, topic: '' })}>
            <option value="">{t('lagConfig.chooseGroup')}</option>
            {groupMissing && <option value={rule.groupId} disabled>{t('lagConfig.missing', { name: rule.groupId })}</option>}
            {groups.map(group => <option key={group} value={group}>{group}</option>)}
          </select></label>
          <label htmlFor={`${id}-topic-${index}`}>Topic
          <select id={`${id}-topic-${index}`} aria-label="Topic" value={rule.topic} disabled={unavailable || !rule.groupId || saving}
            {...fieldAttributes(`overrides.${index}.topic`)}
            onChange={/* 在Topic 选择变化时更新指定规则的组合草稿。 */ event => updateRule(index, { topic: event.target.value })}>
            <option value="">{t('lagConfig.chooseTopic')}</option>
            {topicMissing && <option value={rule.topic} disabled>{t('lagConfig.missing', { name: rule.topic })}</option>}
            {topics.map(topic => <option key={topic} value={topic}>{topic}</option>)}
          </select></label>
          <ThresholdFields value={rule} getAttributes={/* 在规则控件渲染时计算当前字段错误关联，便于定位服务端校验问题。 */ key => fieldAttributes(`overrides.${index}.${key}`)} disabled={unavailable || saving} onChange={/* 在规则阈值变更时合并当前覆盖项数值，保留原 Group 与 Topic 标识。 */ thresholds => updateRule(index, thresholds)} />
          <button type="button" className="danger-button" disabled={saving} onClick={/* 在删除规则按钮点击时从覆盖草稿移除当前项，允许清理已不可用规则。 */ () => onChange({ ...value, overrides: value.overrides.filter((_, i) => i !== index) })}>{t('lagConfig.delete')}</button>
        </fieldset>
      })}
      <button type="button" disabled={!available || saving} onClick={/* 点击新增规则时追加待选择 Group / Topic 的覆盖草稿，初始阈值和持续时间沿用当前默认规则。 */ () => onChange({ ...value,
        overrides: [...value.overrides, { groupId: '', topic: '', ...value.defaults }] })}>{t('lagConfig.add')}</button>
    </section>
    {!valid && <p id={`${id}-validation`} role="alert">{t('lagConfig.validation')}</p>}
    <button type="submit" disabled={!valid || saving}>{t('lagConfig.save')}</button>
  </form>
}
