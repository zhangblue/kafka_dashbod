import i18n from '../../i18n'
import { useState } from 'react'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { LagRulesForm } from './LagRulesForm'
import type { ClusterSnapshot, LagRuleSet } from '../../api/types'
import { rulesFixture, snapshotFixture } from '../../test/handlers'

// 在规则表单测试挂载时保存受控草稿，让用户操作触发表单重渲染。
function Harness({ initial = rulesFixture, snapshot = snapshotFixture }: { initial?: LagRuleSet; snapshot?: ClusterSnapshot | null }) {
  const [value, onChange] = useState<LagRuleSet>(initial)
  return <LagRulesForm snapshot={snapshot} value={value} onChange={onChange} onSave={/* 在请求保存时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />
}

describe('Lag rule configuration', /* 在测试组注册阶段组织当前功能的交互与边界回归。 */ () => {
  it('limits topics to committed offsets in the selected group and resets topic on group change', /* 在本用例执行时验证Topic 仅来自选中 Group 的有效提交，换组清空 Topic。 */ async () => {
    const user = userEvent.setup()
    render(<Harness />)
    await user.click(screen.getByRole('button', { name: '新增规则' }))
    expect(screen.getByLabelText('Topic')).toBeDisabled()
    await user.selectOptions(screen.getByLabelText('Consumer Group'), 'flink-order-enrich')
    expect(screen.getByRole('option', { name: 'raw-orders' })).toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'audit-log' })).not.toBeInTheDocument()
    expect(screen.queryByRole('option', { name: 'uncommitted' })).not.toBeInTheDocument()
    await user.selectOptions(screen.getByLabelText('Topic'), 'raw-orders')
    await user.selectOptions(screen.getByLabelText('Consumer Group'), 'audit-worker')
    expect(screen.getByLabelText('Topic')).toHaveValue('')
    expect(screen.queryByRole('option', { name: 'raw-orders' })).not.toBeInTheDocument()
    expect(screen.getByRole('option', { name: 'audit-log' })).toBeInTheDocument()
  })

  it('disables saving duplicate pairs, invalid thresholds and empty numeric inputs', /* 在本用例执行时验证重复组合、非法阈值和空数值禁止保存。 */ async () => {
    const user = userEvent.setup()
    const rule = { groupId: 'flink-order-enrich', topic: 'raw-orders', ...rulesFixture.defaults }
    render(<Harness initial={{ ...rulesFixture, overrides: [rule, rule] }} />)
    expect(screen.getByRole('button', { name: '保存规则' })).toBeDisabled()
    await user.click(screen.getAllByRole('button', { name: '删除规则' })[1])
    expect(screen.getByRole('button', { name: '保存规则' })).toBeEnabled()
    const defaults = within(screen.getByRole('group', { name: '集群默认规则' }))
    await user.clear(defaults.getByLabelText('高 Lag 起始值'))
    await user.type(defaults.getByLabelText('高 Lag 起始值'), '999')
    expect(screen.getByRole('button', { name: '保存规则' })).toBeDisabled()
    expect(defaults.getByLabelText('高 Lag 起始值')).toHaveAccessibleDescription(/高 Lag 起始值大于正常上限/)
    await user.clear(defaults.getByLabelText('高 Lag 起始值'))
    await user.type(defaults.getByLabelText('高 Lag 起始值'), '5000')
    await user.clear(defaults.getByLabelText('持续超限时间（秒）'))
    expect(screen.getByRole('button', { name: '保存规则' })).toBeDisabled()
  })

  it('marks unavailable saved values read-only and permits deleting them', /* 在本用例执行时验证不可用旧值只读但仍允许删除规则。 */ async () => {
    const user = userEvent.setup()
    render(<Harness initial={{ ...rulesFixture, overrides: [{ id: 'saved', groupId: 'gone', topic: 'gone-topic', ...rulesFixture.defaults }] }} />)
    expect(screen.getByRole('option', { name: 'gone（当前不可用）' })).toBeInTheDocument()
    expect(screen.getByLabelText('Consumer Group')).toBeDisabled()
    expect(screen.getByLabelText('Topic')).toBeDisabled()
    const rule = within(screen.getByRole('group', { name: '规则 1' }))
    expect(rule.getByLabelText('正常上限')).toBeDisabled()
    await user.click(screen.getByRole('button', { name: '删除规则' }))
    expect(screen.queryByLabelText('Consumer Group')).not.toBeInTheDocument()
  })

  it('keeps all old rules read-only and blocks additions while cluster is unreachable', /* 在本用例执行时验证集群不可达时旧规则只读并禁止新增。 */ () => {
    render(<Harness snapshot={null} initial={{ ...rulesFixture, overrides: [{ id: 'saved', groupId: 'flink-order-enrich', topic: 'raw-orders', ...rulesFixture.defaults }] }} />)
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
    expect(screen.getByLabelText('Consumer Group')).toBeDisabled()
    expect(screen.getByLabelText('Topic')).toBeDisabled()
    expect(within(screen.getByRole('group', { name: '规则 1' })).getByLabelText('持续超限时间（秒）')).toBeDisabled()
    expect(within(screen.getByRole('group', { name: '集群默认规则' })).getByLabelText('正常上限')).toBeDisabled()
    expect(screen.getByText(/重新连接/)).toBeInTheDocument()
  })

  it('rejects a snapshot from another cluster for dropdown choices', /* 在本用例执行时验证其他集群快照不能作为下拉选项来源。 */ () => {
    render(<LagRulesForm clusterId="cluster-b" snapshot={snapshotFixture} value={rulesFixture} onChange={/* 在字段变更时，提供无副作用的受控回调替身，让测试聚焦当前表单行为。 */ () => {}} />)
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
  })

  it('treats a successful HTTP snapshot with total discovery failures as unreachable', /* 在本用例执行时验证发现全部失败时即使 HTTP 成功也视为不可达。 */ () => {
    render(<Harness snapshot={{ ...snapshotFixture, consumerGroupIds: [], consumerGroups: [], partialFailures: [
      { scope: 'topics', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'Topic metadata failed' },
      { scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'Group discovery failed' },
    ] }} initial={{ ...rulesFixture, overrides: [{ id: 'saved', groupId: 'flink-order-enrich', topic: 'raw-orders', ...rulesFixture.defaults }] }} />)
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
    expect(screen.getByLabelText('Consumer Group')).toBeDisabled()
    expect(within(screen.getByRole('group', { name: '集群默认规则' })).getByLabelText('正常上限')).toBeDisabled()
    expect(screen.getAllByText(/Kafka 请求超时/)).toHaveLength(2)
    expect(screen.queryByText(/Topic metadata failed/)).not.toBeInTheDocument()
  })

  it('allows known group/topic choices while explaining partial collection failures', /* 在本用例执行时验证部分采集失败时保留已知选项并说明失败原因。 */ async () => {
    const user = userEvent.setup()
    render(<Harness snapshot={{ ...snapshotFixture, partialFailures: [
      { scope: 'watermark', resource: 'other:0', code: 'KAFKA_TIMEOUT', message: 'Partition query failed' },
    ] }} />)
    expect(screen.getByText(/不完整/)).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: '新增规则' }))
    await user.selectOptions(screen.getByLabelText('Consumer Group'), 'flink-order-enrich')
    expect(screen.getByRole('option', { name: 'raw-orders' })).toBeInTheDocument()
    expect(screen.getByLabelText('Topic')).toBeEnabled()
  })
})

// 验证默认范围、缺失资源和规则动作均响应 English，原始资源名不变。
it('localizes ranges and unavailable overrides in English', async () => {
  await i18n.changeLanguage('en')
  const user = userEvent.setup()
  render(<Harness initial={{ ...rulesFixture, overrides: [{ id: 'saved', groupId: 'gone', topic: 'gone-topic', ...rulesFixture.defaults }] }} />)
  expect(screen.getByRole('group', { name: 'Cluster default rules' })).toBeInTheDocument()
  expect(screen.getByText('Normal · 0–999')).toBeInTheDocument()
  expect(screen.getByText('Backlog · 1,000–4,999')).toBeInTheDocument()
  expect(screen.getByRole('option', { name: 'gone (unavailable)' })).toBeInTheDocument()
  expect(screen.getByLabelText('Consumer Group')).toBeDisabled()
  expect(within(screen.getByRole('group', { name: 'Rule 1' })).getByLabelText('Over-limit duration (seconds)')).toBeDisabled()
  await user.click(screen.getByRole('button', { name: 'Delete rule' }))
  await user.click(screen.getByRole('button', { name: 'Add rule' }))
  expect(screen.getByRole('button', { name: 'Save rules' })).toBeDisabled()
  expect(screen.getByRole('alert')).toHaveTextContent('Complete all rules')
})

// 验证采集失败按受控错误码翻译，远端原文不能覆盖 English 页面。
it('localizes collection failure messages without rendering remote text', async () => {
  await i18n.changeLanguage('en')
  render(<Harness snapshot={{ ...snapshotFixture, partialFailures: [
    { scope: 'watermark', resource: 'raw-orders', code: 'KAFKA_TIMEOUT', message: 'remote detail' },
  ] }} />)
  expect(screen.getByRole('list', { name: 'Collection failure reasons' })).toHaveTextContent('The Kafka request timed out.')
  expect(document.body.textContent?.includes('remote detail')).toBe(false)
})
