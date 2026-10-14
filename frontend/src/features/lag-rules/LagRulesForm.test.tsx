import { useState } from 'react'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { describe, expect, it } from 'vitest'
import { LagRulesForm } from './LagRulesForm'
import type { ClusterSnapshot, LagRuleSet } from '../../api/types'
import { rulesFixture, snapshotFixture } from '../../test/handlers'

function Harness({ initial = rulesFixture, snapshot = snapshotFixture }: { initial?: LagRuleSet; snapshot?: ClusterSnapshot | null }) {
  const [value, onChange] = useState<LagRuleSet>(initial)
  return <LagRulesForm snapshot={snapshot} value={value} onChange={onChange} onSave={() => {}} />
}

describe('Lag rule configuration', () => {
  it('limits topics to committed offsets in the selected group and resets topic on group change', async () => {
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

  it('disables saving duplicate pairs, invalid thresholds and empty numeric inputs', async () => {
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
    await user.clear(defaults.getByLabelText('高 Lag 起始值'))
    await user.type(defaults.getByLabelText('高 Lag 起始值'), '5000')
    await user.clear(defaults.getByLabelText('持续超限时间（秒）'))
    expect(screen.getByRole('button', { name: '保存规则' })).toBeDisabled()
  })

  it('marks unavailable saved values read-only and permits deleting them', async () => {
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

  it('keeps all old rules read-only and blocks additions while cluster is unreachable', () => {
    render(<Harness snapshot={null} initial={{ ...rulesFixture, overrides: [{ id: 'saved', groupId: 'flink-order-enrich', topic: 'raw-orders', ...rulesFixture.defaults }] }} />)
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
    expect(screen.getByLabelText('Consumer Group')).toBeDisabled()
    expect(screen.getByLabelText('Topic')).toBeDisabled()
    expect(within(screen.getByRole('group', { name: '规则 1' })).getByLabelText('持续超限时间（秒）')).toBeDisabled()
    expect(within(screen.getByRole('group', { name: '集群默认规则' })).getByLabelText('正常上限')).toBeDisabled()
    expect(screen.getByText(/重新连接/)).toBeInTheDocument()
  })

  it('rejects a snapshot from another cluster for dropdown choices', () => {
    render(<LagRulesForm clusterId="cluster-b" snapshot={snapshotFixture} value={rulesFixture} onChange={() => {}} />)
    expect(screen.getByRole('button', { name: '新增规则' })).toBeDisabled()
  })
})
