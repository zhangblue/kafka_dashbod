import { act, render, screen, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MemoryRouter, Outlet, Route, Routes } from 'react-router'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'
import i18n from '../../i18n'
import { lagRulesApi } from '../../api/client'
import type { ClusterSnapshot, ConsumerGroupTopicSnapshot } from '../../api/types'
import { clusterFixture } from '../../test/handlers'
import { ConsumerGroupLagProvider } from '../consumer-groups/ConsumerGroupLagProvider'
import type { DashboardContext } from '../dashboard/useDashboard'
import { OverviewPage } from './OverviewPage'
import { retainDisplaySnapshot } from '../dashboard/displaySnapshot'

vi.mock('echarts/core', /* 在测试模块准备阶段建立图表边界替身，记录生命周期调用而不依赖真实画布。 */ () => ({ init: /* 在页面测试挂载图表时返回最小图表替身，将断言聚焦页面数据与交互。 */ () => ({ /* 在页面测试更新图表时承接配置调用，避免执行真实画布绘制。 */ setOption() {}, /* 页面测试触发图表尺寸同步时接收调用，避免执行真实画布布局。 */ resize() {}, /* 在图表替身卸载阶段提供释放接口，避免依赖真实绘制环境。 */ dispose() {} }), /* 在页面测试加载图表模块时替代扩展注册，隔离真实图表运行环境。 */ use() {} }))
beforeEach(/* 在每个用例开始时重置环境和受控时钟，避免跨用例共享状态。 */ () => {
  vi.useFakeTimers({ toFake: ['Date', 'setInterval', 'clearInterval'] })
  vi.setSystemTime(new Date('2026-10-15T00:00:00Z'))
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue({ defaults: { normalMax: 999, highMin: 5000, overLimitSeconds: 0 }, overrides: [] })
})
afterEach(/* 在每个用例结束时恢复本用例修改的测试环境，避免拦截或模拟状态影响后续断言。 */ () => { vi.useRealTimers(); vi.restoreAllMocks() })

// 在页面测试准备时创建具有指定 Lag 的消费组行，保留分区及未知状态可控。
const row = (groupId: string, lag: number): ConsumerGroupTopicSnapshot => ({ groupId, topic: 'orders', lag,
  unknownPartitionCount: 0, incomplete: false, partitions: [{ partition: 0, high: 10000, committedOffset: 10000 - lag, lag }] })
// 在概览测试准备时构造确定的集群快照，覆盖汇总、排序及未知值边界。
function fixture(): ClusterSnapshot {
  return { clusterId: 'cluster-a', collectedAt: '2026-10-15T00:00:00Z',
    topics: [{ topic: 'orders', low: 0, latestMessageOffset: 9999, retainedOffsetSpan: 10000, incomplete: false,
      partitions: [{ partition: 0, low: 0, high: 10000, latestMessageOffset: 9999, retainedOffsetSpan: 10000 }] }],
    consumerGroupIds: ['normal', 'backlog', 'high', 'uncommitted', 'failed'],
    consumerGroups: [row('normal', 5), row('backlog', 1200), row('high', 6000),
      { ...row('uncommitted', 0), incomplete: true, unknownPartitionCount: 1, partitions: [{ partition: 0, high: 10000, committedOffset: null, lag: null }] },
      { ...row('failed', 0), incomplete: true, unknownPartitionCount: 1, partitions: [{ partition: 0, high: 10000, committedOffset: null, lag: null }] }],
    partialFailures: [{ scope: 'committed', resource: 'failed', code: 'TIMEOUT', message: 'timeout' }] }
}
// 在页面或表单测试准备时提供隔离的查询和路由上下文，支持交互后重渲染。
function mount(snapshot: ClusterSnapshot | null) {
  const context: DashboardContext = { cluster: clusterFixture, refreshSeconds: 5, monitor: { snapshot,
    history: snapshot ? [{ at: Date.parse(snapshot.collectedAt), snapshot }] : [], status: 'connected', error: null,
    isFetching: false, continuityKey: 'a', refresh: /* 在页面测试中提供立即完成的空刷新替身，不更新快照。 */ async () => {}, reconnect: vi.fn() } }
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  // 在测试挂载或重渲染时建立共享查询与路由树，保留同一组件实例状态。
  const tree = () => <QueryClientProvider client={client}><MemoryRouter><Routes><Route element={
    <ConsumerGroupLagProvider clusterId="cluster-a" monitor={context.monitor}><Outlet context={context} /></ConsumerGroupLagProvider>
  }><Route index element={<OverviewPage />} /></Route></Routes></MemoryRouter></QueryClientProvider>
  const view = render(tree())
  return { context, update: /* 在测试模拟采样或连接变化时修改上下文并重渲染，以验证连续性及 UI 更新。 */ (patch: Partial<DashboardContext['monitor']>, refreshSeconds = context.refreshSeconds) => {
    Object.assign(context.monitor, patch); context.refreshSeconds = refreshSeconds; view.rerender(tree())
  } }
}

it('aggregates known Lag, counts unique abnormal groups and orders actionable rows without classifying normal nonzero lag', /* 在本用例执行时验证汇总已知 Lag、去重异常组并排序，正常非零 Lag 不误报。 */ async () => {
  const snapshot = fixture()
  snapshot.consumerGroups.push({ ...row('high', 10), topic: 'secondary' })
  mount(snapshot)
  await screen.findByRole('table', { name: '异常 Consumer Groups' })
  expect(screen.getByRole('region', { name: 'Topic 数' })).toHaveTextContent('1')
  expect(screen.getByRole('region', { name: 'Consumer Group 数' })).toHaveTextContent('5')
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('7,215')
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('已知 Lag')
  expect(screen.getByRole('region', { name: '异常 Consumer Group 数' })).toHaveTextContent('4')
  const table = screen.getByRole('table', { name: '异常 Consumer Groups' })
  expect(within(table).queryByRole('link', { name: 'normal / orders' })).not.toBeInTheDocument()
  expect(within(table).getAllByRole('link').map(link => link.textContent)).toEqual(['high / orders', 'backlog / orders', 'failed / orders', 'uncommitted / orders'])
  expect(within(table).getByRole('link', { name: 'high / orders' })).toHaveAttribute('href', '/consumer-groups?groupId=high&topic=orders')
  expect(within(table).getByRole('row', { name: /failed \/ orders/ })).toHaveTextContent('数据已陈旧')
  expect(within(table).getByRole('row', { name: /uncommitted \/ orders/ })).not.toHaveTextContent('数据已陈旧')
  expect(screen.getByRole('table', { name: '最近 1 小时总 Lag 数据' })).toHaveTextContent('未知 / 不完整')
})

it('shows a skeleton only before the first sample and preserves the snapshot during refresh and disconnect', /* 在本用例执行时验证仅首个样本前显示骨架，刷新和断连保留已有快照。 */ async () => {
  const view = mount(null)
  expect(screen.getByLabelText('正在加载概览')).toBeInTheDocument()
  view.update({ snapshot: fixture() })
  await screen.findByRole('table', { name: '异常 Consumer Groups' })
  view.update({ isFetching: true })
  expect(screen.queryByLabelText('正在加载概览')).not.toBeInTheDocument()
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('7,205')
  view.update({ status: 'error', error: new Error('offline'), isFetching: false })
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('7,205')
  expect(screen.getByLabelText('图表数据已陈旧')).toBeInTheDocument()
})

it('marks data stale only after two current refresh intervals and recalculates when the interval changes', /* 在本用例执行时验证超过两个当前刷新间隔才陈旧，频率改变后重新计算。 */ async () => {
  const snapshot = fixture(); snapshot.consumerGroups = [row('normal', 5)]; snapshot.partialFailures = []
  const view = mount(snapshot)
  await screen.findByText('暂无异常 Consumer Groups。')
  expect(screen.queryByText('数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(10000))
  expect(screen.queryByText('数据已陈旧')).not.toBeInTheDocument()
  act(() => vi.advanceTimersByTime(1000))
  expect(screen.getByText('数据已陈旧')).toBeInTheDocument()
  view.update({}, 10)
  expect(screen.queryByText('数据已陈旧')).not.toBeInTheDocument()
})

it('shows no fabricated zero total when discovery fails without any known rows', /* 在本用例执行时验证发现失败且无已知行时不伪造零总量。 */ async () => {
  const snapshot = fixture(); snapshot.consumerGroups = []; snapshot.consumerGroupIds = []
  snapshot.partialFailures = [{ scope: 'groups', resource: 'cluster-a', code: 'TIMEOUT', message: 'timeout' }]
  mount(snapshot)
  const card = screen.getByRole('region', { name: '总 Lag' })
  expect(card).toHaveTextContent('—')
  expect(card).toHaveTextContent('不完整')
})

it('groups distribution by consumer group across topics and labels unknown lag instead of treating it as zero', /* 在本用例执行时验证跨 Topic 按消费组聚合分布并显式标记未知分区。 */ async () => {
  const snapshot = fixture()
  snapshot.consumerGroups.push({ ...row('high', 200), topic: 'secondary' })
  mount(snapshot)
  const distribution = await screen.findByRole('region', { name: 'Lag 分布' })
  expect(within(distribution).getByText('6,200')).toBeInTheDocument()
  expect(within(distribution).getAllByText('high')).toHaveLength(1)
  expect(distribution).toHaveTextContent('未知')
})

it('distinguishes fresh uncommitted data from stale collection failures', /* 在本用例执行时验证新鲜未提交与陈旧采集失败保持区别。 */ async () => {
  const snapshot = fixture()
  snapshot.consumerGroups = [snapshot.consumerGroups[3]]
  snapshot.partialFailures = []
  mount(snapshot)
  await screen.findByRole('table', { name: '异常 Consumer Groups' })
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('不完整')
  expect(screen.queryByLabelText('图表数据已陈旧')).not.toBeInTheDocument()
})

it.each(['topics', 'groups'])('does not count confirmed-absent group/topic rows as abnormal when %s discovery fails', /* 在本用例执行时验证发现失败时不把已确认不存在的行计入异常。 */ async scope => {
  const previous = fixture()
  previous.consumerGroups = [row('worker', 6000)]; previous.consumerGroupIds = ['worker']; previous.partialFailures = []
  const raw: ClusterSnapshot = { ...previous, topics: [], consumerGroupIds: [], consumerGroups: [],
    partialFailures: [{ scope, resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' }] }
  const display = retainDisplaySnapshot(raw, retainDisplaySnapshot(previous))
  const view = mount(raw)
  view.update({ displaySnapshot: display.snapshot, staleResources: display.staleResources })
  await screen.findByText('暂无异常 Consumer Groups。')
  expect(screen.queryByRole('link', { name: 'worker / orders' })).not.toBeInTheDocument()
  expect(screen.getByRole('region', { name: '异常 Consumer Group 数' })).toHaveTextContent('0')
  expect(screen.getByRole('region', { name: 'Consumer Group 数' })).toHaveTextContent(scope === 'groups' ? '1' : '0')
})

// 验证英文概览与空状态跟随语言切换，同时保留真实集群名和 Kafka 术语。
it('renders the English overview and empty attention state without translating the cluster name', async () => {
  await i18n.changeLanguage('en')
  const snapshot = fixture()
  snapshot.consumerGroups = [row('正常-group', 5)]
  snapshot.consumerGroupIds = ['正常-group']
  snapshot.partialFailures = []
  mount(snapshot)
  expect(screen.getByRole('heading', { name: 'Cluster Overview' })).toBeInTheDocument()
  expect(screen.getByText(`Live consumption status for ${clusterFixture.name}`)).toBeInTheDocument()
  expect(screen.getByRole('heading', { name: 'Topics' })).toBeInTheDocument()
  expect(screen.getByRole('heading', { name: 'Consumer Groups' })).toBeInTheDocument()
  expect(screen.getByRole('heading', { name: 'Total Lag' })).toBeInTheDocument()
  expect(screen.getByRole('heading', { name: 'Needs Attention' })).toBeInTheDocument()
  expect(await screen.findByText('No consumer groups need attention.')).toBeInTheDocument()
  expect(screen.getByRole('region', { name: 'Lag Distribution' })).toHaveTextContent('正常-group')
})

// 验证英文异常表和分布保留 Group/Topic 名，未知与不完整状态不被降成零值。
it('localizes incomplete overview data while preserving Group and Topic identifiers', async () => {
  await i18n.changeLanguage('en')
  const snapshot = fixture()
  snapshot.consumerGroups = [{ ...row('消费者-group', 12345), topic: '订单-topic', unknownPartitionCount: 1, incomplete: true }]
  snapshot.partialFailures = []
  mount(snapshot)
  const table = await screen.findByRole('table', { name: 'Abnormal Consumer Groups' })
  expect(within(table).getByRole('link', { name: '消费者-group / 订单-topic' })).toBeInTheDocument()
  expect(table).toHaveTextContent('12,345 + 1 unknown')
  expect(screen.getByRole('region', { name: 'Total Lag' })).toHaveTextContent('Known Lag (incomplete; unknown values excluded)')
  expect(screen.getByRole('region', { name: 'Lag Distribution' })).toHaveTextContent('Known Lag · includes unknown Partitions')
})
