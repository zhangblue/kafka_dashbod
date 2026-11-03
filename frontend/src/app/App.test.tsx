import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import App from './App'
import userEvent from '@testing-library/user-event'
import { clustersApi, feishuAlertsApi, lagRulesApi, sessionsApi } from '../api/client'
import { clusterFixture, rulesFixture, snapshotFixture } from '../test/handlers'
import type { ClusterSnapshot } from '../api/types'

// 路由测试无画布上下文，图表替身只承接调用，图表生命周期由专属用例覆盖。
vi.mock('echarts/core', () => ({ /* 页面创建图表时返回无绘制替身。 */ init: () => ({ /* 图表更新时接收配置，避免访问画布。 */ setOption() {}, /* 容器变化时接收尺寸同步调用。 */ resize() {}, /* 图表卸载时接收释放调用。 */ dispose() {} }), /* 模块加载时接收图表插件注册。 */ use() {} }))

// 每个路由用例结束后恢复替身并返回根路径，避免下个用例继承导航位置。
afterEach(() => { vi.restoreAllMocks(); window.history.replaceState({}, '', '/') })

// 验证应用导航与监控管理文案完整，避免仍显示已失效的只读模式。
it('renders the dashboard navigation', () => {
  vi.spyOn(clustersApi, 'list').mockResolvedValue([])
  render(<App />)
  expect(screen.getByText('Kafka Lens')).toBeInTheDocument()
  expect(screen.getByText('Cluster observability')).toBeInTheDocument()
  expect(screen.getByRole('complementary', { name: 'Kafka Lens' })).toHaveTextContent('监控与 Topic 管理')
  expect(screen.getByRole('complementary', { name: 'Kafka Lens' })).not.toHaveTextContent('只读模式')
  expect(screen.getByRole('navigation')).toBeInTheDocument()
  expect(screen.getByRole('link', { name: '概览' })).toBeInTheDocument()
  expect(screen.getByText('Topics')).toBeInTheDocument()
  expect(screen.getByText('Consumer Groups')).toBeInTheDocument()
  expect(screen.getByText('集群设置')).toBeInTheDocument()
})

// 验证进入设置会释放监控会话，直接打开设置也不会额外建立连接。
it('opens settings without retaining a dashboard session and supports direct settings entry', async () => {
  const closed: string[] = []
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'monitor', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'snapshot').mockResolvedValue(snapshotFixture)
  vi.spyOn(sessionsApi, 'close').mockImplementation(async id => { closed.push(id) })
  const first = render(<App />)
  await screen.findByText('已连接')
  fireEvent.click(screen.getByRole('link', { name: '集群设置' }))
  expect(await screen.findByRole('button', { name: '新增集群' })).toBeInTheDocument()
  expect(closed).toEqual(['monitor'])
  first.unmount()
  vi.mocked(sessionsApi.create).mockClear()
  render(<App />)
  expect(await screen.findByRole('button', { name: '新增集群' })).toBeInTheDocument()
  expect(sessionsApi.create).not.toHaveBeenCalled()
})

// 验证 Consumer Groups 路由消费真实快照行，避免页面与布局上下文断开。
it('opens the Consumer Groups route with real snapshot rows', async () => {
  window.history.replaceState({}, '', '/consumer-groups')
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue(rulesFixture)
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'groups', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'snapshot').mockResolvedValue(snapshotFixture)
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  render(<App />)
  expect(await screen.findByRole('table', { name: 'Consumer Groups 汇总' })).toBeInTheDocument()
  expect(screen.getByRole('row', { name: /audit-worker.*audit-log/ })).toBeInTheDocument()
})

// 防止持续连接期间将 Lag 状态放在会卸载的子路由中，造成页面切换后丢失累计时间。
// 验证跨 Topics 路由仍保持 Lag 持续时间与趋势，且不会重复累计保留样本。
it('preserves lag duration and trend across Topics navigation without replaying retained samples', async () => {
  window.history.replaceState({}, '', '/consumer-groups')
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue(rulesFixture)
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'groups', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  let seconds = 0
  let lag = 6000
  vi.spyOn(sessionsApi, 'snapshot').mockImplementation(async () => ({ ...snapshotFixture,
    collectedAt: new Date(Date.parse(snapshotFixture.collectedAt) + seconds * 1000).toISOString(),
    consumerGroups: [{ groupId: 'busy-worker', topic: 'events', lag, unknownPartitionCount: 0, incomplete: false,
      partitions: [{ partition: 0, high: 10000, committedOffset: 10000 - lag, lag }] }],
  }))
  render(<App />)
  // 断言消费组状态时定位当前行，避免路由切换后继续引用旧 DOM。
  const dataRow = () => screen.getByRole('row', { name: /busy-worker/ })
  await screen.findByRole('table', { name: 'Consumer Groups 汇总' })
  // 趋势用例推进模拟时间与 Lag 后手动刷新，并等待本次快照和按钮状态更新。
  const refresh = async (nextSeconds: number, nextLag: number) => {
    seconds = nextSeconds; lag = nextLag
    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }))
    await waitFor(() => expect(screen.getByLabelText('最后更新时间')).toHaveAttribute('datetime', new Date(Date.parse(snapshotFixture.collectedAt) + seconds * 1000).toISOString()))
    await waitFor(() => expect(screen.getByRole('button', { name: '手动刷新' })).toBeEnabled())
  }
  await refresh(20, 6500)
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(within(dataRow()).getByText('正常')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 上升')).toBeInTheDocument()
  await refresh(30, 7000)
  expect(within(dataRow()).getByText('高 Lag')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  // Topics 页面仍持续采集，跨 Lag 区间变化后持续时长门槛仍应满足。
  await refresh(40, 2000)
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(within(dataRow()).getByText('积压')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 下降')).toBeInTheDocument()
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(within(dataRow()).getByText('积压')).toBeInTheDocument()
  expect(within(dataRow()).getByLabelText('Lag 下降')).toBeInTheDocument()
})

// 验证多路由共享失败资源的展示保留值，同时汇总、历史和计时始终使用原始样本。
it('retains failed resource display values across overview, topics and groups while using raw samples for totals and timing', async () => {
  const complete: ClusterSnapshot = { clusterId: 'cluster-a', collectedAt: new Date().toISOString(), partialFailures: [], consumerGroupIds: ['worker'],
    topics: [{ topic: 'orders', low: 0, latestMessageOffset: 99, retainedOffsetSpan: 200, incomplete: false, partitions: [
      { partition: 0, low: 0, high: 100, latestMessageOffset: 99, retainedOffsetSpan: 100 },
      { partition: 1, low: 0, high: 100, latestMessageOffset: 99, retainedOffsetSpan: 100 },
    ] }], consumerGroups: [{ groupId: 'worker', topic: 'orders', lag: 100, incomplete: false, unknownPartitionCount: 0, partitions: [
      { partition: 0, high: 100, committedOffset: 50, lag: 50 }, { partition: 1, high: 100, committedOffset: 50, lag: 50 },
    ] }],
  }
  let sample = complete
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue({ defaults: { normalMax: 10, highMin: 80, overLimitSeconds: 30 }, overrides: [] })
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'display', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  vi.spyOn(sessionsApi, 'snapshot').mockImplementation(async () => sample)
  render(<App />)
  await screen.findByText('暂无异常 Consumer Groups。')
  // 展示保留用例切换原始样本后手动采集，等待新时间被接纳。
  const collect = async (next: ClusterSnapshot) => {
    sample = next
    fireEvent.click(screen.getByRole('button', { name: '手动刷新' }))
    await waitFor(() => expect(screen.getByLabelText('最后更新时间')).toHaveAttribute('datetime', next.collectedAt))
    await waitFor(() => expect(screen.getByRole('button', { name: '手动刷新' })).toBeEnabled())
  }
  // 构造连续采样用例时基于首个快照生成指定秒数后的采集时间。
  const at = (seconds: number) => new Date(Date.parse(complete.collectedAt) + seconds * 1000).toISOString()
  await collect({ ...complete, collectedAt: at(10), partialFailures: [{ scope: 'committed', resource: 'worker/orders:0', code: 'KAFKA_TIMEOUT', message: 'timeout' }],
    consumerGroups: [{ ...complete.consumerGroups[0], lag: 20, incomplete: true, unknownPartitionCount: 1, partitions: [
      { partition: 0, high: 100, committedOffset: null, lag: null }, { partition: 1, high: 100, committedOffset: 80, lag: 20 },
    ] }] })
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('20 已知 Lag')
  const overviewRow = screen.getByRole('row', { name: /worker \/ orders/ })
  expect(overviewRow).toHaveTextContent('70')
  expect(overviewRow).toHaveTextContent('采集异常')
  expect(overviewRow).toHaveTextContent('数据已陈旧')
  expect(within(overviewRow).getByRole('time')).toHaveAttribute('datetime', complete.collectedAt)
  const chartRows = within(screen.getByRole('table', { name: '最近 1 小时总 Lag 数据' })).getAllByRole('row')
  expect(chartRows.at(-1)).toHaveTextContent('未知 / 不完整')
  fireEvent.click(screen.getByRole('link', { name: 'worker / orders' }))
  expect(screen.getByRole('table', { name: 'worker / orders Partitions' })).toHaveTextContent('50')
  expect(screen.getByRole('row', { name: /worker orders/ })).toHaveTextContent('70')
  expect(screen.getByRole('button', { name: '收起 worker / orders' })).toHaveAttribute('aria-expanded', 'true')

  await collect({ ...complete, collectedAt: at(20), partialFailures: [{ scope: 'watermark', resource: 'orders:0', code: 'KAFKA_TIMEOUT', message: 'timeout' }],
    topics: [{ ...complete.topics[0], low: 10, latestMessageOffset: 119, retainedOffsetSpan: 110, incomplete: true, partitions: [
      { partition: 0, low: null, high: null, latestMessageOffset: null, retainedOffsetSpan: null },
      { partition: 1, low: 10, high: 120, latestMessageOffset: 119, retainedOffsetSpan: 110 },
    ] }], consumerGroups: [{ ...complete.consumerGroups[0], lag: 30, incomplete: true, unknownPartitionCount: 1, partitions: [
      { partition: 0, high: null, committedOffset: 60, lag: null }, { partition: 1, high: 120, committedOffset: 90, lag: 30 },
    ] }] })
  fireEvent.click(screen.getByRole('link', { name: 'Topics' }))
  fireEvent.click(screen.getByRole('button', { name: '展开 orders' }))
  const partitionRows = within(screen.getByRole('table', { name: 'orders Partitions' })).getAllByRole('row')
  expect(partitionRows[1]).toHaveTextContent('100')
  expect(partitionRows[1]).toHaveTextContent('数据已陈旧')
  expect(partitionRows[2]).toHaveTextContent('120')
  expect(partitionRows[2]).not.toHaveTextContent('数据已陈旧')

  await collect({ ...complete, collectedAt: at(30), consumerGroupIds: [], consumerGroups: [],
    partialFailures: [{ scope: 'groups', resource: 'cluster-a', code: 'KAFKA_TIMEOUT', message: 'timeout' }] })
  fireEvent.click(screen.getByRole('link', { name: '概览' }))
  expect(screen.getByRole('link', { name: 'worker / orders' })).toBeInTheDocument()
  expect(screen.getByRole('region', { name: 'Consumer Group 数' })).toHaveTextContent('1')
  expect(screen.getByRole('region', { name: '总 Lag' })).toHaveTextContent('—')
  fireEvent.click(screen.getByRole('link', { name: 'Consumer Groups' }))
  expect(screen.getByRole('row', { name: /worker orders/ })).toHaveTextContent('采集异常')
  await collect({ ...complete, collectedAt: at(60) })
  expect(screen.getByRole('row', { name: /worker orders/ })).toHaveTextContent('正常')
  expect(screen.getByRole('row', { name: /worker orders/ })).not.toHaveTextContent('数据已陈旧')
})

// 验证真实路由中的语言切换保留当前页面、刷新选择、快照和原有会话，防止语言触发重挂载。
it('translates navigation without replacing the active route or monitoring session', async () => {
  const user = userEvent.setup()
  window.history.replaceState({}, '', '/consumer-groups')
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(lagRulesApi, 'get').mockResolvedValue(rulesFixture)
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'language-session', leaseSeconds: 30 })
  vi.spyOn(sessionsApi, 'snapshot').mockResolvedValue(snapshotFixture)
  vi.spyOn(sessionsApi, 'close').mockResolvedValue(undefined)
  render(<App />)
  const table = await screen.findByRole('table', { name: 'Consumer Groups 汇总' })
  await user.selectOptions(screen.getByRole('combobox', { name: '刷新频率' }), '10')
  const createsBefore = vi.mocked(sessionsApi.create).mock.calls.length
  await user.click(screen.getByRole('button', { name: 'English' }))
  expect(screen.getByRole('link', { name: 'Overview' })).toBeVisible()
  expect(screen.getByRole('navigation', { name: 'Main navigation' })).toBeInTheDocument()
  expect(screen.getByRole('link', { name: 'Cluster Settings' })).toBeVisible()
  expect(screen.getByRole('complementary', { name: 'Kafka Lens' })).toHaveTextContent('Monitoring and Topic management')
  expect(screen.getByRole('complementary', { name: 'Kafka Lens' })).toHaveTextContent('Data is collected only while the page is open')
  expect(window.location.pathname).toBe('/consumer-groups')
  expect(table).toBeInTheDocument()
  expect(screen.getByRole('combobox', { name: 'Refresh interval' })).toHaveValue('10')
  expect(screen.getByLabelText('Last updated')).toHaveAttribute('datetime', snapshotFixture.collectedAt)
  expect(sessionsApi.create).toHaveBeenCalledTimes(createsBefore)
  expect(sessionsApi.close).not.toHaveBeenCalled()
})

// 验证真实设置路由在翻译重渲染后保留已打开弹窗及输入，防止根节点语言 key 丢弃草稿。
it('preserves an open settings dialog and its draft when language changes', async () => {
  const user = userEvent.setup()
  window.history.replaceState({}, '', '/settings')
  vi.spyOn(clustersApi, 'list').mockResolvedValue([clusterFixture])
  vi.spyOn(feishuAlertsApi, 'get').mockResolvedValue({ accessTokenConfigured: false, maskedAccessToken: null, rules: [] })
  vi.spyOn(sessionsApi, 'create').mockResolvedValue({ sessionId: 'unexpected', leaseSeconds: 30 })
  render(<App />)
  await user.click(await screen.findByRole('button', { name: '新增集群' }))
  const dialog = screen.getByRole('dialog', { name: 'Kafka 集群配置' })
  const name = within(dialog).getByLabelText('集群名称')
  await user.type(name, '保留的集群草稿')
  await user.click(screen.getByRole('button', { name: 'English' }))
  expect(window.location.pathname).toBe('/settings')
  expect(dialog).toBeVisible()
  expect(dialog).toHaveAttribute('open')
  expect(name).toHaveValue('保留的集群草稿')
  expect(screen.getByRole('link', { name: 'Cluster Settings' })).toHaveAttribute('aria-current', 'page')
  expect(sessionsApi.create).not.toHaveBeenCalled()
})
