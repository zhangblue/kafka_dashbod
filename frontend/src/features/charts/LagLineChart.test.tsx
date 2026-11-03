import { act, render, screen } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { LagLineChart } from './LagLineChart'

// ECharts owns a browser canvas; this boundary fake records the lifecycle contract.
const chart = vi.hoisted(/* 在测试模块准备阶段建立图表边界替身，记录生命周期调用而不依赖真实画布。 */ () => ({ setOption: vi.fn(), resize: vi.fn(), dispose: vi.fn() }))
const init = vi.hoisted(/* 在测试模块准备阶段建立图表边界替身，记录生命周期调用而不依赖真实画布。 */ () => vi.fn(() => chart))
vi.mock('echarts/core', /* 在测试模块准备阶段建立图表边界替身，记录生命周期调用而不依赖真实画布。 */ () => ({ init, use: vi.fn() }))
afterEach(/* 在每个用例结束时恢复本用例修改的测试环境，避免拦截或模拟状态影响后续断言。 */ () => { vi.unstubAllGlobals(); vi.clearAllMocks() })

it('initializes once, updates samples without joining unknown gaps, observes size and releases resources', /* 在本用例执行时验证图表仅初始化一次，样本更新保留未知断点，并在卸载时释放尺寸观察和实例。 */ () => {
  // 在图表测试中模拟尺寸通知，验证实例响应布局变化。
  let resize: ResizeObserverCallback = () => {}
  const disconnect = vi.fn()
  const observe = vi.fn()
  vi.stubGlobal('ResizeObserver', class {
    /* 在尺寸观察替身构造时接收回调，供图表测试手动触发布局通知。 */ constructor(callback: ResizeObserverCallback) { resize = callback }
    observe = observe
    disconnect = disconnect
  })
  const { rerender, unmount } = render(<LagLineChart samples={[{ at: 1000, lag: 12 }]} stale={false} />)
  expect(init).toHaveBeenCalledTimes(1)
  expect(observe).toHaveBeenCalledWith(screen.getByLabelText('总 Lag 折线图'))
  expect(chart.setOption).toHaveBeenLastCalledWith(expect.objectContaining({
    textStyle: { color: '#8997ad' },
    tooltip: expect.objectContaining({ backgroundColor: '#151e2f', borderColor: '#243147' }),
    legend: expect.objectContaining({ textStyle: { color: '#8997ad' } }),
    xAxis: expect.objectContaining({ type: 'time', min: -3599000, max: 1000, axisLabel: { color: '#8997ad' } }),
    yAxis: expect.objectContaining({ type: 'value', min: 0, axisLabel: { color: '#8997ad' } }),
    series: [expect.objectContaining({ data: [[1000, 12]], connectNulls: false })],
  }), true)
  rerender(<LagLineChart samples={[{ at: 1000, lag: 12 }, { at: 2000, lag: null }]} stale />)
  expect(init).toHaveBeenCalledTimes(1)
  expect(chart.setOption).toHaveBeenLastCalledWith(expect.objectContaining({ series: [expect.objectContaining({ data: [[1000, 12], [2000, null]] })] }), true)
  expect(screen.getByRole('table', { name: '最近 1 小时总 Lag 数据' })).toHaveTextContent('未知 / 不完整')
  expect(screen.getByLabelText('图表数据已陈旧')).toBeInTheDocument()
  act(() => resize([], {} as ResizeObserver))
  expect(chart.resize).toHaveBeenCalledTimes(1)
  unmount()
  expect(disconnect).toHaveBeenCalledTimes(1)
  expect(chart.dispose).toHaveBeenCalledTimes(1)
})
