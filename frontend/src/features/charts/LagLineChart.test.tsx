import { act, render, screen } from '@testing-library/react'
import { afterEach, expect, it, vi } from 'vitest'
import { LagLineChart } from './LagLineChart'

// ECharts owns a browser canvas; this boundary fake records the lifecycle contract.
const chart = vi.hoisted(() => ({ setOption: vi.fn(), resize: vi.fn(), dispose: vi.fn() }))
const init = vi.hoisted(() => vi.fn(() => chart))
vi.mock('echarts/core', () => ({ init, use: vi.fn() }))
afterEach(() => { vi.unstubAllGlobals(); vi.clearAllMocks() })

it('initializes once, updates samples without joining unknown gaps, observes size and releases resources', () => {
  let resize: ResizeObserverCallback = () => {}
  const disconnect = vi.fn()
  const observe = vi.fn()
  vi.stubGlobal('ResizeObserver', class {
    constructor(callback: ResizeObserverCallback) { resize = callback }
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
