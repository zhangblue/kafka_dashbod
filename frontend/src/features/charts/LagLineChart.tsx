import { useEffect, useRef } from 'react'
import { init, use as registerCharts, type EChartsType } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

registerCharts([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

export interface LagSample { at: number; lag: number | null }

export function LagLineChart({ samples, stale }: { samples: LagSample[]; stale: boolean }) {
  const container = useRef<HTMLDivElement>(null)
  const instance = useRef<EChartsType | null>(null)
  useEffect(() => {
    const chart = init(container.current!)
    instance.current = chart
    const observer = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(() => chart.resize()) : null
    observer?.observe(container.current!)
    return () => { observer?.disconnect(); chart.dispose(); instance.current = null }
  }, [])
  useEffect(() => {
    const newest = samples.length ? Math.max(...samples.map(sample => sample.at)) : undefined
    instance.current?.setOption({
      animation: false,
      tooltip: { trigger: 'axis' },
      legend: { data: ['总 Lag'] },
      grid: { left: 64, right: 24, top: 40, bottom: 36 },
      xAxis: { type: 'time', min: newest === undefined ? undefined : newest - 3_600_000, max: newest },
      yAxis: { type: 'value', min: 0 },
      series: [{ name: '总 Lag', type: 'line', showSymbol: true, connectNulls: false,
        data: samples.map(sample => [sample.at, sample.lag]) }],
    }, true)
  }, [samples])
  return <figure>
    <figcaption>最近 1 小时总 Lag{stale && <span aria-label="图表数据已陈旧"> ⚠ 数据已陈旧</span>}</figcaption>
    <div ref={container} role="img" aria-label="总 Lag 折线图" style={{ width: '100%', height: 280 }} />
    {!samples.length && <p>尚无历史样本。</p>}
    <details><summary>查看图表数据</summary><table aria-label="最近 1 小时总 Lag 数据">
      <thead><tr><th scope="col">采集时间</th><th scope="col">总 Lag</th></tr></thead>
      <tbody>{samples.map(sample => <tr key={sample.at}><td><time dateTime={new Date(sample.at).toISOString()}>{new Date(sample.at).toLocaleString()}</time></td>
        <td>{sample.lag === null ? '未知 / 不完整' : sample.lag.toLocaleString('en-US')}</td></tr>)}</tbody>
    </table></details>
  </figure>
}
