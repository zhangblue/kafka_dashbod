import { useEffect, useMemo, useRef } from 'react'
import { init, use as registerCharts, type EChartsType } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

registerCharts([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

export interface LagSample { at: number; lag: number | null }
export interface LagSeries { name: string; samples: LagSample[] }

export function LagLineChart({ samples, series, stale, label }: { samples?: LagSample[]; series?: LagSeries[]; stale: boolean; label?: string }) {
  const lines = useMemo(() => series ?? [{ name: '总 Lag', samples: samples ?? [] }], [series, samples])
  const times = useMemo(() => [...new Set(lines.flatMap(line => line.samples.map(sample => sample.at)))].sort((a, b) => a - b), [lines])
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
    const newest = times.at(-1)
    instance.current?.setOption({
      animation: false,
      tooltip: { trigger: 'axis' },
      legend: { type: 'scroll', data: lines.map(line => line.name) },
      grid: { left: 64, right: 24, top: 40, bottom: 36 },
      xAxis: { type: 'time', min: newest === undefined ? undefined : newest - 3_600_000, max: newest },
      yAxis: { type: 'value', min: 0 },
      series: lines.map(line => ({ name: line.name, type: 'line', showSymbol: true, connectNulls: false,
        data: line.samples.map(sample => [sample.at, sample.lag]) })),
    }, true)
  }, [lines, times])
  return <figure>
    <figcaption>{label ? `最近 1 小时 ${label}` : '最近 1 小时总 Lag'}{stale && <span aria-label="图表数据已陈旧"> ⚠ 数据已陈旧</span>}</figcaption>
    <div ref={container} role="img" aria-label={label ? `${label}折线图` : '总 Lag 折线图'} style={{ width: '100%', height: 280 }} />
    {!times.length && <p>尚无历史样本。</p>}
    <details><summary>查看图表数据</summary><table aria-label={label ? `${label}数据` : '最近 1 小时总 Lag 数据'}>
      <thead><tr><th scope="col">采集时间</th>{lines.map(line => <th key={line.name} scope="col">{line.name}</th>)}</tr></thead>
      <tbody>{times.map(at => <tr key={at}><td><time dateTime={new Date(at).toISOString()}>{new Date(at).toLocaleString()}</time></td>
        {lines.map(line => { const lag = line.samples.find(sample => sample.at === at)?.lag
          return <td key={line.name}>{lag == null ? '未知 / 不完整' : lag.toLocaleString('en-US')}</td>
        })}</tr>)}</tbody>
    </table></details>
  </figure>
}
