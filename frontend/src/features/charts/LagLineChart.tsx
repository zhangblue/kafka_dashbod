import { useEffect, useMemo, useRef } from 'react'
import { useTranslation } from 'react-i18next'
import { formatDateTime, formatNumber, formatTime } from '../../i18n/format'
import { resolveInitialLanguage } from '../../i18n/language'
import { init, use as registerCharts, type EChartsType } from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent, TooltipComponent, LegendComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'

registerCharts([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer])

export interface LagSample { at: number; lag: number | null }
export interface LagSeries { name: string; samples: LagSample[] }

// 在概览或消费组历史区域渲染时，把内存样本转换为保留未知断点的一小时折线图并管理图表生命周期。
export function LagLineChart({ samples, series, stale, label }: { samples?: LagSample[]; series?: LagSeries[]; stale: boolean; label?: string }) {
  const { t, i18n } = useTranslation()
  const language = resolveInitialLanguage({ stored: i18n.resolvedLanguage ?? null, browserLanguages: [] })
  const totalSeriesName = t('overview.chart.totalSeries')
  const lines = useMemo(() => series ?? [{ name: totalSeriesName, samples: samples ?? [] }], [series, samples, totalSeriesName])
  const times = useMemo(() => [...new Set(lines.flatMap(line => line.samples.map(sample => sample.at)))].sort((a, b) => a - b), [lines])
  const container = useRef<HTMLDivElement>(null)
  const instance = useRef<EChartsType | null>(null)
  useEffect(/* 在图表首次挂载时创建实例并观察尺寸，卸载时先断开观察再销毁实例。 */ () => {
    const chart = init(container.current!)
    instance.current = chart
    const observer = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(() => chart.resize()) : null
    observer?.observe(container.current!)
    return () => { observer?.disconnect(); chart.dispose(); instance.current = null }
  }, [])
  useEffect(/* 在线条、时间轴或语言变化时更新配置与格式化函数；复用已有实例，未知样本保留断线并固定一小时范围。 */ () => {
    const newest = times.at(-1)
    instance.current?.setOption({
      animation: false,
      textStyle: { color: '#8997ad' },
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#151e2f',
        borderColor: '#243147',
        textStyle: { color: '#d8e2f1' },
        axisPointer: { label: { formatter: ({ value }: { value: number | string }) => formatDateTime(Number(value), language) } },
        valueFormatter: (value: number | null) => value == null ? t('overview.chart.unknown') : formatNumber(value, language),
      },
      legend: { type: 'scroll', top: 4, right: 8, data: lines.map(line => line.name), textStyle: { color: '#8997ad' } },
      grid: { left: 48, right: 18, top: 38, bottom: 30 },
      xAxis: {
        type: 'time',
        min: newest === undefined ? undefined : newest - 3_600_000,
        max: newest,
        axisLine: { lineStyle: { color: '#34425a' } },
        axisLabel: { color: '#8997ad', formatter: (value: number) => formatTime(value, language) },
        splitLine: { lineStyle: { color: '#1d283a' } },
      },
      yAxis: {
        type: 'value',
        min: 0,
        axisLine: { lineStyle: { color: '#34425a' } },
        axisLabel: { color: '#8997ad', formatter: (value: number) => formatNumber(value, language) },
        splitLine: { lineStyle: { color: '#1d283a' } },
      },
      series: lines.map((line, index) => ({ name: line.name, type: 'line', showSymbol: line.samples.length === 1, connectNulls: false,
        lineStyle: { width: 2 },
        itemStyle: { color: ['#35c8e6', '#4f7cff', '#2dd4a0', '#f6b94a', '#fb7185'][index % 5] },
        areaStyle: lines.length === 1 ? { opacity: 0.12 } : undefined,
        data: line.samples.map(sample => [sample.at, sample.lag]) })),
    }, true)
  }, [lines, times, language, t])
  return <figure>
    <figcaption>{label ? t('overview.chart.title', { label }) : t('overview.chart.totalTitle')}{stale && <span aria-label={t('overview.chart.staleLabel')}> ⚠ {t('shared.stale')}</span>}</figcaption>
    <div ref={container} role="img" aria-label={label ? t('overview.chart.label', { label }) : t('overview.chart.totalLabel')} style={{ width: '100%', height: 230 }} />
    {!times.length && <p>{t('overview.chart.noSamples')}</p>}
    <details><summary>{t('overview.chart.viewData')}</summary><table aria-label={label ? t('overview.chart.data', { label }) : t('overview.chart.totalData')}>
      <thead><tr><th scope="col">{t('overview.chart.collectedAt')}</th>{lines.map(line => <th key={line.name} scope="col">{line.name}</th>)}</tr></thead>
      <tbody>{times.map(at => <tr key={at}><td><time dateTime={new Date(at).toISOString()}>{formatDateTime(at, language)}</time></td>
        {lines.map(line => { const lag = line.samples.find(sample => sample.at === at)?.lag
          return <td key={line.name}>{lag == null ? t('overview.chart.unknown') : formatNumber(lag, language)}</td>
        })}</tr>)}</tbody>
    </table></details>
  </figure>
}
