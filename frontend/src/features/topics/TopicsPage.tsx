import { useTranslation } from 'react-i18next'
import type { ParseKeys } from 'i18next'
import { formatNumber } from '../../i18n/format'
import type { AppLanguage } from '../../i18n/language'
import { Fragment, useEffect, useId, useRef, useState } from 'react'
import type { PartitionWatermark, TopicMutationResult, TopicSnapshot } from '../../api/types'
import { MetricHelp } from '../../components/MetricHelp'
import { useDashboard } from '../dashboard/useDashboard'
import { StaleNotice } from '../../components/StaleNotice'
import { topicDisplayKey, topicPartitionDisplayKey } from '../dashboard/displaySnapshot'
import { useSnapshotStale } from '../dashboard/useSnapshotStale'
import { CreateTopicModal } from './CreateTopicModal'
import { TopicConfigModal } from './TopicConfigModal'
import { DeleteTopicModal } from './DeleteTopicModal'

type ManagementTarget = { sessionId: string } & ({ kind: 'create' } | { kind: 'config' | 'delete'; topic: string })
type ManagementNotice = { clusterId: string | undefined; text: ParseKeys; deletedTopic?: string; waiting?: boolean }

// 在 Topic 与分区单元格渲染时将未知值保留为破折号。
const format = (value: number | null, language: AppLanguage) => value === null ? '—' : formatNumber(value, language)
// 在展示分区最大消息 Offset 时对非空分区使用 High 减一，空或未知范围返回未知。
const latest = (partition: PartitionWatermark) => partition.low === null || partition.high === null
  || partition.high <= partition.low ? null : partition.high - 1

// 在 Topic 汇总行渲染时计算已知 Offset 边界与范围估算，不把缺失分区计为零。
function summary(topic: TopicSnapshot) {
  const incomplete = topic.incomplete || topic.partitions.some(partition => partition.low === null || partition.high === null)
  const lows = topic.partitions.flatMap(partition => partition.low === null ? [] : [partition.low])
  const maxima = topic.partitions.flatMap(partition => { const value = latest(partition); return value === null ? [] : [value] })
  return { incomplete,
    low: incomplete || !lows.length ? null : Math.min(...lows),
    max: incomplete || !maxima.length ? null : Math.max(...maxima),
    span: incomplete ? null : topic.partitions.reduce((sum, partition) => sum + partition.high! - partition.low!, 0),
  }
}

// 在用户展开 Topic 时展示分区权威水位、范围估算及陈旧标记。
function PartitionTable({ topic, staleResources }: { topic: TopicSnapshot; staleResources: Record<string, string> | undefined }) {
  const { t, i18n } = useTranslation()
  const language = i18n.resolvedLanguage === 'en' ? 'en' : 'zh-CN'
  return <table aria-label={t('topics.partitionTable', { topic: topic.topic })}>
    <thead><tr>
      <th scope="col">{t('topics.partition')}</th>
      <th scope="col">{t('topics.low')} <MetricHelp label={t('topics.low')}>{t('topics.lowHelp')}</MetricHelp></th>
      <th scope="col">{t('topics.high')} <MetricHelp label={t('topics.high')}>{t('topics.highHelp')}</MetricHelp></th>
      <th scope="col">{t('topics.maximum')} <MetricHelp label={t('topics.partitionMaximum', { topic: topic.topic })}>{t('topics.partitionMaxHelp')}</MetricHelp></th>
      <th scope="col">{t('topics.estimate')} <MetricHelp label={t('topics.partitionEstimate', { topic: topic.topic })}>{t('topics.partitionEstimateHelp')}</MetricHelp></th>
    </tr></thead>
    <tbody>{topic.partitions.map(partition => <tr key={partition.partition}>
      <td>{format(partition.partition, language)}</td><td>{format(partition.low, language)}</td><td>{format(partition.high, language)}</td><td>{format(latest(partition), language)}{staleResources?.[topicPartitionDisplayKey(topic.topic, partition.partition)] && <StaleNotice at={staleResources[topicPartitionDisplayKey(topic.topic, partition.partition)]} />}</td><td>{format(partition.high === null || partition.low === null ? null : partition.high - partition.low, language)}</td>
    </tr>)}</tbody>
  </table>
}

// 在 Topics 路由渲染时汇总 Offset、展开分区并把管理弹窗绑定到当前有效会话。
export function TopicsPage() {
  const { t, i18n } = useTranslation()
  const language = i18n.resolvedLanguage === 'en' ? 'en' : 'zh-CN'
  const { cluster, monitor, refreshSeconds } = useDashboard()
  const stale = useSnapshotStale(monitor, refreshSeconds)
  const [search, setSearch] = useState('')
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  const [target, setTarget] = useState<ManagementTarget | null>(null)
  const [notice, setNotice] = useState<ManagementNotice | null>(null)
  const sessionId = monitor.sessionId
  const snapshot = monitor.displaySnapshot ?? monitor.snapshot
  const createButton = useRef<HTMLButtonElement>(null)
  const previousTarget = useRef<ManagementTarget | null>(null)
  const focusedDeleteTrigger = useRef<HTMLButtonElement | null>(null)
  if (target && target.sessionId !== sessionId) setTarget(null)
  if (notice && notice.clusterId !== cluster?.id) setNotice(null)
  useEffect(/* 在快照更新导致已删除行消失时按需恢复工具栏焦点，避免焦点落在已卸载控件。 */ () => {
    const removedTrigger = focusedDeleteTrigger.current && !focusedDeleteTrigger.current.isConnected
    if (((previousTarget.current && !target) || removedTrigger) && document.activeElement === document.body) createButton.current?.focus()
    if (removedTrigger) focusedDeleteTrigger.current = null
    previousTarget.current = target
  }, [target, snapshot])
  useEffect(/* 在删除传播提示出现后启动有界定时器，超时只提示手动确认而不否定成功删除。 */ () => {
    if (!notice?.waiting) return
    const timer = setTimeout(() => setNotice(previous => previous === notice
      ? { ...notice, waiting: false, text: 'topics.deleteDelayed' } : previous), 30000)
    return () => clearTimeout(timer)
  }, [notice])
  const id = useId()
  const topics = snapshot?.topics.filter(topic => topic.topic.toLocaleLowerCase().includes(search.trim().toLocaleLowerCase())) ?? []
  // 在点击管理按钮时记录当前会话和目标，防止弹窗跨会话执行管理请求。
  const openManagement = (kind: ManagementTarget['kind'], topic?: string) => {
    if (!sessionId) return
    setTarget(kind === 'create' ? { kind, sessionId } : { kind, sessionId, topic: topic! })
  }
  // 在管理请求成功后先保留提交结果，再等待快照刷新并处理已删除行的焦点恢复。
  const applied = async (result?: TopicMutationResult) => {
    if (!target) return
    setNotice({ clusterId: cluster?.id, ...(result?.status === 'appliedButUnconfirmed' ? { text: 'topicAdmin.unconfirmed' }
      : target.kind === 'delete' ? { text: 'topics.deleteWaiting', deletedTopic: target.topic, waiting: true }
      : { text: target.kind === 'create' ? 'topics.created' : 'topicAdmin.saved' }) })
    await monitor.refresh()
  }
  const noticeText = notice?.deletedTopic && snapshot && !snapshot.topics.some(topic => topic.topic === notice.deletedTopic)
    ? 'topics.deleted' : notice?.text
  return <section className="page-panel" aria-label={t('topics.content')}>
    <div className="page-head"><div><h2>{t('topics.title')}</h2><p>{t('topics.subtitle')}</p></div>
    <div className="toolbar"><label className="search-label"><span className="sr-only">{t('topics.search')}</span><input type="search" placeholder={t('topics.searchPlaceholder')} value={search} onChange={/* 在搜索内容变化时更新当前过滤词，仅筛选已取得的快照。 */ event => setSearch(event.target.value)} /></label><span className="status-badge status-normal">{t('topics.count', { count: topics.length })}</span><button ref={createButton} type="button" className="primary" disabled={!sessionId} onClick={/* 在新建按钮点击时把创建目标绑定到当前有效会话。 */ () => openManagement('create')}>{t('topicAdmin.createTitle')}</button></div></div>
    {noticeText && <p className="topic-management-notice" role="status" aria-label={t('topics.result')}>{t(noticeText)}</p>}
    {stale && <p role="status" aria-label={t('topics.staleLabel')}>{t('topics.stale')}</p>}
    <p className="offset-note"><b>{t('topics.offsetIndependent')}</b> {t('topics.offsetNote')}</p>
    {!!snapshot?.partialFailures.length && <p role="status">{t('topics.partial')}</p>}
    {!snapshot ? <p>{t('topics.notCollected')}</p> : <>
      <div className="panel table-scroll"><table aria-label={t('topics.summary')}><thead><tr>
        <th scope="col">{t('topics.topic')}</th>
        <th scope="col">{t('topics.partitions')} <MetricHelp label={t('topics.partitions')}>{t('topics.partitionsHelp')}</MetricHelp></th>
        <th scope="col">{t('topics.minimum')} <MetricHelp label={t('topics.minimum')}>{t('topics.minimumHelp')}</MetricHelp></th>
        <th scope="col">{t('topics.maximum')} <MetricHelp label={t('topics.maximum')}>{t('topics.maximumHelp')}</MetricHelp></th>
        <th scope="col">{t('topics.estimate')} <MetricHelp label={t('topics.estimate')}>{t('topics.estimateHelp')}</MetricHelp></th>
        <th scope="col">{t('topics.status')}</th><th scope="col" className="topic-actions-column">{t('topics.actions')}</th>
      </tr></thead><tbody>{topics.map((topic, index) => {
        const staleAt = monitor.staleResources?.[topicDisplayKey(topic.topic)]
        const values = summary(staleAt ? { ...topic, incomplete: false } : topic)
        const open = expanded.has(topic.topic)
        const detailsId = `${id}-partitions-${index}`
        return <Fragment key={topic.topic}>
          <tr><td className="topic-name-cell"><button className="expand-button" type="button" aria-label={t(open ? 'topics.collapse' : 'topics.expand', { topic: topic.topic })} aria-expanded={open} aria-controls={open ? detailsId : undefined}
            onClick={/* 在展开按钮点击时切换当前资源明细，不改变后台采样会话。 */ () => setExpanded(previous => { const next = new Set(previous); if (open) next.delete(topic.topic); else next.add(topic.topic); return next })}>
            {open ? '⌄' : '›'}</button> <span className="name">{topic.topic}</span></td>
          <td>{format(topic.partitions.length, language)}</td><td>{format(values.low, language)}</td><td>{format(values.max, language)}</td><td>{format(values.span, language)}</td>
          <td>{staleAt ? <StaleNotice at={staleAt} /> : values.incomplete ? t('topics.incomplete') : t('topics.complete')}</td>
          <td className="topic-actions-column"><div className="topic-row-actions">
            <button type="button" aria-label={t('topics.configureLabel', { topic: topic.topic })} disabled={!sessionId} onClick={/* 在配置按钮点击时记录当前 Topic 与会话，按需打开实时参数编辑器。 */ () => openManagement('config', topic.topic)}>{t('topics.configure')}</button>
            {!topic.topic.startsWith('__') && <button type="button" className="danger-button" aria-label={t('topics.deleteLabel', { topic: topic.topic })} disabled={!sessionId}
              onFocus={/* 在删除按钮聚焦时记录触发器，只在删除行消失后按需恢复焦点。 */ event => { if (notice?.deletedTopic === topic.topic) focusedDeleteTrigger.current = event.currentTarget }}
              onBlur={/* 在删除按钮失焦时撤销恢复标记，避免抢走用户主动移到别处的焦点。 */ event => { if (event.currentTarget.isConnected) focusedDeleteTrigger.current = null }}
              onClick={/* 在删除按钮点击时绑定当前 Topic 与会话，由确认弹窗再校验完整名称。 */ () => openManagement('delete', topic.topic)}>{t('topics.delete')}</button>}
          </div></td></tr>
          {open && <tr className="detail-row"><td colSpan={7} id={detailsId}><PartitionTable topic={topic} staleResources={monitor.staleResources} /></td></tr>}
        </Fragment>
      })}</tbody></table></div>
      {!topics.length && <p>{snapshot.topics.length ? t('topics.noMatch') : t('topics.empty')}</p>}
    </>}
    {target && target.sessionId === sessionId && (target.kind === 'create'
      ? <CreateTopicModal sessionId={target.sessionId} onClose={/* 在关闭创建弹窗时清除管理目标并卸载创建表单，共享仪表盘会话继续存在并采样。 */ () => setTarget(null)} onApplied={applied} />
      : target.kind === 'config' ? <TopicConfigModal sessionId={target.sessionId} topic={target.topic} onClose={/* 在关闭配置弹窗时清除管理目标并卸载配置表单，取消其配置读取；共享仪表盘会话继续存在并采样。 */ () => setTarget(null)} onApplied={applied} />
        : <DeleteTopicModal sessionId={target.sessionId} topic={target.topic} onClose={/* 在关闭删除弹窗时清除管理目标并卸载删除表单，共享仪表盘会话继续存在并采样。 */ () => setTarget(null)} onApplied={applied} />)}
  </section>
}
