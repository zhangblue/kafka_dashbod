export const zhCN = {
  common: { language: { label: '界面语言', chinese: '中文', english: 'English' }, errors: { requestFailed: '请求失败，请稍后重试。' } },
  navigation: { label: '主要导航', overview: '概览', topics: 'Topics', consumerGroups: 'Consumer Groups', settings: '集群设置' },
  app: { subtitle: 'Cluster observability', monitoring: '监控与 Topic 管理', collectionNotice: '数据仅在页面打开时采集' },
  dashboard: {
    label: '监控面板', cluster: '集群', selectCluster: '请选择集群',
    connection: { idle: '未连接', connecting: '正在连接…', connected: '已连接', paused: '已暂停（页面不可见）', error: '连接已断开，显示最后一次快照' },
    staleLabel: '监控数据已陈旧', stale: '数据已陈旧', updatedAt: '更新于', lastUpdated: '最后更新时间', notCollected: '尚未采集',
    refresh: '刷新', refreshInterval: '刷新频率', seconds: '{{count}} 秒', manualRefresh: '手动刷新', refreshNow: '立即刷新', reconnect: '重新连接',
    partialFailure: '部分数据采集失败；未知值和不完整数据已保留。', loadingClusters: '正在加载集群…', noClusters: '尚未配置集群。', configureCluster: '配置集群',
  },
  settings: { headerDescription: '连接与 Lag 状态规则' },
} as const
