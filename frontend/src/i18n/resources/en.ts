import type { zhCN } from './zh-CN'

type TranslationShape<T> = { [K in keyof T]: T[K] extends string ? string : TranslationShape<T[K]> }

export const en = {
  common: { language: { label: 'Interface language', chinese: 'Chinese', english: 'English' }, errors: { requestFailed: 'Request failed. Please try again later.' } },
  navigation: { label: 'Main navigation', overview: 'Overview', topics: 'Topics', consumerGroups: 'Consumer Groups', settings: 'Cluster Settings' },
  app: { subtitle: 'Cluster observability', monitoring: 'Monitoring and Topic management', collectionNotice: 'Data is collected only while the page is open' },
  dashboard: {
    label: 'Monitoring dashboard', cluster: 'Cluster', selectCluster: 'Select a cluster',
    connection: { idle: 'Disconnected', connecting: 'Connecting…', connected: 'Connected', paused: 'Paused (page hidden)', error: 'Disconnected; showing the last snapshot' },
    staleLabel: 'Monitoring data is stale', stale: 'Stale data', updatedAt: 'Updated at', lastUpdated: 'Last updated', notCollected: 'No samples yet',
    refresh: 'Refresh', refreshInterval: 'Refresh interval', seconds: '{{count}} s', manualRefresh: 'Manual refresh', refreshNow: 'Refresh now', reconnect: 'Reconnect',
    partialFailure: 'Some data could not be collected; unknown values and incomplete data have been preserved.', loadingClusters: 'Loading clusters…', noClusters: 'No clusters configured. ', configureCluster: 'Configure a cluster',
  },
  settings: { headerDescription: 'Connections and Lag status rules' },
} satisfies TranslationShape<typeof zhCN>
