import type { zhCN } from './zh-CN'

type TranslationShape<T> = { [K in keyof T]: T[K] extends string ? string : TranslationShape<T[K]> }

export const en = {
  common: { language: { chinese: 'Chinese', english: 'English' }, errors: { requestFailed: 'Request failed. Please try again later.' } },
  navigation: { overview: 'Overview', topics: 'Topics', consumerGroups: 'Consumer Groups', settings: 'Cluster Settings' },
} satisfies TranslationShape<typeof zhCN>
