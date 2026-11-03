import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router'
import { DashboardLayout } from '../features/dashboard/DashboardLayout'
import { ClustersPage } from '../features/clusters/ClustersPage'
import { TopicsPage } from '../features/topics/TopicsPage'
import { ConsumerGroupsPage } from '../features/consumer-groups/ConsumerGroupsPage'
import { OverviewPage } from '../features/overview/OverviewPage'

// 应用根节点渲染时建立稳定查询上下文、导航与页面路由，监控页面共享仪表盘布局。
function App() {
  const { t } = useTranslation()
  // 首次挂载时创建查询客户端，后续重渲染复用缓存，失败重连由页面显式处理。
  const [client] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: false } } }))
  // 翻译只触发原树重渲染，不给路由或查询上下文添加语言 key，保留会话、历史和弹窗草稿。
  return (
    <QueryClientProvider client={client}><BrowserRouter>
    <div className="app-shell">
      <aside className="sidebar" aria-label="Kafka Lens">
        <div className="brand"><span className="brandmark" aria-hidden="true">K</span><div><b>Kafka Lens</b><small>{t('app.subtitle')}</small></div></div>
        <nav aria-label={t('navigation.label')}>
          <ul>
            <li><NavLink to="/" end><i aria-hidden="true">⌁</i><span>{t('navigation.overview')}</span></NavLink></li>
            <li><NavLink to="/topics"><i aria-hidden="true">◫</i><span>{t('navigation.topics')}</span></NavLink></li>
            <li><NavLink to="/consumer-groups"><i aria-hidden="true">◉</i><span>{t('navigation.consumerGroups')}</span></NavLink></li>
            <li><NavLink to="/settings"><i aria-hidden="true">⚙</i><span>{t('navigation.settings')}</span></NavLink></li>
          </ul>
        </nav>
        <div className="sidebar-footer">{t('app.monitoring')}<br />{t('app.collectionNotice')}</div>
      </aside>
      <main className="app-main">
        <Routes>
          <Route element={<DashboardLayout />}>
            <Route index element={<OverviewPage />} />
            <Route path="topics" element={<TopicsPage />} />
            <Route path="consumer-groups" element={<ConsumerGroupsPage />} />
          </Route>
          <Route path="settings" element={<ClustersPage />} />
        </Routes>
      </main>
    </div>
    </BrowserRouter></QueryClientProvider>
  )
}

export default App
