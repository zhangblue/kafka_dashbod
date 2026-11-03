import { useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router'
import { DashboardLayout } from '../features/dashboard/DashboardLayout'
import { ClustersPage } from '../features/clusters/ClustersPage'
import { TopicsPage } from '../features/topics/TopicsPage'
import { ConsumerGroupsPage } from '../features/consumer-groups/ConsumerGroupsPage'
import { OverviewPage } from '../features/overview/OverviewPage'

// 应用根节点渲染时建立稳定查询上下文、导航与页面路由，监控页面共享仪表盘布局。
function App() {
  // 首次挂载时创建查询客户端，后续重渲染复用缓存，失败重连由页面显式处理。
  const [client] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: false } } }))
  return (
    <QueryClientProvider client={client}><BrowserRouter>
    <div className="app-shell">
      <aside className="sidebar" aria-label="Kafka Lens">
        <div className="brand"><span className="brandmark" aria-hidden="true">K</span><div><b>Kafka Lens</b><small>Cluster observability</small></div></div>
        <nav aria-label="主要导航">
          <ul>
            <li><NavLink to="/" end><i aria-hidden="true">⌁</i><span>概览</span></NavLink></li>
            <li><NavLink to="/topics"><i aria-hidden="true">◫</i><span>Topics</span></NavLink></li>
            <li><NavLink to="/consumer-groups"><i aria-hidden="true">◉</i><span>Consumer Groups</span></NavLink></li>
            <li><NavLink to="/settings"><i aria-hidden="true">⚙</i><span>集群设置</span></NavLink></li>
          </ul>
        </nav>
        <div className="sidebar-footer">监控与 Topic 管理<br />数据仅在页面打开时采集</div>
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
