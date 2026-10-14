import { useState } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { BrowserRouter, NavLink, Route, Routes } from 'react-router'
import { DashboardLayout } from '../features/dashboard/DashboardLayout'
import { ClustersPage } from '../features/clusters/ClustersPage'
import { TopicsPage } from '../features/topics/TopicsPage'
import { ConsumerGroupsPage } from '../features/consumer-groups/ConsumerGroupsPage'

function App() {
  const [client] = useState(() => new QueryClient({ defaultOptions: { queries: { retry: false } } }))
  return (
    <QueryClientProvider client={client}><BrowserRouter>
    <main>
      <h1>Kafka Lens</h1>
      <nav aria-label="主要导航">
        <ul>
          <li><NavLink to="/" end>概览</NavLink></li>
          <li><NavLink to="/topics">Topics</NavLink></li>
          <li><NavLink to="/consumer-groups">Consumer Groups</NavLink></li>
          <li><NavLink to="/settings">集群设置</NavLink></li>
        </ul>
      </nav>
      <Routes>
        <Route element={<DashboardLayout />}>
          <Route index element={<section aria-label="概览内容" />} />
          <Route path="topics" element={<TopicsPage />} />
          <Route path="consumer-groups" element={<ConsumerGroupsPage />} />
        </Route>
        <Route path="settings" element={<ClustersPage />} />
      </Routes>
    </main>
    </BrowserRouter></QueryClientProvider>
  )
}

export default App
