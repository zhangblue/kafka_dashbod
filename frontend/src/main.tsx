import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
// 入口挂载组件前完成内置翻译初始化，避免首屏出现尚未解析的翻译键。
import './i18n'
import './index.css'
import App from './app/App.tsx'

// 浏览器入口加载后挂载应用；严格模式会回放开发期生命周期，便于发现清理遗漏。
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
