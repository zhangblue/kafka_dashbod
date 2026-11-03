import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import { applyDocumentLanguage, readBrowserLanguages, readStoredLanguage, resolveInitialLanguage } from './language'
import { en } from './resources/en'
import { zhCN } from './resources/zh-CN'

// 先安全读取明确保存的选择，再解析浏览器首选项，避免刷新覆盖用户语言。
const initialLanguage = resolveInitialLanguage({
  stored: readStoredLanguage(),
  browserLanguages: readBrowserLanguages(),
})

// 在初始化前注册变更监听，确保初始化及后续切换都会同步辅助技术读取的文档语言。
i18n.on('languageChanged', language => {
  applyDocumentLanguage(resolveInitialLanguage({ stored: language, browserLanguages: [] }))
})

// 内置资源同步初始化后再由入口挂载 React，保证首屏立即可用；React 已负责文本转义。
void i18n.use(initReactI18next).init({
  resources: { 'zh-CN': { translation: zhCN }, en: { translation: en } },
  lng: initialLanguage,
  fallbackLng: 'zh-CN',
  supportedLngs: ['zh-CN', 'en'],
  defaultNS: 'translation',
  initAsync: false,
  interpolation: { escapeValue: false },
})

export default i18n
