export type AppLanguage = 'zh-CN' | 'en'

export const LANGUAGE_STORAGE_KEY = 'kafka-lens.language'

// 应用初始化时优先恢复明确选择，否则仅按首个有效浏览器偏好确定内置语言。
export function resolveInitialLanguage(input: { stored: string | null; browserLanguages: readonly string[] }): AppLanguage {
  if (input.stored === 'zh-CN' || input.stored === 'en') return input.stored

  for (const preference of input.browserLanguages) {
    try {
      const [language] = Intl.getCanonicalLocales(preference.trim())
      if (language) return language.toLowerCase().startsWith('zh') ? 'zh-CN' : 'en'
    } catch {
      // 浏览器偏好格式无效时继续检查下一项，不让无效值决定界面语言。
    }
  }
  return 'zh-CN'
}

// 初始化读取保存偏好时隔离存储异常，仅接受应用支持的两种语言。
export function readStoredLanguage(storage?: Storage): AppLanguage | null {
  try {
    const stored = (storage ?? window.localStorage).getItem(LANGUAGE_STORAGE_KEY)
    return stored === 'zh-CN' || stored === 'en' ? stored : null
  } catch {
    return null
  }
}

// 用户切换语言后尽力保存偏好，存储不可用时仍允许本次页面切换继续。
export function persistLanguage(language: AppLanguage, storage?: Storage): void {
  try {
    // 获取存储对象也可能被浏览器拒绝，因此必须与写入放在同一安全边界内。
    const target = storage ?? window.localStorage
    target.setItem(LANGUAGE_STORAGE_KEY, language)
  } catch {
    // 不读取或记录存储内容，持久化失败只影响下次访问的语言恢复。
  }
}

// 初始化及语言切换完成时更新文档语言，让浏览器和辅助技术使用当前语言。
export function applyDocumentLanguage(language: AppLanguage): void {
  document.documentElement.lang = language
}
