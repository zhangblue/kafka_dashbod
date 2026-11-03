import { useTranslation } from 'react-i18next'
import i18n from '../i18n'
import { persistLanguage, type AppLanguage } from '../i18n/language'

// 顶部栏挂载时提供原生双按钮入口，仅更新翻译状态，不重建页面与监控会话。
export function LanguageSwitch() {
  const { t } = useTranslation()
  // 用户选择语言时先完成内存切换，再尽力保存；存储失败不能阻止或回滚本页翻译。
  async function selectLanguage(language: AppLanguage) {
    await i18n.changeLanguage(language)
    persistLanguage(language)
  }
  return <div className="language-switch" role="group" aria-label={t('common.language.label')}>
    <button type="button" className="language-option" lang="zh-CN" aria-label="中文" aria-pressed={i18n.resolvedLanguage === 'zh-CN'} onClick={() => void selectLanguage('zh-CN')}>
      <span className="language-label-full" aria-hidden="true">中文</span><span className="language-label-short" aria-hidden="true">中</span>
    </button>
    <button type="button" className="language-option" lang="en" aria-label="English" aria-pressed={i18n.resolvedLanguage === 'en'} onClick={() => void selectLanguage('en')}>EN</button>
  </div>
}
