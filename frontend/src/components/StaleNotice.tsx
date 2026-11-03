import { useTranslation } from 'react-i18next'
import { formatDateTime } from '../i18n/format'
import { resolveInitialLanguage } from '../i18n/language'

// 资源使用保留值时按当前语言提示陈旧状态，并保留机器可读的原始 ISO 采集时间。
export function StaleNotice({ at }: { at: string | undefined }) {
  const { t, i18n } = useTranslation()
  const language = resolveInitialLanguage({ stored: i18n.resolvedLanguage ?? null, browserLanguages: [] })
  return <span> ⚠ {t('shared.stale')}{at && <>{t('shared.retainedAt')} <time dateTime={at}>{formatDateTime(new Date(at), language)}</time>{t('shared.retainedEnd')}</>}</span>
}
