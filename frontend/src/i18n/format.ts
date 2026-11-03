import type { AppLanguage } from './language'

// 展示已知数值时按调用方传入的界面语言格式化，避免机器默认 locale 改变显示。
export function formatNumber(value: number, language: AppLanguage): string {
  return new Intl.NumberFormat(language).format(value)
}

// 顶部栏或图表展示采样时间时使用明确语言，测试或跨时区视图可额外指定时区。
export function formatTime(value: number | Date, language: AppLanguage, timeZone?: string): string {
  return new Intl.DateTimeFormat(language, { hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone }).format(value)
}

// 陈旧提示或完整时间展示时使用明确语言，保留秒精度并支持调用方指定时区。
export function formatDateTime(value: number | Date, language: AppLanguage, timeZone?: string): string {
  return new Intl.DateTimeFormat(language, { year: 'numeric', month: 'numeric', day: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit', timeZone }).format(value)
}
