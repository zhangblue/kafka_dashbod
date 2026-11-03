import { expect, it, vi } from 'vitest'
import { formatDateTime, formatNumber, formatTime } from './format'

// 验证显式语言决定数字格式，防止读取机器默认 locale。
it('formats numbers with the explicit application language', () => {
  const formatter = vi.spyOn(Intl, 'NumberFormat')
  expect(formatNumber(1234567, 'en')).toBe('1,234,567')
  expect(formatNumber(1234567, 'zh-CN')).toBe('1,234,567')
  expect(formatter.mock.calls.map(([locale]) => locale)).toEqual(['en', 'zh-CN'])
  formatter.mockRestore()
})

// 验证日期和时间透传语言、时区且支持 Date，防止 UTC 输入受机器时区影响。
it('formats time and date-time with explicit locales and the requested time zone', () => {
  const at = Date.UTC(2026, 0, 2, 3, 4, 5)
  const formatter = vi.spyOn(Intl, 'DateTimeFormat')
  expect(formatTime(at, 'en', 'UTC')).toMatch(/3:04:05/)
  expect(formatTime(new Date(at), 'zh-CN', 'UTC')).toContain('03:04:05')
  expect(formatDateTime(at, 'zh-CN', 'UTC')).toContain('2026')
  expect(formatDateTime(new Date(at), 'en', 'UTC')).toMatch(/2026/)
  expect(formatter.mock.calls.map(([locale, options]) => [locale, options?.timeZone])).toEqual([
    ['en', 'UTC'], ['zh-CN', 'UTC'], ['zh-CN', 'UTC'], ['en', 'UTC'],
  ])
  formatter.mockRestore()
})
