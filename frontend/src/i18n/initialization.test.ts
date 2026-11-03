import { afterEach, expect, it, vi } from 'vitest'
import { LANGUAGE_STORAGE_KEY } from './language'

// 每个初始化用例结束后还原浏览器属性，避免污染其他语言及页面测试。
afterEach(() => { vi.restoreAllMocks(); vi.unstubAllGlobals(); localStorage.removeItem(LANGUAGE_STORAGE_KEY) })

// 重载真实入口验证空或缺失 languages 会回退单值 language，而非只测试解析 helper。
it.each([{ languages: [] }, { languages: undefined }])('initializes from navigator.language when languages is $languages', async ({ languages }) => {
  vi.spyOn(navigator, 'languages', 'get').mockReturnValue(languages as unknown as readonly string[])
  vi.spyOn(navigator, 'language', 'get').mockReturnValue('en-US')
  vi.resetModules()
  const { default: initialized } = await import('./index')
  expect(initialized.t('navigation.overview')).toBe('Overview')
  expect(document.documentElement.lang).toBe('en')
})

// 浏览器属性读取失败时仍须完成真实初始化，优先尝试可读的单值语言，全部失败则中文兜底。
it.each([false, true])('initializes safely when browser preference access throws (both: %s)', async both => {
  vi.spyOn(navigator, 'languages', 'get').mockImplementation(() => { throw new Error('Denied') })
  vi.spyOn(navigator, 'language', 'get').mockImplementation(() => { if (both) throw new Error('Denied'); return 'en-US' })
  vi.resetModules()
  const { default: initialized } = await import('./index')
  expect(initialized.t('navigation.overview')).toBe(both ? '概览' : 'Overview')
  expect(document.documentElement.lang).toBe(both ? 'zh-CN' : 'en')
})

// 无法访问 navigator 时保留明确保存的偏好，避免初始化读取浏览器失败覆盖用户选择。
it('initializes the stored language even when navigator is unavailable', async () => {
  localStorage.setItem(LANGUAGE_STORAGE_KEY, 'en')
  vi.stubGlobal('navigator', undefined)
  vi.resetModules()
  const { default: initialized } = await import('./index')
  expect(initialized.t('navigation.overview')).toBe('Overview')
  expect(document.documentElement.lang).toBe('en')
})
