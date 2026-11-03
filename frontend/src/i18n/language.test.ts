import { afterEach, expect, it, vi } from 'vitest'
import {
  applyDocumentLanguage,
  LANGUAGE_STORAGE_KEY,
  persistLanguage,
  readStoredLanguage,
  resolveInitialLanguage,
} from './language'

// 每个语言用例结束后恢复浏览器访问，防止存储拒绝替身影响后续测试。
afterEach(() => {
  vi.restoreAllMocks()
  localStorage.removeItem(LANGUAGE_STORAGE_KEY)
})

// 验证繁体中文首选项归一到中文资源，防止按地区误选英文。
it('maps the first Chinese browser preference to zh-CN', () => {
  expect(resolveInitialLanguage({ stored: null, browserLanguages: ['zh-HK'] })).toBe('zh-CN')
})

// 验证第一项有效偏好优先，避免后续中文偏好覆盖英文首选。
it('uses the first valid browser preference instead of searching for Chinese', () => {
  expect(resolveInitialLanguage({ stored: null, browserLanguages: ['en-US', 'zh-CN'] })).toBe('en')
  expect(resolveInitialLanguage({ stored: null, browserLanguages: ['fr-FR', 'zh-CN'] })).toBe('en')
})

// 验证已保存的有效选择优先于浏览器语言，刷新时保持用户决定。
it('prefers the saved application language', () => {
  expect(resolveInitialLanguage({ stored: 'en', browserLanguages: ['zh-CN'] })).toBe('en')
  expect(resolveInitialLanguage({ stored: 'zh-CN', browserLanguages: ['en-US'] })).toBe('zh-CN')
})

// 验证损坏或缺失的保存值不会阻止浏览器解析与中文兜底。
it('falls back safely when the saved language is invalid', () => {
  expect(resolveInitialLanguage({ stored: 'broken', browserLanguages: [] })).toBe('zh-CN')
  expect(resolveInitialLanguage({ stored: 'broken', browserLanguages: ['en-US'] })).toBe('en')
  expect(resolveInitialLanguage({ stored: null, browserLanguages: [] })).toBe('zh-CN')
})

// 验证空白与格式错误的首选项被忽略，避免无效值抢占后续中文首选。
it('ignores invalid browser language entries', () => {
  expect(resolveInitialLanguage({ stored: null, browserLanguages: ['', ' ', 'not_a_locale', 'zh-HK'] })).toBe('zh-CN')
})

// 验证语言读写仅使用约定的保存键，避免覆盖其他功能的数据。
it('persists and reads a supported language without changing other settings', () => {
  localStorage.setItem('other-setting', 'retained')
  persistLanguage('en')
  expect(localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe('en')
  expect(readStoredLanguage()).toBe('en')
  expect(localStorage.getItem('other-setting')).toBe('retained')
  localStorage.removeItem('other-setting')
})

// 验证存储中的非支持值返回空值，供初始化继续使用浏览器偏好。
it('does not accept unsupported stored languages', () => {
  localStorage.setItem(LANGUAGE_STORAGE_KEY, 'broken')
  expect(readStoredLanguage()).toBeNull()
})

// 验证存储接口抛错后当前页面仍能切到英文，避免隐私模式阻断切换。
it('keeps language changes available when storage operations throw', () => {
  const storage = {
    // 读取偏好时模拟浏览器拒绝访问，不返回任何存储内容。
    getItem() { throw new Error('Storage unavailable') },
    // 保存偏好时模拟浏览器拒绝访问，验证失败可安全降级。
    setItem() { throw new Error('Storage unavailable') },
  } as unknown as Storage
  expect(readStoredLanguage(storage)).toBeNull()
  expect(() => persistLanguage('en', storage)).not.toThrow()
  applyDocumentLanguage('en')
  expect(document.documentElement.lang).toBe('en')
})

// 验证获取 localStorage 属性本身抛错也被隔离，而非仅捕获读写方法错误。
it('handles browsers that deny access to the localStorage property', () => {
  vi.spyOn(window, 'localStorage', 'get').mockImplementation(() => { throw new Error('Storage unavailable') })
  expect(readStoredLanguage()).toBeNull()
  expect(() => persistLanguage('en')).not.toThrow()
  applyDocumentLanguage('en')
  expect(document.documentElement.lang).toBe('en')
})

// 验证显式同步文档语言时更新根元素，使辅助技术读取当前语言。
it('applies the active language to the document root', () => {
  applyDocumentLanguage('en')
  expect(document.documentElement.lang).toBe('en')
  applyDocumentLanguage('zh-CN')
  expect(document.documentElement.lang).toBe('zh-CN')
})
