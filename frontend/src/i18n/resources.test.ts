import { expect, it } from 'vitest'
import i18n from './index'
import { en } from './resources/en'
import { zhCN } from './resources/zh-CN'

type ResourceTree = { readonly [key: string]: string | ResourceTree }

// 资源完整性检查时递归收集叶子值，以发现深层翻译缺失或空文本。
function flattenLeaves(resource: ResourceTree, prefix = ''): Record<string, string> {
  const leaves: Record<string, string> = {}
  for (const [key, value] of Object.entries(resource)) {
    const path = prefix ? `${prefix}.${key}` : key
    if (typeof value === 'string') leaves[path] = value
    else Object.assign(leaves, flattenLeaves(value, path))
  }
  return leaves
}

// 资源结构对比时排序叶子路径，使比较不依赖对象属性的排列顺序。
function flattenKeys(resource: ResourceTree): string[] {
  return Object.keys(flattenLeaves(resource)).sort()
}

// 验证两种语言的所有叶子路径相同，防止新增中文键后遗漏英文翻译。
it('keeps English and Chinese resource keys identical', () => {
  expect(flattenKeys(en)).toEqual(flattenKeys(zhCN))
})

// 验证每个资源叶子都包含文本，避免界面在切换语言后静默显示空白。
it('contains no empty translations in either language', () => {
  expect(Object.values(flattenLeaves(en))).not.toContain('')
  expect(Object.values(flattenLeaves(zhCN))).not.toContain('')
})

// 验证首批导航和通用错误键已交付，防止同构但空缺的资源通过检查。
it('provides the required first-stage translation keys', () => {
  expect(flattenKeys(zhCN)).toEqual(expect.arrayContaining([
    'common.language.chinese',
    'common.language.english',
    'common.errors.requestFailed',
    'navigation.overview',
    'navigation.topics',
    'navigation.consumerGroups',
    'navigation.settings',
  ]))
})

// 验证入口导入后内置资源已可立即使用，避免首屏读到未初始化的翻译键。
it('initializes built-in translations synchronously', () => {
  expect(i18n.isInitialized).toBe(true)
  expect(i18n.t('navigation.overview', { lng: 'zh-CN' })).toBe('概览')
  expect(i18n.t('navigation.overview', { lng: 'en' })).toBe('Overview')
  expect(document.documentElement.lang).toBe('zh-CN')
})

// 验证每次语言变更都会更新页面语言并读取对应资源，防止仅更新首次初始化。
it('synchronizes document language whenever the active language changes', async () => {
  await i18n.changeLanguage('en')
  expect(i18n.t('common.errors.requestFailed')).toBe('Request failed. Please try again later.')
  expect(document.documentElement.lang).toBe('en')
  await i18n.changeLanguage('zh-CN')
  expect(i18n.t('common.errors.requestFailed')).toBe('请求失败，请稍后重试。')
  expect(document.documentElement.lang).toBe('zh-CN')
})
