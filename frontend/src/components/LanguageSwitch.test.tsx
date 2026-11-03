import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, expect, it, vi } from 'vitest'
import i18n from '../i18n'
import { LANGUAGE_STORAGE_KEY } from '../i18n/language'
import { LanguageSwitch } from './LanguageSwitch'
import { useTranslation } from 'react-i18next'

// 在存储异常用例中展示实际翻译，防止只有按钮状态变化而页面文案未更新。
function TranslatedContent() {
  const { t } = useTranslation()
  return <p>{t('common.errors.requestFailed')}</p>
}

// 每个语言控件用例后恢复存储替身，避免异常注入影响后续场景。
afterEach(() => vi.restoreAllMocks())

// 验证切换会更新真实翻译状态、文档语言与持久偏好，防止只改变按钮外观。
it('switches from Chinese to English and persists the completed language change', async () => {
  const user = userEvent.setup()
  render(<LanguageSwitch />)
  expect(screen.getByRole('button', { name: '中文' })).toHaveAttribute('aria-pressed', 'true')
  expect(screen.getByRole('button', { name: 'English' })).toHaveAttribute('aria-pressed', 'false')
  await user.click(screen.getByRole('button', { name: 'English' }))
  expect(i18n.resolvedLanguage).toBe('en')
  expect(localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe('en')
  expect(document.documentElement.lang).toBe('en')
  expect(screen.getByRole('button', { name: 'English' })).toHaveAttribute('aria-pressed', 'true')
})

// 验证两个原生按钮都进入键盘顺序，空格和回车均可激活对应语言。
it('supports keyboard focus and activation for both language buttons', async () => {
  const user = userEvent.setup()
  render(<LanguageSwitch />)
  await user.tab()
  expect(screen.getByRole('button', { name: '中文' })).toHaveFocus()
  await user.tab()
  expect(screen.getByRole('button', { name: 'English' })).toHaveFocus()
  await user.keyboard('{Enter}')
  expect(i18n.resolvedLanguage).toBe('en')
  await user.tab({ shift: true })
  expect(screen.getByRole('button', { name: '中文' })).toHaveFocus()
  await user.keyboard(' ')
  expect(i18n.resolvedLanguage).toBe('zh-CN')
  expect(localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe('zh-CN')
})

// 验证持久化只能发生在内存语言切换之后，且拒绝写入不能回滚当前页面语言。
it('keeps the page language when saving the preference throws', async () => {
  const user = userEvent.setup()
  let languageAtWrite: string | undefined
  let documentLanguageAtWrite: string | undefined
  vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
    languageAtWrite = i18n.resolvedLanguage
    documentLanguageAtWrite = document.documentElement.lang
    throw new DOMException('Storage unavailable', 'SecurityError')
  })
  render(<><LanguageSwitch /><TranslatedContent /></>)
  await user.click(screen.getByRole('button', { name: 'English' }))
  expect(screen.getByText('Request failed. Please try again later.')).toBeVisible()
  expect(i18n.resolvedLanguage).toBe('en')
  expect(languageAtWrite).toBe('en')
  expect(documentLanguageAtWrite).toBe('en')
  expect(screen.getByRole('button', { name: 'English' })).toHaveAttribute('aria-pressed', 'true')
})

// 验证浏览器拒绝整个存储属性时仍可来回切换真实译文和文档语言。
it('renders both languages when the browser denies localStorage access', async () => {
  const user = userEvent.setup()
  vi.spyOn(window, 'localStorage', 'get').mockImplementation(() => { throw new DOMException('Storage unavailable', 'SecurityError') })
  render(<><LanguageSwitch /><TranslatedContent /></>)
  await user.click(screen.getByRole('button', { name: 'English' }))
  expect(screen.getByText('Request failed. Please try again later.')).toBeVisible()
  expect(document.documentElement.lang).toBe('en')
  await user.click(screen.getByRole('button', { name: '中文' }))
  expect(screen.getByText('请求失败，请稍后重试。')).toBeVisible()
  expect(document.documentElement.lang).toBe('zh-CN')
})

// 验证异步语言切换尚未完成时不提前保存偏好，防止刷新恢复尚未生效的语言。
it('waits for the language change to complete before persisting the preference', async () => {
  const user = userEvent.setup()
  // 仅延迟真实语言切换的边界，保留实际语言事件与文档同步副作用。
  const changeLanguage = i18n.changeLanguage.bind(i18n)
  let complete!: () => void
  const pending = new Promise<void>(resolve => { complete = resolve })
  vi.spyOn(i18n, 'changeLanguage').mockImplementationOnce(async language => {
    await pending
    return changeLanguage(language)
  })
  render(<LanguageSwitch />)
  try {
    await user.click(screen.getByRole('button', { name: 'English' }))
    expect(i18n.resolvedLanguage).toBe('zh-CN')
    expect(localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBeNull()
  } finally {
    // 顺序断言即使失败也完成待处理切换，避免遗留 Promise 污染下个用例。
    await act(async () => complete())
  }
  expect(i18n.resolvedLanguage).toBe('en')
  expect(localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe('en')
})
