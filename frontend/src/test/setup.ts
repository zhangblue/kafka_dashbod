import '@testing-library/jest-dom/vitest'
import { cleanup } from '@testing-library/react'
import { afterEach, beforeEach } from 'vitest'
import i18n from '../i18n'
import { LANGUAGE_STORAGE_KEY } from '../i18n/language'

// 每个用例开始前仅清除语言偏好，再恢复中文，避免上个用例污染语言或其他功能存储。
beforeEach(async () => {
  localStorage.removeItem(LANGUAGE_STORAGE_KEY)
  // 内置资源已同步可用，仍等待切换完成后执行用例，保证翻译和文档语言一致。
  await i18n.changeLanguage('zh-CN')
})

// jsdom has no native dialog top layer; keep visibility semantics for interaction tests.
// 在交互测试调用 showModal 时模拟顶层弹窗的可见标记，让 jsdom 保持可观察的打开语义。
HTMLDialogElement.prototype.showModal = function () { this.setAttribute('open', '') }
// 在交互测试关闭弹窗时移除打开标记，便于验证卸载、焦点与取消流程。
HTMLDialogElement.prototype.close = function () { this.removeAttribute('open') }

// 每个用例结束后卸载页面，触发组件清理并防止遗留 DOM 混入后续测试。
afterEach(cleanup)
