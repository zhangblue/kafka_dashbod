import { act, render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import { ApiError } from '../../api/client'
import i18n from '../../i18n'
import { TopicAdminErrorNotice } from './TopicConfigModal'

// 验证缺少 Topic 专属文案的错误使用当前语言安全回退，不能显示内部占位或后端原文。
it.each([
  ['INTERNAL_ERROR', '请求无法完成，请稍后重试。', 'The request could not be completed. Please try again later.'],
  ['PAYLOAD_TOO_LARGE', '请求超出 256 KiB 大小限制。', 'The request exceeds the 256 KiB limit.'],
  ['UNKNOWN_TOPIC_FAILURE', '请求失败，请稍后重试。', 'Request failed. Please try again later.'],
  ['constructor', '请求失败，请稍后重试。', 'Request failed. Please try again later.'],
])('uses a safe localized fallback for %s', async (code, chinese, english) => {
  const { container } = render(<TopicAdminErrorNotice error={new ApiError(500, {
    code, message: 'private backend message', requestId: 'req-topic-fallback',
  })} />)
  expect(screen.getByRole('alert')).toHaveTextContent(chinese)
  expect(screen.getByText('Request ID: req-topic-fallback')).toBeInTheDocument()
  expect(container.innerHTML).not.toMatch(/API_REQUEST_FAILED|private backend message/)
  await act(async () => { await i18n.changeLanguage('en') })
  expect(screen.getByRole('alert')).toHaveTextContent(english)
  expect(screen.getByText('Request ID: req-topic-fallback')).toBeInTheDocument()
  expect(container.innerHTML).not.toMatch(/API_REQUEST_FAILED|private backend message/)
})

// 验证兜底迁移保留已有 Topic 专属提示，避免本修复提前改写完整 Topic 国际化行为。
it('retains the existing Topic-specific message and request ID', async () => {
  render(<TopicAdminErrorNotice error={new ApiError(403, {
    code: 'TOPIC_AUTHORIZATION_FAILED', message: 'private backend detail', requestId: 'req-topic-known',
  })} />)
  expect(screen.getByRole('alert')).toHaveTextContent('Kafka 账户无权执行此 Topic 操作。')
  await act(async () => { await i18n.changeLanguage('en') })
  expect(screen.getByRole('alert')).toHaveTextContent('Kafka 账户无权执行此 Topic 操作。')
  expect(screen.getByText('Request ID: req-topic-known')).toBeInTheDocument()
})

// 验证未知非 API 异常同样使用当前语言的通用错误，不能输出其原始错误文本。
it('localizes non-API failures without exposing their message', async () => {
  await act(async () => { await i18n.changeLanguage('en') })
  const { container } = render(<TopicAdminErrorNotice error={new Error('private runtime failure')} />)
  expect(screen.getByRole('alert')).toHaveTextContent('Request failed. Please try again later.')
  expect(container.innerHTML).not.toContain('private runtime failure')
})
