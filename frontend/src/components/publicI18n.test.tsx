import { act, render, screen } from '@testing-library/react'
import { expect, it } from 'vitest'
import i18n from '../i18n'
import { ApiError } from '../api/client'
import { ApiErrorNotice } from '../features/clusters/ClusterForm'
import { MetricHelp } from './MetricHelp'
import { Modal } from './Modal'
import { StaleNotice } from './StaleNotice'
import { StatusBadge } from './StatusBadge'

// 验证公共组件响应语言切换，同时保留 ISO 时间和业务状态样式。
it('updates shared component labels and stale timestamps on language changes', async () => {
  const at = '2026-01-02T03:04:05.000Z'
  render(<Modal title="Example" onClose={() => {}}>
    <MetricHelp label="Lag">Details</MetricHelp><StatusBadge status="high" /><StaleNotice at={at} />
  </Modal>)
  expect(screen.getByRole('button', { name: '关闭弹窗' })).toBeInTheDocument()
  await act(async () => { await i18n.changeLanguage('en') })
  expect(screen.getByRole('button', { name: 'Close dialog' })).toBeInTheDocument()
  expect(screen.getByRole('button', { name: 'Lag field help' })).toBeInTheDocument()
  expect(screen.getByText('High Lag')).toHaveClass('status-high')
  expect(screen.getByText(/Stale data/)).toBeInTheDocument()
  expect(document.querySelector('time')).toHaveAttribute('dateTime', at)
})

// 验证未知后端消息和字段键值不进入 DOM，切换语言仍保留安全提示和 Request ID。
it('keeps unknown API and field messages out of the DOM in both languages', async () => {
  const { container } = render(<ApiErrorNotice fieldIdPrefix="form-error" error={new ApiError(500, {
    code: 'UNKNOWN', message: 'private backend message', fields: { name: 'private backend field detail',
      'private backend field key': 'Name is required' }, requestId: 'req-visible',
  })} />)
  expect(screen.getByText('字段值无效。')).toHaveAttribute('id', 'form-error-name')
  expect(screen.getByText(/req-visible/)).toBeInTheDocument()
  expect(container.innerHTML).not.toContain('private backend')
  await act(async () => { await i18n.changeLanguage('en') })
  expect(screen.getByText('Request failed. Please try again later.')).toBeInTheDocument()
  expect(screen.getByText('This field is invalid.')).toBeInTheDocument()
  expect(container.innerHTML).not.toContain('private backend')
})
