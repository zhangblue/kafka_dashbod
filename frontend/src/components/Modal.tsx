import { useTranslation } from 'react-i18next'
import { useEffect, useId, useRef, type ReactNode } from 'react'

// 配置或确认流程需要阻断背景交互时渲染原生弹窗，忙碌时限制关闭入口。
export function Modal({ title, description, children, onClose, busy = false, wide = false }: {
  title: string; description?: string; children: ReactNode; onClose: () => void; busy?: boolean; wide?: boolean
}) {
  const { t } = useTranslation()
  const ref = useRef<HTMLDialogElement>(null)
  const id = useId()
  // 挂载时记住焦点和滚动状态；卸载时恢复，确保退出弹窗后键盘操作回到原位置。
  useEffect(() => {
    const dialog = ref.current!
    const previous = document.activeElement as HTMLElement | null
    const overflow = document.body.style.overflow
    // 挂载完成后进入原生模态层，再锁定页面滚动，保持焦点与背景交互一致。
    dialog.showModal()
    document.body.style.overflow = 'hidden'
    return () => { dialog.close(); document.body.style.overflow = overflow; previous?.focus() }
  }, [])
  return <dialog ref={ref} className={`config-modal${wide ? ' wide' : ''}`} aria-labelledby={id}
    onCancel={event => { event.preventDefault(); if (!busy) onClose() }}>
    <div className="modal-heading"><div><h2 id={id}>{title}</h2>{description && <p>{description}</p>}</div>
      <button type="button" className="icon-button" aria-label={t('shared.closeDialog')} disabled={busy} onClick={onClose}>×</button>
    </div>
    {children}
  </dialog>
}
