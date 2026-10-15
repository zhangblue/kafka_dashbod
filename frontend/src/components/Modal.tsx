import { useEffect, useId, useRef, type ReactNode } from 'react'

export function Modal({ title, description, children, onClose, busy = false, wide = false }: {
  title: string; description?: string; children: ReactNode; onClose: () => void; busy?: boolean; wide?: boolean
}) {
  const ref = useRef<HTMLDialogElement>(null)
  const id = useId()
  useEffect(() => {
    const dialog = ref.current!
    const previous = document.activeElement as HTMLElement | null
    const overflow = document.body.style.overflow
    dialog.showModal()
    document.body.style.overflow = 'hidden'
    return () => { dialog.close(); document.body.style.overflow = overflow; previous?.focus() }
  }, [])
  return <dialog ref={ref} className={`config-modal${wide ? ' wide' : ''}`} aria-labelledby={id}
    onCancel={event => { event.preventDefault(); if (!busy) onClose() }}>
    <div className="modal-heading"><div><h2 id={id}>{title}</h2>{description && <p>{description}</p>}</div>
      <button type="button" className="icon-button" aria-label="关闭弹窗" disabled={busy} onClick={onClose}>×</button>
    </div>
    {children}
  </dialog>
}
