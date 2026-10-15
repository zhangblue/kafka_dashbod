import { useEffect, useId, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

export function MetricHelp({ label, children, trigger = '?' }: { label: string; children: ReactNode; trigger?: ReactNode }) {
  const id = useId()
  const anchor = useRef<HTMLSpanElement>(null)
  const tooltip = useRef<HTMLSpanElement>(null)
  const [portalHost, setPortalHost] = useState<Element>(document.body)
  const [hovered, setHovered] = useState(false)
  const [focused, setFocused] = useState(false)
  const [pinned, setPinned] = useState(false)
  const [dismissed, setDismissed] = useState(false)
  const visible = !dismissed && (hovered || focused || pinned)
  useEffect(() => {
    if (!visible) return
    const dismiss = (event: KeyboardEvent) => {
      if (event.key === 'Escape') { setDismissed(true); setPinned(false) }
    }
    const position = () => {
      if (!anchor.current || !tooltip.current) return
      const rect = anchor.current.getBoundingClientRect()
      const box = tooltip.current
      box.style.left = `${Math.max(8, Math.min(rect.left, window.innerWidth - box.offsetWidth - 8))}px`
      box.style.top = `${Math.max(8, rect.bottom + box.offsetHeight + 8 <= window.innerHeight ? rect.bottom + 8 : rect.top - box.offsetHeight - 8)}px`
    }
    position()
    document.addEventListener('keydown', dismiss)
    window.addEventListener('resize', position)
    window.addEventListener('scroll', position, true)
    return () => { document.removeEventListener('keydown', dismiss); window.removeEventListener('resize', position); window.removeEventListener('scroll', position, true) }
  }, [visible])
  return <span ref={anchor} style={{ position: 'relative', display: 'inline-block' }}
    onMouseEnter={event => { setPortalHost(event.currentTarget.closest('dialog') ?? document.body); setHovered(true); setDismissed(false) }} onMouseLeave={() => setHovered(false)}>
    <button type="button" className={trigger === '?' ? 'help-trigger' : 'rule-badge'} aria-label={`${label} 字段说明`} aria-describedby={visible ? id : undefined}
      onFocus={event => { setPortalHost(event.currentTarget.closest('dialog') ?? document.body); setFocused(true); setDismissed(false) }}
      onBlur={() => { setFocused(false); setPinned(false); setDismissed(false) }}
      onClick={() => { setPinned(!pinned); setDismissed(pinned) }}>{trigger}</button>
    {visible && createPortal(<span ref={tooltip} id={id} role="tooltip" style={{ position: 'fixed', zIndex: 100,
      width: 'min(22rem, 80vw)', maxHeight: '80vh', overflowY: 'auto', padding: '0.75rem', background: '#172033', color: '#fff', textAlign: 'left', fontWeight: 400 }}>
      {children}
    </span>, portalHost)}
  </span>
}
