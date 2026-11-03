import { useEffect, useId, useRef, useState, type ReactNode } from 'react'
import { createPortal } from 'react-dom'

// 指标字段渲染时提供可悬停、聚焦或固定的说明，并将提示挂到当前弹窗或页面。
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
  // 提示可见期间监听定位和关闭事件，隐藏或卸载时成对移除，避免遗留全局监听器。
  useEffect(() => {
    if (!visible) return
    // 提示打开期间响应 Escape，关闭并解除固定状态。
    const dismiss = (event: KeyboardEvent) => {
      if (event.key === 'Escape') { setDismissed(true); setPinned(false) }
    }
    // 提示初次打开及窗口滚动、缩放时重新定位，尽量保持在视口边界内。
    const position = () => {
      if (!anchor.current || !tooltip.current) return
      const rect = anchor.current.getBoundingClientRect()
      const box = tooltip.current
      box.style.left = `${Math.max(8, Math.min(rect.left, window.innerWidth - box.offsetWidth - 8))}px`
      box.style.top = `${Math.max(8, rect.bottom + box.offsetHeight + 8 <= window.innerHeight ? rect.bottom + 8 : rect.top - box.offsetHeight - 8)}px`
    }
    // 先计算首次位置，再监听后续视口变化，避免提示保持初始坐标。
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
