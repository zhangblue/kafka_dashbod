import { useId, useState, type ReactNode } from 'react'

export function MetricHelp({ label, children }: { label: string; children: ReactNode }) {
  const id = useId()
  const [hovered, setHovered] = useState(false)
  const [focused, setFocused] = useState(false)
  const [pinned, setPinned] = useState(false)
  const [dismissed, setDismissed] = useState(false)
  const visible = !dismissed && (hovered || focused || pinned)
  return <span style={{ position: 'relative', display: 'inline-block' }}
    onMouseEnter={() => { setHovered(true); setDismissed(false) }} onMouseLeave={() => setHovered(false)}>
    <button type="button" aria-label={`${label} 字段说明`} aria-describedby={visible ? id : undefined}
      onFocus={() => { setFocused(true); setDismissed(false) }}
      onBlur={() => { setFocused(false); setPinned(false); setDismissed(false) }}
      onClick={() => { setPinned(!pinned); setDismissed(pinned) }}
      onKeyDown={event => { if (event.key === 'Escape') { setDismissed(true); setPinned(false) } }}>?</button>
    {visible && <span id={id} role="tooltip" style={{ position: 'absolute', left: 0, top: '100%', zIndex: 1,
      width: 'min(22rem, 70vw)', padding: '0.75rem', background: '#172033', color: '#fff', textAlign: 'left', fontWeight: 400 }}>
      {children}
    </span>}
  </span>
}
