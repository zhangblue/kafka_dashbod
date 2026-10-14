export function StaleNotice({ at }: { at: string | undefined }) {
  return <span> ⚠ 数据已陈旧{at && <>（保留值采集于 <time dateTime={at}>{new Date(at).toLocaleString()}</time>）</>}</span>
}
