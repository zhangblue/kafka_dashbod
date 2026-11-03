// 资源展示使用保留值时渲染陈旧提示，并标出最后成功采集时间。
export function StaleNotice({ at }: { at: string | undefined }) {
  return <span> ⚠ 数据已陈旧{at && <>（保留值采集于 <time dateTime={at}>{new Date(at).toLocaleString()}</time>）</>}</span>
}
