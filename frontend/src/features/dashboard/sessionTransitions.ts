// 页面级队列跨路由重挂载保留：下一次创建必须等待旧 DELETE 及服务端原生资源关闭完成。
let pending: Promise<void> | undefined

// 创建或释放会话时串行执行操作，前序失败也会结算队列，避免阻断后续清理和重连。
export function sessionTransition<T>(operation: () => Promise<T>): Promise<T> {
  const result = pending ? pending.then(operation) : operation()
  const settled = result.then(() => {}, () => {})
  pending = settled
  // 仅由仍占据队尾的操作清空队列，避免较早操作结算时丢失后续等待关系。
  void settled.then(() => { if (pending === settled) pending = undefined })
  return result
}
