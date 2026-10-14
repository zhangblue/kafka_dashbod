// Session creation/release belongs to the browser page, including route remounts.
// The next creation must wait for an old DELETE (and native resource close) to finish.
let pending: Promise<void> | undefined

export function sessionTransition<T>(operation: () => Promise<T>): Promise<T> {
  const result = pending ? pending.then(operation) : operation()
  const settled = result.then(() => {}, () => {})
  pending = settled
  void settled.then(() => { if (pending === settled) pending = undefined })
  return result
}
