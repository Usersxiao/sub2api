// The api client rejects with plain `{ message }` objects rather than Error instances.
export function errMsg(e: unknown): string {
  if (e && typeof e === 'object' && 'message' in e) {
    const m = (e as { message?: unknown }).message
    if (typeof m === 'string' && m) return m
  }
  return String(e)
}
