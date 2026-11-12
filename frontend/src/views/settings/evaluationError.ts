/** HTTP interceptors reject structured objects, not necessarily Error instances. */
export function evaluationErrorMessage(cause: unknown): string {
  if (typeof cause === 'string' && cause.trim()) return cause
  if (cause && typeof cause === 'object') {
    const record = cause as { message?: unknown; error?: unknown }
    if (typeof record.message === 'string' && record.message.trim()) return record.message
    if (record.error && typeof record.error === 'object') {
      const message = (record.error as { message?: unknown }).message
      if (typeof message === 'string' && message.trim()) return message
    }
    if (typeof record.error === 'string' && record.error.trim()) return record.error
  }
  return '评估请求失败，请稍后重试。 / Evaluation request failed; please retry.'
}
