export function buildDirectorySearchQuery(query: string, limit = 20, offset = 0): string {
  const params = new URLSearchParams()
  const trimmed = query.trim()
  if (trimmed) params.set('q', trimmed)
  params.set('limit', String(Math.max(1, Math.min(100, Math.floor(limit) || 20))))
  if (Number.isFinite(offset) && offset > 0) params.set('offset', String(Math.floor(offset)))
  return params.toString()
}
