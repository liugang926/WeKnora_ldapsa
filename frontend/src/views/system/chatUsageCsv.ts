import type { ChatUsage, ChatUserUsage } from '@/api/system/chatUsage'

type TokenField = 'prompt_tokens' | 'completion_tokens' | 'total_tokens'

export function reportedTokens(row: ChatUsage, field: TokenField) {
  if (!row.reported_turns) return null
  if (field !== 'total_tokens' && !row.prompt_tokens && !row.completion_tokens && row.total_tokens > 0) return null
  return row[field]
}

function csvCell(value: string | number | null | undefined) {
  let text = String(value ?? '')
  // Keep user-controlled names and accounts literal in spreadsheet applications.
  if (typeof value === 'string' && /^(?:[\t\r\n]|\s*[=+@\-＝＋＠－])/u.test(text)) text = "'" + text
  return '"' + text.replaceAll('"', '""') + '"'
}

type ExportPage = { items: ChatUserUsage[]; total: number; page: number; page_size: number }

export async function peopleCsv(headers: string[], fetchPage: (page: number) => Promise<ExportPage>) {
  const lines = [headers.map(csvCell).join(',')]
  const seen = new Set<string>()
  let total: number | undefined
  for (let page = 1; ; page++) {
    const result = await fetchPage(page)
    total ??= result.total
    if (result.page !== page || result.total !== total || result.page_size <= 0 ||
        result.items.length !== Math.min(result.page_size, total - seen.size)) {
      throw new Error('Chat usage list changed during export')
    }
    for (const row of result.items) {
      if (seen.has(row.user_id)) throw new Error('Duplicate user during export')
      seen.add(row.user_id)
      lines.push([row.display_name, row.username, row.user_id, row.session_count,
        reportedTokens(row, 'prompt_tokens'), reportedTokens(row, 'completion_tokens'),
        reportedTokens(row, 'total_tokens'), row.unreported_turns].map(csvCell).join(','))
    }
    if (seen.size === total) return '\uFEFF' + lines.join('\r\n') + '\r\n'
  }
}
