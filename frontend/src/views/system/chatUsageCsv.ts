import type { ChatUsage, ChatUsageCounts, ChatUserUsage } from '@/api/system/chatUsage'

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

export const csvHeaderKeys = [
  'displayName', 'account', 'userId', 'questions', 'webQuestions', 'dingtalkQuestions',
  'sessions', 'input', 'output', 'tokens', 'missing',
] as const

export function hasUsage(row?: ChatUsageCounts) {
  return !!row && (row.question_count > 0 || row.session_count > 0 || row.reported_turns > 0 || row.unreported_turns > 0)
}

type ExportPage = {
  items: ChatUserUsage[]; total: number; page: number; page_size: number
  unattributed_dingtalk?: ChatUsageCounts
}

function csvRow(name: string | undefined, account: string, id: string, row: ChatUsageCounts) {
  return [name, account, id, row.question_count, row.web_question_count, row.dingtalk_question_count,
    row.session_count, reportedTokens(row, 'prompt_tokens'), reportedTokens(row, 'completion_tokens'),
    reportedTokens(row, 'total_tokens'), row.unreported_turns].map(csvCell).join(',')
}

export async function peopleCsv(
  headers: string[], fetchPage: (page: number) => Promise<ExportPage>, unattributedLabel?: string,
) {
  const lines = [headers.map(csvCell).join(',')]
  const seen = new Set<string>()
  let unattributed: ChatUsageCounts | undefined
  let total: number | undefined
  for (let page = 1; ; page++) {
    const result = await fetchPage(page)
    if (page === 1) unattributed = result.unattributed_dingtalk
    total ??= result.total
    if (result.page !== page || result.total !== total || result.page_size <= 0 ||
        result.items.length !== Math.min(result.page_size, total - seen.size)) {
      throw new Error('Chat usage list changed during export')
    }
    for (const row of result.items) {
      if (seen.has(row.user_id)) throw new Error('Duplicate user during export')
      seen.add(row.user_id)
      lines.push(csvRow(row.display_name, row.username, row.user_id, row))
    }
    if (seen.size === total) {
      if (unattributedLabel && unattributed && hasUsage(unattributed)) {
        lines.push(csvRow(unattributedLabel, '', '', unattributed))
      }
      return '\uFEFF' + lines.join('\r\n') + '\r\n'
    }
  }
}
