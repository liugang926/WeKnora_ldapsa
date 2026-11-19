import { get } from '@/utils/request'

export const CHAT_USAGE_PAGE_SIZE = 20

export interface ChatUsage {
  question_count: number
  web_question_count: number
  dingtalk_question_count: number
  prompt_tokens: number
  completion_tokens: number
  total_tokens: number
  reported_turns: number
  unreported_turns: number
}

export interface ChatUsageCounts extends ChatUsage {
  session_count: number
}

export interface ChatUserUsage extends ChatUsageCounts {
  user_id: string
  display_name?: string
  username: string
}

export interface ChatSessionUsage extends ChatUsage {
  source: 'web' | 'dingtalk'
  session_id: string
  started_at: string
  workspace_id: number
  workspace_name: string
}

interface Page<T> {
  items: T[]
  total: number
  page: number
  page_size: number
}

type UserPage = Page<ChatUserUsage> & {
  summary: ChatUsageCounts & { user_count: number }
  unattributed_dingtalk: ChatUsageCounts
}
const usersURL = '/api/v1/system/admin/chat-usage/users'

async function readPage<T>(url: string, page: number, signal: AbortSignal, q?: string, pageSize = CHAT_USAGE_PAGE_SIZE) {
  const result = await get<{ success: boolean; data: T }>(url, {
    params: { page, page_size: pageSize, q: q || undefined }, signal,
  })
  if (!result.success || !result.data) throw new Error('Could not load chat usage')
  return result.data
}

export function getChatUsers(page: number, query: string, signal: AbortSignal, pageSize = CHAT_USAGE_PAGE_SIZE) {
  return readPage<UserPage>(usersURL, page, signal, query, pageSize)
}

export function getChatSessions(userID: string, page: number, signal: AbortSignal) {
  return readPage<Page<ChatSessionUsage>>(`${usersURL}/${encodeURIComponent(userID)}/sessions`, page, signal)
}
