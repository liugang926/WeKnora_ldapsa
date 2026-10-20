import { get, put, post } from '@/utils/request'

// ChatHistoryConfig represents the chat history KB configuration for a tenant.
// knowledge_base_id is auto-managed by the backend; frontend only sets other fields.
export interface ChatHistoryConfig {
  enabled: boolean
  embedding_model_id: string
  knowledge_base_id?: string // read-only, auto-managed
}

// ChatHistoryKBStats represents statistics about the chat history knowledge base
export interface ChatHistoryKBStats {
  enabled: boolean
  embedding_model_id?: string
  knowledge_base_id?: string
  knowledge_base_name?: string
  indexed_message_count: number
  has_indexed_messages: boolean
}

// MessageSearchRequest defines search parameters for message search
export interface MessageSearchRequest {
  query: string
  mode?: 'keyword' | 'vector' | 'hybrid'
  limit?: number
  session_ids?: string[]
  // Resume the same query, mode and session filter using the server cursor.
  cursor?: string
}

// MessageSearchGroupItem represents a merged Q&A pair in search results
export interface MessageSearchGroupItem {
  request_id: string
  // Stable identity of a standalone message without a request_id.
  message_id?: string
  session_id: string
  session_title: string
  query_content: string
  answer_content: string
  score: number
  match_type: string
  created_at: string
}

// Request groups and standalone messages have separate identity namespaces.
// A missing stable identity cannot certify that distinct results were retained.
export function messageSearchGroupKey(item: MessageSearchGroupItem): string {
  const identity = item.request_id || item.message_id
  if (!item.session_id || !identity) throw new Error('Message search result has no stable identity')
  return JSON.stringify([item.session_id, item.request_id ? 'request' : 'message', identity])
}

// One authorized search page; total counts this page's returned groups.
export interface MessageSearchResult {
  items: MessageSearchGroupItem[]
  total: number
  // More keyword candidates remain; an empty page may still need continuation.
  has_more?: boolean
  next_cursor?: string
}

export interface MessageSearchResponse {
  success: boolean
  data?: MessageSearchResult
}

// Get tenant chat history config via KV API
export function getTenantChatHistoryConfig() {
  return get('/api/v1/tenants/kv/chat-history-config')
}

// Update tenant chat history config via KV API
export function updateTenantChatHistoryConfig(config: ChatHistoryConfig) {
  return put('/api/v1/tenants/kv/chat-history-config', config)
}

// Get chat history KB statistics
export function getChatHistoryKBStats() {
  return get('/api/v1/messages/chat-history-stats')
}

// Search messages across all sessions (keyword + vector hybrid search)
export function searchMessages(data: MessageSearchRequest) {
  return post<MessageSearchResponse>('/api/v1/messages/search', data)
}
