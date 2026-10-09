const RETURN_KEY = 'weknora_nextcloud_ask_return'

/** Accept only a local, bounded Files-sidebar target. Never an external URL. */
export function safeNextcloudAskPath(value: unknown): string | null {
  if (typeof value !== 'string' || value.length > 1024 || !value.startsWith('/platform/nextcloud-ask?')) {
    return null
  }
  let parsed: URL
  try {
    parsed = new URL(value, 'https://weknora.invalid')
  } catch {
    return null
  }
  if (parsed.origin !== 'https://weknora.invalid' || parsed.pathname !== '/platform/nextcloud-ask' || parsed.hash) {
    return null
  }
  const query = parsed.searchParams
  if ([...query.keys()].length !== 4 ||
      ['instance_id', 'binding_id', 'file_id', 'source_etag'].some(key => query.getAll(key).length !== 1)) {
    return null
  }
  if (!/^[A-Za-z0-9_-]{1,128}$/.test(query.get('instance_id') || '') ||
      !/^[A-Za-z0-9_-]{1,128}$/.test(query.get('binding_id') || '') ||
      !/^[1-9][0-9]{0,18}$/.test(query.get('file_id') || '') ||
      !/^[A-Za-z0-9._:-]{1,256}$/.test(query.get('source_etag') || '')) {
    return null
  }
  return parsed.pathname + parsed.search
}

export function rememberNextcloudAskReturn(value: unknown): void {
  const safe = safeNextcloudAskPath(value)
  if (safe) sessionStorage.setItem(RETURN_KEY, safe)
}

export function consumeNextcloudAskReturn(): string | null {
  const stored = sessionStorage.getItem(RETURN_KEY)
  sessionStorage.removeItem(RETURN_KEY)
  return safeNextcloudAskPath(stored)
}
