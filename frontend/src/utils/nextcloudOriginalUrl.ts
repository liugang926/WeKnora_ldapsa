/** A Nextcloud citation uses the human Files route, never the machine feed. */
export function nextcloudOriginalUrl(metadata?: Record<string, string>): string {
  const fileID = metadata?.nextcloud_file_id || ''
  const raw = metadata?.nextcloud_human_url || ''
  if (!/^[1-9][0-9]{0,18}$/.test(fileID) || !raw || raw.length > 2048) return ''
  try {
    const parsed = new URL(raw)
    if (!['http:', 'https:'].includes(parsed.protocol) || !parsed.hostname ||
        parsed.username || parsed.password || parsed.search || parsed.hash ||
        !parsed.pathname.endsWith(`/f/${fileID}`)) return ''
    return parsed.href
  } catch {
    return ''
  }
}
