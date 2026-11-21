// Local routes only. The server registry determines which source types are installed.
export function managerHref(type: string, kbId: string, sourceId?: string): string | undefined {
  if (type !== 'nextcloud_webdav' || !kbId) return
  const query = new URLSearchParams({ kb_id: kbId })
  if (sourceId) query.set('source_id', sourceId)
  return '/api/v1/extensions/nextcloud-webdav/?' + query
}
