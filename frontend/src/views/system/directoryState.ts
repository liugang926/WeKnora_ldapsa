import type { DirectoryConfig, DirectoryConfigUpdate, DirectoryServer } from '@/api/directory'

export function parseDirectoryServers(value: string): DirectoryServer[] {
  const seen = new Set<string>()
  const servers: DirectoryServer[] = []
  for (const line of value.split(/\r?\n/)) {
    const address = line.trim()
    if (!address || seen.has(address)) continue
    seen.add(address)
    servers.push({ address })
  }
  return servers
}

export function directoryServersText(servers?: DirectoryServer[]): string {
  return (servers || []).map((server) => server.address).filter(Boolean).join('\n')
}

export function isDirectoryFieldReadOnly(readOnlyFields: readonly string[], field: string): boolean {
  return readOnlyFields.some((entry) => entry === field || entry.startsWith(`${field}.`))
}

export function buildDirectoryConfigUpdate(
  config: DirectoryConfig,
  serverText: string,
  replacementPassword: string,
): DirectoryConfigUpdate {
  const payload: DirectoryConfigUpdate = {
    enabled: config.enabled,
    display_name: config.display_name.trim(),
    servers: parseDirectoryServers(serverText),
    transport: config.transport,
    ca_file: config.ca_file?.trim() || '',
    base_dn: config.base_dn.trim(),
    user_base_dn: config.user_base_dn?.trim() || '',
    group_base_dn: config.group_base_dn?.trim() || '',
    bind_dn: config.bind_dn.trim(),
    user_filter: config.user_filter.trim(),
    group_filter: config.group_filter.trim(),
    allowed_login_filter: config.allowed_login_filter?.trim() || '',
    login_attributes: Array.from(new Set(config.login_attributes.map((item) => item.trim()).filter(Boolean))),
    connect_timeout_seconds: config.connect_timeout_seconds,
    query_timeout_seconds: config.query_timeout_seconds,
    result_limit: config.result_limit,
    page_size: config.page_size,
    sync_interval_seconds: config.sync_interval_seconds,
    stale_after_seconds: config.stale_after_seconds,
  }
  if (replacementPassword) payload.bind_password = replacementPassword
  return payload
}
