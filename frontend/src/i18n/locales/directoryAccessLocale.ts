/**
 * LDAP/AD keys shared by all five locale bundles. Keeping the shape in one
 * place makes dynamic status/role/source keys impossible to drift. Locales
 * can override individual leaves later without changing the contract.
 */
export const directoryAccessLocale = {
  directoryAdmin: {
    title: 'Directory services',
    description: 'Configure LDAP/Active Directory authentication, synchronization, and health.',
    refresh: 'Refresh',
    loading: 'Loading directory configuration…',
    readOnlyHint: 'Fields supplied by an environment variable or file are read-only here.',
    source: { database: 'UI managed', env: 'Environment managed', file: 'File managed', default: 'Default', mixed: 'Mixed sources' },
    tabs: { configuration: 'Configuration', diagnostics: 'Diagnostics', sync: 'Synchronization', runs: 'Run history' },
    status: {
      activeServer: 'Active server', lastSuccess: 'Last successful sync', nextSync: 'Next sync', failures: 'Failures',
      accessPaused: 'Directory access is paused because the last successful sync is stale.',
      lastError: 'Last directory error: {error}', disabled: 'Disabled', paused: 'Access paused', healthy: 'Healthy', unavailable: 'Unavailable',
    },
    sections: { search: 'Search scope', serviceAccount: 'Read-only service account', limits: 'Timeouts and limits' },
    fields: {
      enabled: 'Enable directory module', enabledHint: 'Local and OIDC authentication remain available.', displayName: 'Provider display name',
      servers: 'Domain controllers', serversHint: 'One host:port per line, tried in order.', transport: 'Secure transport', caFile: 'Enterprise CA file',
      caHint: 'The server certificate is always verified. Add the enterprise CA rather than disabling verification.',
      baseDn: 'Base DN', userBaseDn: 'User search base', groupBaseDn: 'Group search base', userFilter: 'User filter', groupFilter: 'Group filter',
      filterHint: 'LDAP filter configured by an administrator; login input is escaped by the server.', allowedLoginFilter: 'Allowed-login filter',
      allowedLoginFilterHint: 'Users outside this filter cannot authenticate.', loginAttributes: 'Login attributes', bindDn: 'Service account DN', bindPassword: 'Service account password',
      passwordPlaceholder: 'Enter only to replace the stored secret', passwordManaged: 'Managed by {source}; value is never returned.',
      passwordStored: 'A password is stored. Leave blank to keep it.', passwordMissing: 'No password is stored.',
      connectTimeout: 'Connection timeout (seconds)', queryTimeout: 'Query timeout (seconds)', resultLimit: 'Result limit', pageSize: 'LDAP page size',
      syncInterval: 'Sync interval (seconds)', staleAfter: 'Pause access after (seconds)',
    },
    actions: { test: 'Test connection', preview: 'Preview sync', syncNow: 'Sync now' },
    diagnostics: {
      title: 'Connection and lookup', description: 'Test TLS/bind and inspect a bounded directory search.', users: 'Users', groups: 'Groups',
      searchPlaceholder: 'Search by account, UPN, display name, or group', disabled: 'Disabled', truncated: 'Results were truncated by the configured limit.',
      empty: 'No matching directory objects.', searchFailed: 'Directory search failed.',
    },
    browse: {
      search: 'Search name, account, email, or group; leave empty to show all',
      memberSearch: 'Search member name, account, or email', total: '{count} results',
      name: 'Name', groupName: 'Group name', searchButton: 'Search', account: 'Login account', email: 'Email', status: 'Status', enabled: 'Enabled',
      direct: 'Direct users', effective: 'Effective users', details: 'Members & origins',
      parents: 'Parent groups', children: 'Child groups', origins: 'Membership origins', depth: '{count} levels',
      originHint: 'Includes direct, primary-group and nested membership. Each origin shows one shortest group path. Membership alone does not grant workspace access; disabled users cannot sign in.',
      unresolved: '{count} direct member objects are outside the selected user/group scope (for example computers or foreign principals). They do not grant access.',
    },
    catalog: {
      choose: 'Select AD users or groups',
      emailInvite: 'Invite by email',
      title: 'Synced AD users and groups', hint: 'All synchronized objects are searchable here. Being listed does not grant workspace access.',
      disabled: 'Directory synchronization is disabled.', synced: 'Synced', select: 'Add to workspace', linked: 'Group linked',
      sync: 'Auto-sync every {minutes} minutes · Last success: {time}', stale: 'Directory synchronization is unavailable or stale. Browsing the saved snapshot is allowed; additions are paused.',
      userHint: 'Choose a workspace role. No prior login is required; this identity must still authenticate through AD.',
      groupHint: 'The selected role applies to direct, nested, and primary-group members. Owner and system-admin roles are never granted.', added: 'Workspace authorization added.',
    },
    sync: {
      title: 'Directory snapshot', description: 'Preview changes before starting an exclusive synchronization.', incomplete: 'The preview is incomplete and will not be applied.',
      users: 'Users', groups: 'Groups', memberships: 'Memberships', emptyPreview: 'Run a preview to inspect changes.', previewFailed: 'Sync preview failed.',
      started: 'Synchronization started.', startFailed: 'Could not start synchronization.',
    },
    runs: {
      title: 'Synchronization runs', startedAt: 'Started', trigger: 'Trigger', statusLabel: 'Status', summary: 'Snapshot', error: 'Error', empty: 'No runs yet.',
      summaryValue: '{users} users · {groups} groups · {memberships} memberships',
      status: { queued: 'Queued', running: 'Running', success: 'Succeeded', failed: 'Failed' },
    },
    messages: {
      loadFailed: 'Could not load directory settings.', required: 'Servers, base DN, and bind DN are required.', saved: 'Directory settings saved.',
      saveFailed: 'Could not save directory settings.', testSuccess: 'Directory connection succeeded.', testFailed: 'Directory connection failed.',
    },
    login: {
      local: 'Local account', directory: 'Directory account', identifier: 'Account or UPN', identifierPlaceholder: 'sAMAccountName or user{\'@\'}domain',
      identifierRequired: 'Enter your directory account or UPN.',
    },
  },
  directoryGroups: {
    title: 'Directory groups', description: 'Associate synchronized groups with workspace roles.', add: 'Add group', empty: 'No directory groups are linked.',
    searchPlaceholder: 'Search directory groups', candidateEmpty: 'No available groups.', group: 'Group', members: 'Members', role: 'Workspace role', actions: 'Actions',
    direct: 'Direct', effective: 'Effective', nested: 'Nested groups', removeConfirm: 'Remove {name} from this workspace?', selectRequired: 'Select a group.',
    loadFailed: 'Could not load directory groups.', searchFailed: 'Could not search directory groups.', added: 'Directory group added.', addFailed: 'Could not add directory group.',
    updated: 'Group role updated.', updateFailed: 'Could not update group role.', removed: 'Directory group removed.', removeFailed: 'Could not remove directory group.',
    roles: { viewer: 'Viewer', contributor: 'Contributor', admin: 'Admin' },
  },
  groupAccess: {
    title: 'Group access', description: 'Control directory-group access without bypassing workspace membership.', inherit: 'Inherit workspace access',
    inheritHint: 'Keep the existing workspace-role behavior.', restricted: 'Only specified groups', restrictedHint: 'A directory user must match a grant below.',
    managerBypass: 'Workspace Owner and Admin retain management access. Using an agent does not grant access to its knowledge bases.',
    groupsTitle: 'Allowed groups', addGroup: 'Add group', selectGroup: 'Select a workspace-linked group', empty: 'No groups are granted access.',
    effectiveMembers: '{count} effective members', missingWorkspaceLink: 'This group is not linked to the workspace; its resource grant cannot provide access.',
    previewTitle: 'Review access impact', previewBody: 'Switching to restricted mode changes who can use this resource.', confirmRestricted: 'Apply restricted mode',
    currentlyAllowed: 'Currently allowed', allowedAfter: 'Allowed after', losing: 'Losing access', gaining: 'Gaining access', unaffected: 'Managers retained',
    loadFailed: 'Could not load group access.', previewFailed: 'Could not preview access impact.', saved: 'Group access saved.', saveFailed: 'Could not save group access.',
    permissions: { read: 'Read', use: 'Use', edit: 'Edit' },
    sources: { direct: 'Direct member', nested: 'Nested member', primary: 'Primary group' },
  },
} as const
