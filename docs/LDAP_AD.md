# LDAP / Active Directory authentication and group authorization

> Status: implemented against the LDAP protocol and deterministic AD fixtures.
> The real-enterprise-AD checklist in this document has **not** been executed
> against the customer's domain controllers as part of this delivery.

The directory module is an optional, default-off extension. It adds LDAP/AD
password authentication, complete directory snapshots, nested/primary group
membership, workspace roles, and group restrictions for knowledge bases and
agents. Local and OIDC authentication remain available. Keep at least one
local system administrator that is not linked to a directory identity.

## Implementation baseline and compatibility

This module was developed in an isolated worktree from Tencent WeKnora
`main` commit `33c0333ec9474a6d090bab346e1d631a842bafaf` (2026-09-22,
`fix(chat): restore shared workspace images after stream completion (#3523)`).
The upstream source is <https://github.com/Tencent/WeKnora>; its commit history,
MIT license for WeKnora's own code, `THIRD_PARTY_NOTICES.md`, third-party
licenses, notices, and authorship are retained. The submission repository's
separate initial Apache-2.0 commit is retained in the merged Git history; it
does not replace the upstream worktree's `LICENSE` or third-party notices. The
schema additions follow the upstream versioned PostgreSQL migration sequence
and the SQLite test/lite migration sequence. Compatibility validation targets
the Go and Node versions pinned by that baseline plus the repository's Docker
build.

The completion audit on 2026-10-09 starts from the submission repository's
checked `main` commit `c5ee8c4e3b4148bbe8f3cf6db636eeaf95830e20`, which already
contains merged LDAP PR #1 and subsequent RAG/CI work. Development takes place
in a separate worktree; existing local deployments and unrelated uncommitted
Nextcloud/RAG work are preserved. No new schema migration is required by this
completion patch: LDAP remains PostgreSQL migration 109 / SQLite migration 28
within the current main sequences (115 / 34).

The audit fills the following gaps in the merged implementation:

- Cancellation and read/write deadlines cover TCP/TLS establishment, StartTLS
  upgrade, Bind and Search, including a controller that stops reading requests.
- Equivalent escaped or multi-valued AD DNs match the same membership object;
  malformed and duplicate equivalent member DNs fail the complete snapshot.
- A login that discovers a disabled or out-of-scope account requests a complete
  synchronization through the existing lease/cooldown path. A failed collection
  preserves the prior snapshot and cannot revoke users as an empty directory.
- Document/manual/FAQ processing and subsequent enrichment stages restore the
  admitted human identity and recheck live directory/resource access. A queued
  direct workspace Admin still loses access when its directory identity pauses.
  Rejected work closes the appropriate processing attempt without publishing
  content; Lite honors the same non-retryable cancellation as Redis workers.
- Human tasks in inherited KBs recheck their current workspace membership and
  role after admission. Durable Wiki operations retain each source's identity;
  a coalesced administrator trigger cannot authorize a revoked source. Mapping,
  reduction, model calls, finalization and Wiki writes recheck those identities.
- Restricted resource edit grants also work for workspace Viewers on applicable
  document/FAQ, batch reparse/tag/folder and agent editing paths. The separate
  `group_access_permission` response field informs the content UI without
  changing ownership/share metadata or granting management. Original-file
  downloads permit the explicit restricted edit grant; group read alone keeps
  the existing original-file download restriction.
- Resource previews include bounded, stable pages of workspace users, proposed
  permissions and direct/nested/primary group provenance. In inherited mode the
  preview reports `workspace`, leaving existing ownership/edit rules in force.
- Groups can be prepared while access still inherits from the workspace. This
  does not revoke that KB's current sharing capabilities. Entering restricted
  mode and subsequent directory permission changes revoke affected capabilities.
  The UI confirms the same proposed grants it previewed and discards responses
  from a previously selected resource.

The patch does not perform real enterprise AD acceptance. Historical limited
smoke evidence in PR #1 does not complete the checklist below.

## Directory browser

Workspace **Member management** also exposes **Synced AD users and groups**
to Owner/Admin users. This catalog reads the last committed sync, includes
users who have never logged in, supports name/account/email search and
pagination, and shows the automatic sync interval and last successful run.
It never auto-adds everyone to a workspace. Disabled identities are visible
but cannot be selected; stale sync pauses additions, not snapshot inspection.

`GET /api/v1/tenants/:id/directory/catalog/:kind` (`users` or `groups`)
requires the tenant path-match and Admin guards. Selecting a user uses
`POST /api/v1/tenants/:id/directory/members` with `object_guid` and `role`,
protected by the same Owner guard as ordinary direct member additions.
It safely provisions an AD-only user if needed, rejects local identity
collisions, rechecks snapshot freshness, and uses the standard audited
membership service. No session, local password, Owner role or system-admin
privilege is issued by this operation. Selecting a group uses the existing
Admin-protected group-role endpoint; direct/nested/primary memberships apply.
The group picker also offers **Members & origins** before assigning a role.
`GET /api/v1/tenants/:id/directory/groups/:object_guid/members` requires
tenant path-match and Admin authorization, and reads the committed snapshot
without connecting to AD. It supports search and pagination, shows distinct
effective members, direct/primary/nested origins, inherited group paths and
parent/child groups. A stale snapshot remains inspectable, while granting
access stays paused until a successful sync. The preview verifies that its
reconstructed membership set matches the stored authorization snapshot.

Synchronization runs automatically (default 300 seconds) and can also be
started manually from system administration. Synced visibility and workspace
authorization are separate states.

System administrators can open **Directory services → Users & groups** (the
Diagnostics tab in English). Name, login account, email and UPN are separate
columns. Empty searches list all in-scope objects; searches match name,
account, UPN, email or DN, case-insensitively. Users and groups support
20/50/100-row pages with deterministic name/account/GUID ordering. LDAP
changes between requests can change page contents; this is a live browser,
not a historical snapshot viewer.

The administrator-only `GET /api/v1/system/admin/directory/users` and
`/groups` endpoints accept `q`, `limit` (default 20, maximum 100), and
`offset` (default 0), returning the filtered `total` alongside the page.
`GET /api/v1/system/admin/directory/groups/:object_guid/members` supports
the same parameters and returns distinct effective users, all membership
origins, one deterministic shortest path per origin, direct parent/child
groups, and a count of unresolved direct member objects. Parent and child
groups can be opened from the detail panel. Disabled users remain visible
for inspection but cannot authenticate. Listing does not grant permissions.

Display names fall back through `displayName`, `name`, `cn`, then
`sAMAccountName`; no email or name is used to link identities. A successful
sync refreshes these names without a schema migration. Missing AD email
attributes render as a dash, rather than reusing UPN as an email address.

Regression coverage includes name fallback, Chinese/case-insensitive search,
stable and bounded pagination, direct/primary/nested membership origins,
shortest paths, missing groups, failed directory queries, and disabled
directory access. The local deployment was additionally checked against a
real AD for full user/group pagination and group-origin inspection; this is
not a replacement for the full lifecycle/failover acceptance checklist.

## Security model

Whole-domain searches include the optional AD domain-scope control
`1.2.840.113556.1.4.1339`, keeping queries within the configured naming
context instead of receiving continuation referrals to other partitions.
This applies to login, paged snapshots, and ranged membership queries.
Unexpected referrals and incomplete paging still fail closed; credentials
are never forwarded to referral URLs. Non-AD servers may ignore the control.
See [Microsoft's domain-scope specification](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-adts/ba5f20c6-7753-417c-b93d-e66e722458ed).

UPN login requires the account's `userPrincipalName` attribute to match the
entered identifier. AD may accept an implicit UPN for a direct Bind even when
that attribute is unset; in that case use `sAMAccountName` for application
login. Successful authentication does not automatically grant a workspace role.

- A read-only service account searches for the login object. On the selected
  controller connection, the application binds as the returned user DN to
  verify the password, then re-binds the service account before querying live
  direct, nested, and primary-group membership on that same controller.
- Empty passwords are rejected before a connection is opened. Login values are
  escaped as LDAP filter values and are never interpolated as raw filter text.
- Every connection uses certificate-verified LDAPS or StartTLS. Plain LDAP and
  `InsecureSkipVerify` are not supported. Private PKI roots can be supplied via
  `LDAP_CA_FILE`.
- Domain controllers are tried in configured order for transport, TLS,
  retryable service-bind, and search failures. Invalid service credentials are
  a terminal configuration error. `invalidCredentials` from the user bind is
  authoritative and is not repeated against another controller.
- Directory identities use `(directory_id, objectGUID)` as their stable key.
  DN, UPN, account name, display name, and email are attributes, so rename and
  OU moves do not create another account. Matching email alone never merges an
  existing local account; a system administrator must link a conflict.
- A linked directory identity cannot use, change, or receive a reset local
  password. Disabling or removing the identity revokes all persisted access and
  refresh tokens.
- UI-managed bind passwords are encrypted with the existing
  `SYSTEM_AES_KEY` AES-256-GCM mechanism. Saving a secret without a valid key is
  refused. Secret values and CA contents are never returned by APIs or logs.

## Configuration ownership

Set `LDAP_CONFIG_SOURCE=file` when YAML, environment variables, Docker
secrets, or Kubernetes secrets are authoritative. The system-management page
shows these values but makes deployment-owned fields read-only. Use
`LDAP_CONFIG_SOURCE=database` to let a system administrator maintain the
connection in the UI.

The minimal file-managed configuration is:

```dotenv
LDAP_ENABLED=true
LDAP_CONFIG_SOURCE=file
LDAP_DIRECTORY_ID=corp-ad
LDAP_URLS=ldaps://dc1.example.com:636,ldaps://dc2.example.com:636
LDAP_TLS_MODE=ldaps
LDAP_SERVER_NAMES=dc1.example.com,dc2.example.com
LDAP_CA_FILE=/run/secrets/ldap-ca.pem
LDAP_BIND_DN=CN=svc-weknora,OU=Service Accounts,DC=example,DC=com
LDAP_BIND_PASSWORD_FILE=/run/secrets/ldap-bind-password
LDAP_BASE_DN=DC=example,DC=com
LDAP_USER_BASE_DN=OU=Users,DC=example,DC=com
LDAP_GROUP_BASE_DN=OU=Groups,DC=example,DC=com
```

For StartTLS, use `ldap://...:389` URLs and `LDAP_TLS_MODE=starttls`. The full
set of filters, timeouts, paging, limits, sync, and stale-access settings is
documented in `.env.example`. The defaults are a five-minute sync interval and
a fifteen-minute stale threshold.

Docker Compose supports both `LDAP_BIND_PASSWORD` and the preferred
`LDAP_BIND_PASSWORD_FILE` read-only mount. The Helm chart supports either
`secrets.ldapBindPassword` / `LDAP_BIND_PASSWORD` or a read-only existing
Secret volume selected by `app.directory.bindPasswordSecretName` and
`bindPasswordSecretKey`. Configure exactly one password source. The Helm CA
mount is selected independently with `app.directory.caSecretName` and
`caSecretKey`.

## Synchronization and failure semantics

The scheduler and manual endpoint share both a per-process single-flight lock
and a database-backed lease, so only one sync for a directory may collect and
apply a snapshot across all application replicas. Lease ownership is renewed
while collection runs and expires after a crashed worker stops heartbeating,
allowing another replica to recover automatically. The adapter first reads
every user, group, direct membership, nested-group edge, and AD primary-group
relation into memory. It validates result limits, paging completion, object
identifiers, duplicate objects, and group cycles before persistence.

Only a complete snapshot is applied. Applying a snapshot is one database
transaction that upserts stable objects, replaces directory-managed edges and
effective memberships, marks no-longer-visible objects out of scope, and bumps
permission versions. A failed or partial read only records a failed sync run;
it never treats the directory as empty and never revokes all users.

New LDAP logins fail closed while the directory is unavailable. Existing
directory sessions remain usable only while the last complete snapshot is
fresh. After `LDAP_STALE_AFTER`, directory sessions are paused. A later
successful snapshot automatically restores access for identities that are
active and still in scope. Explicitly disabled/deleted/out-of-scope identities
have their sessions revoked immediately.

## Authorization semantics

- A directory group can grant workspace `viewer`, `contributor`, or `admin`.
  `owner` and system administrator are never directory-derived. When several
  groups match, the highest fresh role wins.
- Knowledge-base grants are `read` or `edit`; agent grants are `use` or `edit`.
  A resource grant is effective only for a member of the owning workspace.
  Workspace membership may come from a direct grant or another directory group.
  Cross-workspace shares cannot bypass this constraint. The editor and preview
  identify groups with no workspace-role association so administrators can
  distinguish a resource grant from admission to the workspace.
- `inherit` preserves normal workspace behavior. `restricted` requires a
  matching resource group for ordinary members. Workspace Owner/Admin retain
  management access. Edit grants do not grant delete, share management, access
  policy changes, or ownership transfer.
- Running an agent does not imply access to any knowledge base referenced by
  the agent. Each knowledge-base read/search is authorized again for the human
  caller.
- Restricted resources require a verifiable web-user identity. Tenant API
  keys, platform keys, anonymous shares, embeds, IM callbacks, and other
  synthetic principals are denied unless a future explicit identity mapping is
  added. Existing resources migrate as `inherit`, and disabling the module
  preserves legacy behavior.
- Changing group membership, grants, or resource mode increments a
  tenant-scoped permission version. Subsequent requests and background stages
  re-authorize. Switching a resource to `restricted` revokes its revocable
  resource grants; unrestricted long-lived presigned links are not issued for
  restricted resources.

Background payloads created before this patch, machine-triggered jobs and
system ingestion with no verifiable initiating human cannot process restricted
KBs. They retain legacy behavior for inherited KBs and when LDAP is off. For a
restricted KB, admit new work as an authorized web user; do not replay an old
anonymous payload to recover a rejected job. This also applies to chained
summary, question, image, graph, table, tagging and profile stages.
Wiki operations stored before this patch have no source actor and therefore
cannot process restricted KBs; re-admit them through an authorized user rather
than borrowing the identity of the coalesced trigger.

`POST /api/v1/group-access/:resource_type/:resource_id/preview` accepts the
proposed mode/grants plus `offset` and `limit` query parameters (default 0/100,
maximum limit 100). Aggregate impact counts cover all eligible workspace
members; `effective_users` is the selected page with `effective_users_total`,
`effective_users_offset`, `effective_users_limit` and
`effective_users_truncated`. User rows include current/proposed read or use
access, `permission_after`, workspace role and matching group sources/depths.
Only the existing authenticated workspace-management route can request it.
It is a preview against current directory state, not a saved authorization
capability: saving and executing later still check live permissions.

## Upgrade and rollback

1. Back up PostgreSQL/SQLite and retain the current `SYSTEM_AES_KEY`.
2. Deploy the new application and run the normal migrations. New resources
   default to `inherit`; no existing workspace membership is replaced.
3. Configure the directory while `LDAP_ENABLED=false`, test the connection,
   inspect search results and sync preview, then enable it and run one manual
   sync.
4. Link any email/account-name conflicts explicitly. Add workspace group roles
   before enabling resource restrictions.
5. Observe at least two scheduled successful syncs and the system audit log.

The down migrations remove directory identities and grants and therefore lose
their history. Export audit/sync records before a rollback. Turning the module
off is the preferred reversible rollback: legacy local/OIDC authorization and
all `inherit` resources continue to work.

## Automated validation

The repository includes unit coverage for TLS construction, empty-password
rejection, filter escaping, failover classification, objectGUID/SID/primary
group parsing, nested membership/cycles, identity conflicts, maximum-role
merge, restricted-resource downgrade, and failed-snapshot preservation.

The opt-in integration harness under `tests/integration/ldap` starts an
isolated LDAP service and checks service-account search, user bind, paging,
TLS, StartTLS, controller failover, and sync. AD-only binary attributes and
primary-group behavior use fixed protocol fixtures because OpenLDAP does not
implement Active Directory semantics.

The `LDAP directory` CI workflow now runs that isolated wire harness and a
separate disposable PostgreSQL/SQLite migration gate. The migration gate checks
default-off installation, inherited policies, role/permission constraints,
directory upgrade/rollback and preservation of existing workspace data. Other
Go and frontend CI retain the full regression suites and normal Docker builds.
Each fixture has its own Compose project, and debug ports bind to loopback.

### Completion validation, 2026-10-09

The complete Go suite at code revision `35e630125203c441166ded588ad8141baf4cdc5e`
passed 11,435 tests, with 40 existing external-environment/opt-in skips. `go vet`
and the server build passed. The only later Go change before submission is
field-tag whitespace at `13995a35ef214fdfd31fb66626d44f8bc5f1a8a9`;
golangci-lint 2.12.2 against checked main `c5ee8c4e` reports zero new issues
with the unchanged lint configuration.

- Disposable PostgreSQL 17: full migration chain to 115, directory 108→109→108
  round trip, constraints and preservation checks pass. SQLite upgrade/schema/
  rollback tests through 34 pass in the same database package run.
- Isolated LDAP wire suite: all five tests pass on the submitted adapter,
  including both TLS transports, paging, Bind, ordered failover and untrusted
  certificate rejection. Directory cancellation/deadline race tests also pass.
- Frontend: 1,217 tests pass, one existing human-handoff fixture is skipped;
  Vue type-check and Vite production build pass. Standard UI Dockerfile builds
  the arm64 image `weknora-ldap-completion-ui:20261009-group-edit`, image ID
  `sha256:8becfb4e49348c8987e12bcaf502d3f60bc51157b37d15c3bc485900999e490e`.
- Bash acceptance/fixture syntax, workflow actionlint and clean Git diff checks
  pass. Standard backend `WITH_ANYDOC=1` image verification remains a required
  PR check; consult the PR's exact-head checks for its result.

The local Go runner needed Debian's `python3-venv` for an existing skill-runtime
test. This dependency was added to an isolated test image; that test then passed
without changing or skipping it. No running deployment was rebuilt or restarted
by this completion work. Enterprise AD acceptance remains outside this run.

## Real AD acceptance checklist (not yet executed)

Run `scripts/ldap/acceptance-real-ad.sh` from a secured administrator host.
Provide the bind password through a protected file; do not put it on the
command line or commit it. Record evidence without capturing passwords,
tokens, or full directory exports.

Set `AD_URLS=ldaps://dc1.example.com:636,ldaps://dc2.example.com:636` with
`AD_TLS_MODE=ldaps`, or use `ldap://...:389` URLs with
`AD_TLS_MODE=starttls`. `AD_URL` and legacy `AD_LDAPS_URL` remain accepted for
single-controller runs. Use `AD_TLS_SERVER_NAMES` when URL hosts do not match
the certificate names. The helper validates every controller's certificate
chain and hostname, service bind, critical paged search, escaped lookup, GUID,
SID, and primary-group attributes. Set `AD_TEST_GROUP` to check a group and
`AD_TEST_USER_PASSWORD_FILE` to perform a real user bind without exposing the
password on the command line. When the WeKnora API variables are also set, the
same protected password file is streamed as JSON to the LDAP login endpoint
after a successful full sync, exercising the application's same-controller
live direct, primary, and nested-group verification without logging the
password or returned tokens.

For the failover drill, configure the application with the same ordered
`AD_URLS`, intentionally stop or isolate the first DC, then run with
`AD_FAILOVER_DRILL=true`, `WEKNORA_BASE_URL`, and
`WEKNORA_ADMIN_TOKEN_FILE`. The script requires at least one unavailable and
one healthy DC, triggers preview/sync, and verifies that `active_server` is a
healthy controller. Restore the DC immediately after collecting evidence.

- Verify LDAPS certificate chain, hostname, expiry, and enterprise CA rotation.
- Repeat with StartTLS if that transport will be supported in production.
- Stop the first configured DC and confirm failover to the second. Enter a
  wrong user password and confirm the second DC receives no user-bind attempt.
- Authenticate once with `sAMAccountName` and once with UPN.
- Rename a test account and move it to another OU; confirm the WeKnora user ID
  is unchanged because `objectGUID` is stable.
- Exercise direct, two-level nested, cyclic test groups, and a non-default AD
  primary group; compare effective membership and provenance in the UI.
- Create an intentional email collision with a local user and confirm no
  automatic link occurs; link it explicitly as a system administrator.
- Remove a user from the allowed scope and disable another account; confirm
  active sessions are revoked and resource access disappears.
- Repeat the helper with `AD_EXPECT_LOGIN_ABSENT=true` after the scope change
  to prove that the configured user base/filter no longer returns that user.
- Make all DCs unreachable. Confirm new LDAP login fails, the last snapshot is
  retained, and existing directory access pauses only after the configured
  stale threshold. Restore a DC and confirm eligible access resumes.
- Assign overlapping workspace group roles and confirm the highest role wins,
  with no Owner/system-admin elevation.
- Restrict one knowledge base and one agent. Test list/search/RAG, document and
  chunk reads, download/grant URLs, agent execution, linked KB checks, API keys,
  share, embed, and IM paths, including cross-workspace attempts.
- Revoke a group and confirm the next request and an in-flight/background
  stage re-check permission; confirm revocable share/resource grants no longer
  work.
- Export the system audit events for configuration, sync, identity link, and
  grant changes, then verify no bind password is present.
