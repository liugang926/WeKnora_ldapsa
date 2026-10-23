# Nextcloud directory onboarding

This protocol creates **knowledge bases in an existing tenant**. It never creates a tenant or ordinary tenant membership. Onboarding machine routes use TLS and the separate Nextcloud server automation credential. The administrator connection routes below additionally accept an Owner/Admin browser session or a platform API key; tenant-scoped content keys cannot call them.

## Administrator connection

The current machine contract is version `2`. Additive v2 fields are safe for
older clients, while a client that cannot understand the negotiated contract
must keep existing sources readable and block new bindings. The connection
workspace response includes a bounded `health` projection with worker and
dispatcher observations; absence is `unverified` and never healthy.

An administrator with a WeKnora platform API key (`system_tenants_read` and
`system_tenants_manage`) or an Owner/Admin browser session can use the
connection setup routes without pre-installing onboarding credentials:

- `GET /api/v1/integrations/nextcloud/connection/workspaces` lists eligible
  workspaces and, when one already exists, the default processing template
  selected from each workspace.
- `POST /api/v1/integrations/nextcloud/connection/authorize` accepts
  `instance_id`, canonical `base_url`, `directory_id`, optional verified
  `actor_directory_id`/`actor_object_guid`, and one or more `tenant_ids`. It
  creates one scoped credential per workspace and returns each token exactly
  once.
- `POST /api/v1/integrations/nextcloud/connection/revoke` disables the exact
  connection-issued credentials. A Nextcloud instance can also revoke its own
  connection through `POST /api/v1/integrations/nextcloud/onboarding/connection/revoke`
  using any scoped onboarding credential it holds.
- `GET /api/v1/integrations/nextcloud/onboarding/health` lets a Nextcloud
  instance refresh the non-secret worker/dispatcher observation with one of
  its scoped credentials. Missing or stale observations fail closed and do not
  authorize a new binding.

Connection credentials are held only by the Nextcloud server. They are
instance-scoped and still require a live verified directory identity; the
Nextcloud server verifies the current folder manager before selecting that
actor. A folder manager authorized by Nextcloud may bind any eligible
knowledge base in a workspace authorized by the connection. Existing
credentials with durable onboarding operations are retained when a connection
is re-authorized.

An eligible workspace does not require an operator to create a template first.
Authorization reuses a valid existing template or creates a workspace-local
`Nextcloud processing template` in the same database transaction as the scoped
credential. The generated template is empty and uses an active workspace
storage backend. It prefers vector plus keyword processing when an active
embedding model is available; otherwise it uses graph-only processing with an
active chat model. Later authorizations reuse the same template. If neither a
valid template nor usable workspace defaults exist, discovery reports
`no_processing_template` instead of enabling a connection that cannot process
content.

## Initial operator configuration

Apply all packaged migrations through PostgreSQL `000165` / SQLite `000084` before starting this binary. The earlier onboarding sequence (`000155`–`000161` / `000074`–`000080`) alone is insufficient: the current release also requires the original-body retention and later lifecycle controls. Production must use the offline journal initialization/verification procedure in [the production runbook](../deploy/production/README.md), with application readers stopped. The matching Nextcloud plugin requires app migrations through `0029`; migrate both owned databases before enabling workers. An operator installs one row in `nextcloud_onboarding_credentials` per trusted instance/tenant/template:

| Column | Configuration |
| --- | --- |
| `id` | Stable administrator chosen credential ID |
| `token_sha256` | Lowercase SHA-256 of a random server token of at least 32 bytes; the token belongs only in private Nextcloud system configuration or an injected secret store |
| `instance_id`, `base_url` | Exact pinned Nextcloud instance and canonical HTTPS origin |
| `tenant_id` | Existing allowed tenant |
| `directory_id` | Same trusted directory ID used by NC GUID mappings |
| `template_kb_id` | Existing document KB whose administrator approved processing configuration is copied; storage backend and the models required by its indexing strategy must be active |
| `enabled` | Explicitly enabled after configuration validation; disabled by default |
| `max_knowledge_bases` | Maximum retained operations for this credential; reserved operations count too |

The actor must be an active verified directory GUID with target Owner/Admin membership, or an explicit row in `nextcloud_onboarding_delegations(tenant_id,directory_id,object_guid)`. GUID delegation also works before the first WeKnora login; it does not manufacture a user or membership. Source directory write/share/read permissions do not grant target management. Do not paste either the server token or one-time pairing token into logs or checked-in configuration.

A fresh document KB created after this migration receives a database virgin-inventory witness. Historical KBs intentionally receive no witness; a zero document count cannot certify past vectors, tasks, leases or GC. There is no automatic takeover or timeout release for unknown history. Content/data-source/chunk/wiki/profile mutations permanently invalidate the witness. Reserved KBs reject concurrent ordinary content writes. Active sources retain the existing provenance/GC/recovery barriers.

## HTTP contract

Prefix: `/api/v1/integrations/nextcloud/onboarding`. Header: `Authorization: Bearer <server token>`. Responses use `Cache-Control: no-store`.

1. `GET /candidates?tenant_id=7&processing_template_id=...&actor_directory_id=...&actor_object_guid=...&q=&cursor=&limit=` returns contract v2 fields `contract_version`, `request_id`, `candidate_revision`, `candidates`, `ineligible`, `next_cursor`, and `total` together with the existing tenant/template descriptor. Eligible candidates include `content_count`, `can_take_over`, and a configuration summary. Visible but ineligible KBs are returned with a stable `reason_code`, human-readable explanation and `next_action`; permission failure is an HTTP 403 and is never represented as an empty space. `graph_mode` is `off`, `optional`, or `required`; no model instructions or secrets are returned. Usable document KBs in the credential's exact tenant are returned even when they already contain content, provided they have verified configuration provenance and have never been attached to another Nextcloud source.
2. `POST /operations` receives `operation_id` (UUID), `tenant_id`, `instance_id`, `binding_id`, `actor_directory_id`, `actor_object_guid`, `mode` (`create` or `existing`), `name`, `knowledge_base_id` (for existing mode), `processing_template_id` (defaults to the credential's template), and optional `candidate_revision`. A new operation whose supplied revision does not match the current scoped candidate observation receives HTTP 409 with `nextcloud_candidate_revision_changed`; an exact operation-ID replay still returns its durable result. It returns `{operation:{operation_id,knowledge_base_id,state:"reserved",revision:1,...}}`. Store the exact immutable intent before the Nextcloud node operation.
3. Prepare the NC binding against this reserved KB using the existing transactional encrypted handoff outbox.
4. `POST /operations/:operation_id/pair` receives the existing pairing tuple: `knowledge_base_id`, `base_url`, `binding_id`, `operation_id`, `instance_id`, `publication_epoch`, `key_id`, `token`. It verifies signed NC capabilities, persists the paused source, signs the exact commit and activates only after ACK. The response contains `operation`, `pairing`, and `event_connection:{connection_id,key_id,secret,receiver_url}`. Resolve the relative receiver URL against the WeKnora origin. Retry the exact operation after ambiguous ACK; do not create another KB.
5. `GET /operations/:operation_id` returns recoverable `operation`, `pairing`, `sync_enabled`, and the exact active event connection credential. Credentials are decrypted only for this pinned operation and an actor still authorized for target management. Optional current actor query fields let another delegated manager recover/manage the operation.
6. `PUT /operations/:operation_id/grants` receives `{permission_version,publication_epoch,complete:true,subjects:[{kind:"user"|"group",directory_id,object_guid,source_relation_id}]}`. The trusted NC server submits a complete authoritative root audience. Partial snapshots, different epoch, older revisions and conflicting equal revisions cannot revoke or widen access. This background route does not require the original creator to remain an administrator: creator departure must not prevent revocation. Stable unmapped/conflicting subjects reject the whole snapshot transaction; an identity not yet linked to a user can be retained and becomes effective only after verified directory linking.
7. `POST /operations/:operation_id/sync` receives `{sync_enabled,expected_revision,actor_directory_id,actor_object_guid}`. It uses CAS; a lost-response retry with the same old revision and desired flag is idempotent. GET status supplies the current flag/revision. Sync pause is independent of source read withdrawal.

The grant ledger retains each source relation separately and never writes `tenant_members`, `tenant_group_role_grants`, or manual/directory resource grants. Removing one source relation/KB preserves other grants. Effective scoped entry requires active identity/group status, a complete current directory snapshot no older than 300 seconds, no directory sync error, an active pair and active source. The existing signed NC file authorization and current publication/version guards remain required for every actual content read.

## Source-only QA

A user whose only qualification is the source ledger receives `scoped_only` temporary entry; no ordinary Viewer member is persisted. Inherited KBs and unrelated agents are denied even when a policy is absent or rollout RBAC is disabled. Last-share removal and directory failure cannot fall back to legacy workspace Admin.

Such a user may create a private session and use `/knowledge-chat/:session_id` or `/knowledge-search` with explicit granted `knowledge_base_ids`. Sessions are marked `nextcloud_scoped` by the server. Only the user's marked session detail, message loading, stream continuation and stop routes are enabled. All requested KBs are re-authorized before QA; the existing file/lineage/stream checks still run. Agent execution, model overrides, attached documents/images, browser/web search, MCP, skills and general session/history/model lists remain closed to source-only entry. Ordinary existing members retain their normal role and unrelated legitimate access.


## Tenant and processing-template selection

A Nextcloud rule has `targets:[{tenant_id,processing_template_id,credential_id}]`. The exact tenant/template pair is unique; one tenant can offer several approved templates with independent credentials. The rule's exact default pair must be allowed. An explicit tenant with an ambiguous omitted template is rejected. Browser-selected model IDs, raw processing configuration and credentials are never accepted. Nextcloud validates the returned candidate tenant/template against the chosen scoped credential and retains the immutable pair in its intent.

New KBs copy the approved template's processing settings. Adopting an existing KB preserves its ID, metadata, content and existing processing/storage configuration; the integration validates that configuration instead of replacing it with the template. The KB becomes managed by the bound Nextcloud source: existing content remains, while subsequent non-Nextcloud content writes are rejected. A KB that was already used by another Nextcloud source, or whose configuration provenance cannot be verified, remains ineligible. Required graph mode cannot be enabled with graph extraction disabled. Model/backend checks prove active configured resources; actual source/model/provider failures remain visible and fail closed rather than being reported as successful processing.

## Root rename and target migration

Same-rule moves/renames use `POST /operations/:id/root-migrations` with a durable migration UUID. NC stops the source before the filesystem event, records its same physical file ID/storage/rule proof and external journal boundary, then serves the proof and accepts an exact signed ACK with the original source key. WK releases every old read/build lease token while preserving successful immutable body/graph generations. Only a finalized exact proof authorizes the new root/publication epoch; root-relative paths remain unchanged. A failed or ambiguous ACK keeps the durable phase; a late additional move cannot reuse an older ACK. This does not rewrite source bytes or re-stamp graph receipts.

Changing the target tenant, KB or processing template creates a fresh reserved blank target. `POST /operations/:old/target-migrations` retires only the old source's publication/build generations and grant ledger. It keeps the old KB/source history and hands NC the exact new operation/binding. NC retires the old machine/event keys and atomically creates the new intent for the same physical root. Finalization at `/target-migrations/:uuid/finalize` requires fresh permissions plus the complete stable supported manifest and actual completed/indexed/required-graph results; an empty manifest is an empty successful build. No IDs are overwritten to disguise a transfer. Unknown outcomes preserve the same target and migration UUID. NC exposes authenticated folder-scoped GET/POST `.../folders/:id/target-migrations/:uuid` status/retry, and an exact begin retry remains valid after the local handoff.

## Coordinated control recovery

The external authenticated publication journal now distinguishes `content_write_begin` and `content_write_complete`. Replay records monotone physical-file revision floors; it never assigns a retained revision to restored bytes. An old completion cannot clear a later retained floor or incomplete write. A genuine next write advances beyond `max(actual,floor)`.

Replay closes NC and WK control gates, pauses all restored sources, retires old body generations/leases, disables automation credentials and removes only the source grant ledger. Full control inventories include rule/intents, encrypted handoffs, source/root/target identities, graph/state records, current grants and protective disable state. Missing current control or pairing tables remain closed once the independent onboarding marker is present; a missing gate row never substitutes for an original restore receipt during reopening.

Recovery proceeds through owner-only maintenance tools:

1. Obtain a newly authenticated external terminal journal pin and inspect current control inventory. `integration_weknora:replay-recovery-ledger --reconcile-controls --plan <signed-latest-tail> --key-file <private-key> --control-plan-sha256 <original-plan> --control-inventory-sha256 <current-pin> --actor <current-LDAP-admin>` validates all control tables, live GUID/admin authority, each supported complete root audience/tree, content floors and exact current journal head. It only releases the global NC barrier; **every source remains stopped**.
2. Explicitly resume verified sources through the normal current-ACL management path. Use the existing WK DB-owner `publication-recovery --mode=reconcile` for each original source. Signed fresh pairing plus two complete stable manifest scans permit new generations while old generations remain permanently retired.
3. WK control closure still denies ordinary owners and source-only readers, KB metadata and read/search leases. A maintenance rebuild is limited to an immutable reconciled scope for the same plan/tenant/KB/source, a newer KB epoch, exact manifest path/ETag/content revision and a non-retired new candidate. Each new build lease receives an immutable `nextcloud_recovery_build_leases` permit; final graph SQL/provider writes verify that exact lease/plan/scope. Old workers, old lease IDs and unrelated sources cannot borrow this admission.
4. After complete current indexed coverage, obtain a fresh WK `--mode=snapshot` and control inventory pin. `--mode=reopen-controls --plan <signed-latest-tail> --key-file <private-key> --plan-sha256 <original-plan> --inventory <original-pinned-inventory> --inventory-sha256 <original-pin> --control-inventory-sha256 <current-pin> --actor-directory-id ... --actor-object-guid ...` rechecks complete live manifests, non-retired indexed generations, signed NC `recovery-state` against the exact latest external head, current directory management authority and inventory CAS. It does not start public ingress.
5. Disabled automation tokens stay disabled. Reauthorizing a chosen token requires the explicit owner-only `--mode=reauthorize-onboarding --onboarding-credential-id ...` action with original plan, current inventory pin and fresh authorized GUID. NC then submits a fresh complete source grant snapshot through normal reconciliation. Other manual, group and tenant roles remain independent.

Private recovery inputs/outputs must be owned regular files with mode `0600` in owned `0700` directories, with no symlinks or replacement during reads. No public endpoint accepts a recovery-open boolean or an arbitrary source URL. Unknown external pins, ACLs, incomplete content writes, missing tables, stale current manifests or changed inventories keep the affected source closed.

The local component tests use disposable services. The paired runtime fixture explicitly uses synthetic WK actor/model inventory and empty roots; it does not attest production AD, remote LLM quality, or 100 GB ingestion. Consult retained raw logs and the final acceptance matrix for exactly which checks ran.


Missing onboarding control tables are compatible only with a complete legacy schema. Independent current-module markers (such as `sessions.nextcloud_scoped`, credentials or recovery controls) survive a dropped ledger and make the group repository return an authorization error. Actual repository-to-service-to-role tests cover disabled directory mode, RBAC on/off, prevented legacy Admin and orphan Owner promotion, preserved direct Contributor membership, and legitimate legacy rollout behavior. See `processing-fourth-short-window-receipt.json` in the evidence directory for command-level results.
