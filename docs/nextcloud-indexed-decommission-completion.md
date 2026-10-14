# Indexed-source decommission: completion design and release gates

Status: implementation plan only. **No indexed-source cleanup ACK is enabled.**
This plan applies PRD §7.5 and §7.7 to a source that has already entered the
PostgreSQL logical-withdrawal stage documented in the patched WeKnora
`docs/nextcloud-indexed-withdrawal.md`. The current withdrawal permanently
blocks publication and records observed SQL identities, but returns
`inventory_complete: false`. Keep the Nextcloud binding stopped and retain its
pairing and signing key until the completion protocol below is implemented and
verified.

## Current boundary

- `nextcloud_indexed_withdrawals` is an immutable source fence. Its
  `nextcloud_indexed_withdrawal_items` table is append-only observation, not a
  complete resource list: `nextcloud_source_versions` retains only a latest
  candidate; a prior hard-deleted knowledge row or a former vector backend may
  leave no local row to scan. `unverified_external_index` is deliberately
  permanent in today's inventory.
- `nextcloud_gc_jobs` and `nextcloud_gc_items` have exact known local entries,
  but `derived_index` remains blocked. The current local-object deletion path
  accepts only a persisted `resource://` handle resolving to an exact local
  backend-scoped path and does not verify remote storage release.
- The general `DeleteKnowledge` path groups vector deletion using the KB's
  *current* `vector_store_id`, model and type; those values do not prove where
  an older generation was indexed. It also has best-effort file and Wiki
  cleanup paths. It cannot be the ACK implementation.
- PostgreSQL migration `000125` rejects writes to withdrawn knowledge,
  chunks and embeddings. Future cleanup must add a narrowly scoped deletion
  path; silently bypassing this fence would also admit stale builders.
- Nextcloud `SourceDecommissionService::acknowledge` currently accepts only
  the SHA-256 of an empty inventory. Preserve that path for genuinely empty
  sources. An indexed source needs a distinct endpoint and proof format.

## Persist exact lineage before enabling cleanup

Add durable tables rather than widening the immutable withdrawal row:

| Entity | Required immutable fields | Purpose |
| --- | --- | --- |
| `nextcloud_source_artifacts` | tenant, KB, datasource, external ID, knowledge ID, publication generation, artifact kind, backend kind and **instance ID**, collection/bucket identity, exact object ID and provider version if available, write intent ID, created time | One row per possible physical or derived write, persisted **before** its external side effect. A crash after the write leaves a discoverable pending intent. Never reuse an ID for a new generation. |
| `nextcloud_source_artifact_refs` | artifact ID, owner type/ID, tenant, generation, relation, creation and release times | Protect a shared image or attachment while any legitimate owner exists. Add/release/GC claim serialize on the artifact row; a nontransactional `COUNT(*) = 0` is insufficient. |
| `nextcloud_source_backend_history` | tenant, KB, backend instance ID, provider, collection identity, first/last write watermarks, manifest revision | Retain every vector, graph and storage destination used by the source, including after a KB setting changes. The current KB setting never substitutes for this history. |
| `nextcloud_source_cleanup_runs/items` | exact withdrawal operation, immutable inventory generation, snapshot watermark, artifact ID, kind, not-before, state, attempt, lease token/expiry, provider delete receipt, later absence receipt | Persist the whole deletion plan before deleting business rows. Each item moves independently and idempotently through claimed, deletion requested, absence verified and collected. |
| `nextcloud_source_cleanup_blockers` | operation, typed code, scope, first/last observed time, resolution evidence ID | Durable fail-closed reasons, including unknown history, unsupported provider, malformed metadata, active leases, missing ownership or an incomplete restore ledger. |

Store provider locators and credentials only in private server-side tables. A
public status returns counts, state and typed blocker codes, never object paths
or secret error bodies. Protect resource accounting from double decrement: a
delete receipt is keyed by `(operation, artifact ID, backend instance ID,
provider version)` and confirmed bytes are credited once, only when the
backend can actually confirm release. Snapshot/versioned backend capacity is
reported separately.

For **new** writes, create a prepared artifact intent with the final stable
provider ID while holding the source/KB and generation fence, then perform the
external write under a renewable build lease. Mark the intent committed only
after the provider responds and verify the generation again before publishing.
A retry must reuse the same external ID or inventory both IDs. A task that
lost its lease may not create a new object or publish. This applies to parse,
summary, question, multimodal, graph, Wiki, clone/reparse and delayed task
retries, including legacy `Attempt=0` jobs. The content-lease schema alone is
inert until every such path and every read path is wired and old workers and
streams are drained.

## Provider cleanup contract

Use a typed adapter for each concrete backend **instance**, not a generic
`DeleteKnowledge` call or a path prefix:

1. Resolve the persisted instance and collection/bucket; reject any mismatch
   with the source tuple and artifact ID. A changed backend configuration is
   a blocker until the historical instance can be reached.
2. Enumerate exact owned IDs with a provider cursor or stable snapshot, and
   reconcile the result with artifact intents and current SQL rows. A provider
   that cannot enumerate or verify absence stays unsupported. A whole
   collection delete is permitted only when its durable ownership record proves
   that collection was dedicated to this source for its entire lifetime.
3. `DeleteExact` uses the pinned object ID and provider version/ETag where
   supported, with an idempotency key. A timeout has an unknown outcome and
   triggers `VerifyAbsent`, never an assumed success. For eventually consistent
   services, verify again after the provider's documented settling interval.
4. Save a redacted provider request/receipt digest, verified absence time and
   backend identity. A missing object can complete an item with zero newly
   credited bytes only after matching the exact identity and proving no current
   owner. Errors and unsupported operations leave the item blocked.

Implement and test adapters separately for local PostgreSQL embeddings and
chunks, every enabled remote vector store (for example Qdrant, Milvus,
Weaviate, Tencent VectorDB, Doris, Elasticsearch/OpenSearch), graph namespaces,
Wiki pages/references, local and cloud objects, and generated/attached copies.
An adapter's mere availability does not certify a historical backend was
unused. Historical chat and agent answers also need the §7.7 authorization
barrier; persisted source-derived attachments or exports require their own
owner/ref inventory or a blocker. Do not promise removal of bytes in backups:
the deletion ledger and restore checkpoint must keep restored content hidden
until replay and reconciliation finish.

## Completion transaction and ACK

The existing withdrawal tombstone remains immutable. A new cleanup run may
progress only while the exact Nextcloud decommission intent is still stopped
and its source tuple, publication epoch and key match. Lock the KB/source and
cleanup run in a fixed order. Close build acquisition, cancel queued tasks,
wait for bounded read/build leases, and require a recorded rollout watermark
proving pre-lease workers have drained. Withdrawal does not extend any user's
read permission. The PostgreSQL `000125` write guard must permit **only** an
exact cleanup item in a claimed run to mutate its pinned rows under a dedicated
database procedure/role and the same KB lock; it must continue rejecting
ordinary SQL writes and moved rows on both old and new scopes. Do not use a
caller-set session flag as a general bypass.

For legacy history, a current empty query is never proof. Require an immutable
backend history plus provider-side scoped enumeration reaching a stable
watermark, or a dedicated backend namespace whose complete lifetime ownership
and deletion are proven. Old hard-deleted rows, unrecorded backend changes,
unattributed resources, unenumerable shared collections, and unknown external
writes remain `unknown_history` blockers. A human note or two empty SQL scans
cannot clear them. An operator may quarantine and replace a dedicated backend
instance, but the run needs the backend's exact destruction/absence receipt and
a restore-ledger checkpoint. Out-of-band provider write credentials must also
be revoked or proven scoped away from this source before a final scan can be
trusted. If this cannot be established, leave the source
withdrawn and credentials retained; no indexed ACK exists for that history.

This first indexed ACK deliberately has a stricter threshold than an inventory
receipt: all source-exclusive copies must have verified deletion receipts, and
legitimately retained shared/history references must have owner and revocation
proof. This avoids retiring the only recovery credential while unsupported
external copies remain. A later two-stage ACK protocol would need a separately
specified, durable responsibility transfer.

Only after all item receipts, shared-reference checks, graph/Wiki scans,
provider absence checks, lease coverage, two separated orphan scans and
restore-ledger checks pass may the run become `ready_to_ack`. Freeze its
canonical inventory generation. Compute `SHA-256` over a versioned,
length-prefixed UTF-8 record stream sorted by `(kind, backend instance ID,
artifact ID, provider version)`; include operation/pair/source tuple,
publication epoch, snapshot and coverage watermarks, every item state and
receipt digest, and the blocker set (which must be empty). The record format
and digest must be independently testable and never depend on map iteration or
locale. Later discoveries invalidate the run and demand a new generation.

The final predicate is the conjunction of: exact stopped source identity;
withdrawal and write fence active; no live or unaccounted legacy workers or
readers; every historical backend proven enumerable or destroyed; all
source-exclusive items `verified_deleted`; every retained shared item backed
by a live owner and revoked source access; zero blockers; two matching orphan
scans; and a restore-ledger replay checkpoint. A zero row count satisfies none
of these predicates by itself.

The WeKnora sender durably records this digest and an exact signed outbound
ACK before network delivery. Add a separate indexed ACK handler to Nextcloud:
verify the machine HMAC, exact stopped intent, pair/source tuple, epoch,
inventory generation/digest and no previous conflicting ACK. Record the ACK
idempotently in `weknora_src_decom`; do not relax its empty-source hash check.
WeKnora marks remote acknowledgement only after reading the exact committed
response. Nextcloud then finalizes the binding/key removal by its existing
transactional mechanism; uncertain HTTP outcomes retry the same operation and
digest. Immutable audit/tombstone and restore ledger outlive key retirement.

## Required acceptance before any ACK route is exposed

- Build V2 fails while V1 remains hidden; V2/V3 out-of-order completions and
  late legacy tasks cannot write or republish after the withdrawal fence.
- Concurrent read/build lease versus GC claim is tested with two PostgreSQL
  connections; revocation stops output even while a lease is still valid.
- Failure is injected before and after every provider deletion and receipt
  commit. Retries neither lose objects nor double-credit released bytes.
- Two generations sharing one image preserve the remaining owner; concurrent
  new binding versus delete claim cannot create a dangling reference.
- The backend changes after a previous generation was indexed, a knowledge
  row was hard-deleted, a cloud object is versioned, or an external store is
  unavailable: every case blocks ACK until independently reconciled.
- Graph/Wiki/history access after withdrawal denies source content; backup
  restoration before the deletion event stays closed until ledger replay.
- Indexed ACK with an empty-source digest, mismatched tuple/epoch, stale
  generation, unresolved blocker, changed digest or lost HTTP response is
  rejected or retried idempotently as appropriate. Empty-source ACK remains
  limited to the old no-history protocol.

Until these tests pass in disposable PostgreSQL and all configured provider
fixtures, report `logical_withdrawn: true`, `inventory_complete: false` and
leave the Nextcloud decommission operation in `prepared`.
