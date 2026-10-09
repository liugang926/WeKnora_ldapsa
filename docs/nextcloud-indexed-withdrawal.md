# Indexed Nextcloud source withdrawal: first stage

An indexed source cannot use the empty-source decommission ACK. Current source
versions retain only the latest candidate, while old external vector backends,
graph and Wiki outputs, hard-deleted knowledge, and in-flight external writes
have no complete durable lineage. An empty current query is therefore not an
empty historical inventory.

After the Nextcloud administrator has persisted a **stopped** decommission
intent for an exact binding and operation UUID, the WeKnora administrator can
call `POST /api/v1/datasource/nextcloud-source-pairings/{pair_operation_id}/indexed-withdrawal`
with `{"operation_id":"<same UUID>"}`. WeKnora reads the signed Nextcloud
intent through the paired credential and compares the entire source tuple.
The PostgreSQL repository then locks the dedicated knowledge base, checks the
pair and credential, refuses a live key rotation or in-flight object deletion,
requires running source syncs to have finished,
pauses the source, clears publication ETags and disables its local knowledge,
revokes its active event connection and blocks its dispatch cursor, and inserts
an immutable withdrawal row in the same transaction. An in-flight event
receipt that holds the connection causes a retryable withdrawal conflict;
no partial pause is committed. After withdrawal, a signed batch cannot
advance the receipt watermark. PostgreSQL triggers reject subsequent SQL
writes to that source's knowledge, chunks, local embeddings, source versions,
event connection and new sync log. They permit only a strict active-to-revoked
event credential transition so emergency administrator revocation remains
possible for an older inconsistent row; they reject any key, identity, or
source change in that transition. The GC object claim takes the
same KB lock and stops new physical deletions after withdrawal.
The write trigger checks both old and new source scopes on an update, so a
copy cannot be moved out of or into a withdrawn KB to evade the fence.

The response is HTTP 202 with `logical_withdrawn: true` and
`inventory_complete: false`. It does **not** send a Nextcloud ACK. The
Nextcloud binding stays stopped, the source key remains available for recovery,
and the WeKnora pair and source remain as paused historical identities.
Repeated POST calls retry the local inventory without creating a new
withdrawal. `GET .../indexed-withdrawal` reports only durable state and the
number of observed items, never paths or document IDs. A different operation
or identity conflicts with the first operation.

The private, append-only SQL inventory records exact observed identities for
knowledge rows (including soft-deleted rows), source files, extracted images,
chunks, PostgreSQL embedding rows, source versions, sync logs, and existing GC
jobs/items. It also records every row of the append-only source revision ledger,
including a candidate identity whose knowledge row was later hard-deleted.
It scans the whole dedicated KB for copies with a missing live
knowledge row, and keeps an original item even if later processing changes
state. Malformed image metadata and an absent PostgreSQL embeddings table
produce explicit blocker items. An external-index blocker is always present.
Refresh errors leave the withdrawal in force and require a retry.
The item count does not certify completeness: a revision records a local source
mutation, not every copy made from it, and pre-ledger history remains unknown.
The migration does not assign a
historical index backend to an old version, discover an external copy, cancel
an already running external write, or confirm physical storage release.

This first-stage admission is PostgreSQL-only. SQLite migrates the tombstone
schema for forward compatibility, but rejects indexed withdrawal because its
runtime-created local embedding table lacks an installation-time write fence.
Even on PostgreSQL, direct SQL/vector access outside the guarded application
is outside this protocol. A future completion phase needs persisted backend
identities, build/read lease draining, provider-specific deletion receipts,
historical object reconciliation and a signed inventory digest before it may
ACK Nextcloud or release credentials.
