# Nextcloud local source revision observations

Migration 126 (PostgreSQL) / 45 (SQLite) preserves each committed mutation of
`nextcloud_source_versions` in `nextcloud_source_revisions`. The source key is
`(tenant_id, knowledge_base_id, datasource_id, external_id)` and `revision` is
a per-key, increasing **local observation sequence**. Each observation stores
the desired ETag, candidate knowledge ID, staging/published/tombstone state,
source-row update time and ledger record time. These fields contain neither
document bodies nor credentials.

The migration copies each existing mutable row once at revision 1 with
`legacy_history_unknown = true`. Earlier candidates, ETags, tombstones and
other copies cannot be reconstructed. The flag remains true on later
observations for that source. Sources first observed after migration start at
revision 1 with the flag false. This only means the local version-row history
is observed from creation; it does **not** prove a complete external copy
inventory.

Database triggers append in the same transaction as each current-row INSERT
or UPDATE, including UPSERT conflict updates. They reject current-row key
changes and deletion, ledger UPDATE/DELETE, and PostgreSQL TRUNCATE. A failed
write rolls back its observation. PostgreSQL row locks and SQLite's single
writer serialize competing writes for a source. State changes may produce
multiple observations with the same ETag. The sequence is not a content hash,
approved publication generation, or proof that every vector, graph, Wiki,
resource or backup copy is known.

PostgreSQL migration 125's indexed withdrawal fence still blocks subsequent
source-version writes. This ledger does not change `inventory_complete=false`,
authorize GC, produce a Nextcloud acknowledgement, or retire a signing key.
It does not implement the PRD's full `document_revision` state machine,
90-day retention policy, recovery-copy policy, or physical cleanup. Both down
migrations intentionally fail closed; automatic rollback would destroy the
only retained local history.
