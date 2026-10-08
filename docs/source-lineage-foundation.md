# Source lineage foundation

This additive foundation provides storage/types, not complete prompt capture or
message authorization. Existing producers do not assign complete lineage and
existing read/SSE/history-index paths are not switched by this change. It does
not close the history paraphrase, compaction or derived GC requirements.

PostgreSQL migration 132 adds nullable `messages.source_lineage` JSONB; SQLite
migration 51 adds nullable TEXT. Existing answers retain SQL NULL. The internal
Message field is excluded from public JSON. ContextCheckpoint has a separate
persistence envelope, also excluded through Message's existing `json:"-"`
checkpoint field. Never serialize a checkpoint directly as a public response.

Version 1 records `complete`/`unknown` plus controlled source identities,
including effective source tenant, dedicated KB/data source, immutable pair,
instance/binding/file, original knowledge and exact build-fence epoch, and opaque
ETag. Constructors/codec/union reject duplicate JSON keys and invalid raw UTF-8,
validate identities, retain different revisions,
propagate unknown, produce deterministic independent sets and fail closed at
1024 identities or 256 KiB. A complete empty set is an explicit trusted
producer assertion. NULL, missing/malformed fields and unsupported versions do
not become complete. The schema checks the envelope; the typed codec also
validates every identity. Neither is an authorization grant.

`nextcloud_source_tombstones` stores independent positive ever-source facts for
tenant, KB, data source and pair scopes. It has no business-row foreign key.
Backfill includes retained/deleted business rows and retained local revision,
pairing/rotation/abort/withdrawal, event-connection and GC evidence. A retained
pair-rebuild ID without an owner is not assigned to a guessed tenant. New
evidence appends facts transactionally; business deletion cannot remove them.
Update/delete and PostgreSQL TRUNCATE are rejected. SQLite REPLACE retains the
first conflicting fact even when recursive triggers are disabled. Automatic
down migration deliberately fails rather than discarding provenance.

`LookupNextcloudSourcePresence` is an unused foundation API: matching evidence
returns `ever`; absence returns `unknown`; failed storage returns `unknown`
with an error. It cannot prove `never` or recertify old messages. Backup restore
must still replay/verify the separate withdrawal ledger before reopening reads.

The next implementation phase must capture every actual influencing input and
emitted source result in RAG, Agent tools and mode switches, inherit history,
propagate summaries/forks, atomically save answer and sealed lineage, and apply
mandatory authorization before model input, output, replay and derivatives.
Until then, source-dependent indexing and physical derived GC remain subject
to their existing safety gates; this foundation provides no coverage marker.

The next source-only step supplies an unused
[trusted resolver/authorizer](source-lineage-policy.md). Its build epoch is not
the append-only observation sequence; same-content bookkeeping does not alter
the generation. Complete producer/reader wiring remains required.
