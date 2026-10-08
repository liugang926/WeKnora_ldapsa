# Nextcloud version GC safety boundary

The hourly Nextcloud GC runner writes a durable inventory before attempting
cleanup. PostgreSQL migration `000119` and SQLite migration `000038` add
`nextcloud_gc_jobs` and `nextcloud_gc_items`; forward migrations `000121`
and `000040` add the durable object-deletion claim. Forward migrations `000122`
and `000041` allow exact local derived-row IDs in the same durable item list.
Each job is unique by knowledge
ID. It records the retired source identity, why the version retired, the
earliest derived cleanup time, the 7-day original-copy expiry, a retry cursor,
and machine-readable failure status. Items preserve exact source-file and
extracted-image references before the knowledge row or its chunks are changed.

The default derived safety window is one hour. A terminal failed build uses
24 hours. A confirmed source tombstone may be inventoried immediately. The
original-copy timestamp is seven days for ordinary retirement and immediate
for a confirmed tombstone. These timestamps are eligibility limits, not proof
that storage was freed.

`GET /api/v1/datasource/nextcloud-gc-jobs` lists tenant-scoped job status for
an administrator session. `POST /api/v1/datasource/nextcloud-gc-jobs/:id/retry`
advances a blocked job to the next retry; it cannot bypass a safety window.
The response omits object references, backend paths and document content.
`estimated_bytes` is the requested original's recorded storage size;
`confirmed_released_bytes` remains zero until a storage backend confirms
deletion. A blocked job must not be interpreted as reclaimed capacity.

Automatic physical deletion is limited to registered Nextcloud resources on
an explicit `storage://<backend>/local://<tenant>/...` local backend. An
ordinary replaced source file can be released at or after its 7-day original
deadline; a confirmed tombstone sets that deadline to now. An
extracted image can be released after the one-hour derived window. A terminal
failed build waits 24 hours. A resource shared with another bound owner stays
on disk and credits zero bytes to this job. A sole-owner resource is locked,
made `deleting`, and then removed by its exact provider path. Every new bind
locks the same row and refuses `deleting`, so it cannot race the zero-owner
decision. A persisted item lease prevents concurrent workers from deleting
the same object. A provider failure leaves the resource `deleting` and the
item retryable, with zero confirmed bytes. An already absent local file can
complete a previously claimed `deleting` item after a crash, but credits
zero bytes because this attempt cannot prove when the unlink happened. Only
a successful local unlink in the current attempt credits the recorded
resource size once. Filesystem snapshots or an open file descriptor can
still retain blocks, so volume usage must be measured separately.

Every retired Nextcloud knowledge job keeps a `derived_index` blocker, even
when the current row has no chunks, embedding model or local index rows.
Historical graph, Wiki or external index output could still exist, and the
current row is not proof that those artifacts were never created. A migration
reopens previously `collected` jobs and adds this blocker so old false-complete
statuses are not carried forward. A job can remain blocked after its local
source file was successfully released; the file item retains its separately
confirmed byte count. The knowledge metadata row remains for history policy.
For a retired row, inventory stores each exact chunk ID and each observed
PostgreSQL `embeddings.id` or SQLite `lite_embeddings.id`, scoped to the
knowledge base and knowledge ID. A retry adds rows written after an earlier
scan before it can consider the job complete. These rows are an audit and
recovery prerequisite: the current worker does not delete them or claim
their storage. In particular, a knowledge row with no chunks but a lingering
local embedding cannot be marked `collected` merely because its model ID is
empty. The knowledge-level `derived_index` item remains pending as the
conservative barrier for external index backends and graph/wiki output.
Invalid image JSON is recorded as `invalid_image_inventory`; a later retry
rebuilds the image item list after the data is repaired. A row that becomes
the current candidate is blocked as `candidate_is_current`. The publication
and access guards continue to deny a retired or tombstoned row independently
of GC.

The existing generic `DeleteKnowledgeList` cannot yet execute these jobs:
it drops knowledge rows before deleting files, only logs file/image deletion
errors, adjusts storage usage even if those deletes fail, and releases a
resource binding separately from the later physical deletion. Its
`Release`/count sequence cannot prevent another owner from binding between
the count and delete. Calling it from GC would risk lost objects and a false
"collected" state.

The minimum protocol still needed for derived indexes and cloud storage is:

1. Persist the index backend and collection identity at build time, including
   every backend used by composite retrieval. Record or cancel build and read
   leases. Historical knowledge rows currently have no such provenance;
   the KB's current vector-store setting cannot prove where an older version
   was indexed. Graph and Wiki references need explicit acknowledgements.
2. Under a transaction, lock the source version, job, knowledge row and each
   stored resource. Recheck that the version is retired, the access barrier is
   active, no build/read lease exists, the original-copy deadline has passed,
   and the persisted object identity still matches the row and backend.
3. Add backend-specific acknowledgement for object stores. `DeleteObject`
   alone may create a delete marker while previous versions still consume
   capacity. Record logical unlink and confirmed physical capacity separately.
4. Delete vectors, graph, chunks, wiki references and knowledge metadata in
   separate idempotent phases, each with a durable completion marker. Only
   mark a job `collected` after all items and phases are complete.

The remaining code touch points are the vector/graph/chunk delete adapters,
wiki references, provider-specific storage adapters, the knowledge delete
plan, and this GC store. The current implementation does not yet reclaim
cloud-backed source files or derived indexes, enforce the one-old-original
cap, purge 90-day history metadata, or run a two-pass orphan scan.
