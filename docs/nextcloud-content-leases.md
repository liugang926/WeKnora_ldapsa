# Nextcloud content lease foundation

This repository layer supplies a durable barrier between Nextcloud content
readers/builders and future exact-generation garbage collection. It does not
enable physical deletion. The coverage marker is absent after migration, so
`ClaimKnowledgeGC` returns `ErrNextcloudContentGCUncovered` by default.

## Protocol

`nextcloud_content_fences` has an immutable `(tenant_id,
knowledge_base_id, knowledge_id)` key. The `knowledge_id=''` row fences a
whole KB. An exact row also pins the datasource and external ID for the
knowledge generation. `open -> retired -> deleting -> deleted` is monotone;
`deleting -> retired` permits a failed GC attempt to retry without reopening
reads or builds. Retirement advances the epoch. A new document version needs
a new knowledge ID; it must not reopen an old fence.

`nextcloud_content_leases` records unguessable lease ID, scope, `read` or
`build`, epoch, owner, database-clock expiry, and release time. No content or
credential is stored. Every acquire, renewal, retirement, and GC claim locks
the KB row first, then the exact row. The supported TTL is at most five
minutes; streams and long tasks need renewal and must stop if renewal fails.

A KB read lease protects a search before document IDs are known. A caller
then calls `UpgradeKBRead` for each result before hydrating it. The broad
lease remains held until all exact leases cover the response, or until the
response ends. Retiring a document between search and upgrade rejects that
upgrade. Existing leases still block GC until release or expiry, but they do
not grant access after publication or authorization is withdrawn.
An active exact GC claim temporarily blocks new KB-wide searches so a search
cannot overlap index deletion. A released or expired claim stops blocking
new searches; an expired claimant cannot validate its token, and a new claim
must wait for any intervening KB read lease.

Builders acquire an exact build lease at worker start. `ValidateBuildLeaseInTx`
must run in each SQL write transaction before changing chunks, embeddings, or
other derived rows. Nontransactional vector, graph, object, and Wiki writes
need a separate adapter with equivalent fencing. A queued worker may not rely
on an enqueue-time lease: it must acquire after dequeue. A failed heartbeat
requires cancellation.

`RetireKnowledgeInTx` and `RetireKBInTx` are provided so publication/source
retirement can close lease admission atomically. `ClaimKnowledgeGC` requires
the exact fence to be retired, the caller's `notBefore` to be no earlier than
the persisted retirement time, no unexpired exact
read/build or KB read lease, and a per-KB `nextcloud_content_lease_coverage`
row. `ValidateGCClaimInTx` must run in the same transaction as a future exact
SQL delete and receipt. Claims are token and epoch fenced; a stale claim
cannot finalize a newer attempt.

The caller must load `notBefore` from the persisted GC job and verify its
source tuple, state, and policy-specific delay. This repository layer does
not independently enforce the PRD's one-hour or longer windows; passing an
arbitrary time after retirement is insufficient for physical deletion.

## Activation gate

Migration creates no coverage rows. A future rollout must cover every
RAG/Agent/MCP/search/direct download/preview/history/FAQ/Wiki/graph read and
every parse/fanout/retry/write path; then drain or cancel pre-rollout streams
and queued/running workers. Only after that proof may deployment insert a
coverage row with its activation and legacy-drain database times and explicit
reader/builder revisions. An empty lease table alone is not proof. Unknown
legacy history stays blocked for manual review.

The repository does not make authorization decisions. Callers still need the
live Nextcloud publication guard before and during source exposure, and the
source version must be retired before physical deletion.

## Partial HTTP reader integration

The direct HTTP handlers for knowledge detail, span detail, knowledge batch,
chunk detail, chunk list, chunk revision list, file preview, single-file
download, and batch ZIP download now acquire exact read leases after their
existing KB and live publication checks. They renew during long requests,
release after the synchronous response finishes, and reject lease failures.
Publication is checked again after acquisition and before the first output.
File reads check it after each blocking `Read`, and a response writer checks
it before each body `Write` or `WriteString`. A ZIP is built under the leases
and all included documents are checked again for every response write.

The HTTP integration does not yet cover `ListKnowledge`,
`ListKnowledgeFolders`, route-guard preflight reads, KB-wide search/RAG,
Agent, MCP, FAQ, Wiki, graph, or background builders. No coverage
marker is inserted, and physical derived deletion remains blocked. Completed
lease rows are retained for one hour, then pruned by database time in batches
of at most 1,000 rows using release/expiry indexes; active rows are preserved.

The response writer rechecks Nextcloud publication while sending bytes, but
does not recheck WeKnora KB, organization, shared-agent or group grants after
their initial route authorization. Thus it is not the full stream revocation
barrier. The current per-read and per-write remote publication checks also
make large downloads expensive; throughput must be measured before a pilot.

## Partial build-worker integration

Redis/Asynq and Lite task registrations now admit exact Nextcloud build leases
after dequeue for document/manual parsing, multimodal image work, postprocess,
summary, question generation, auto-tagging, graph chunk extraction and data
table summaries. Legacy graph tasks that carry only a chunk ID resolve the
persisted chunk and then acquire the exact knowledge lease. Every worker has a
bounded heartbeat, a canceled context on renewal failure, and a detached
release. A retired generation cannot enter these handlers; a handler that
loses its lease fails its task even if it otherwise returned success.

The build token now fences chunk create/update/save/delete-by-knowledge and
covered knowledge-row updates in their SQL write transaction. Publication
checks the token in its candidate commit transaction. Primary parse checks
the lease before chunk cleanup, writes, indexing and completion. This closes
important late-retry SQL paths but does **not** prove every builder is covered:
external vector/graph/object writes are not token-fenced at their provider
commit, Wiki ingest/finalize and KB profile aggregation are KB-wide workers,
FAQ import and direct user-triggered edits need separate review, and all side
effects of enrichment and source reconciliation need an inventory. There is
no `nextcloud_content_lease_coverage` row and physical derived deletion
remains disabled.

The SQL fences apply only when a build context accompanies the write. Other
callers of `UpdateKnowledgeColumns`, including source replacement, still run
without this worker lease and require a separate source-lifecycle review.
`UpdateKnowledgeBatch` has no production callsite today; its existing
Nextcloud fallback invokes single-row `UpdateKnowledge` repeatedly and is not
an atomic batch. A future caller must not infer all-or-none publication from
that API.
