# Automatic classification completion durability — PostgreSQL 153 / SQLite 72

Code freeze: `7bbe13ce592de303de23882f0cf8f54244df535b`. Base: `be3fea862fc0d40536b2c69eff71521196bc8810`, plus the root fixture-only correction `e0b33efe6eb89321060f3e1a401afc1499117b4b` (local cherry-pick `adb510d0`). This independent worktree does not edit the root candidate.

## Persistent handoff

The completion winner seals the final actual document/chunks, all candidate/relationship/name controls, model/tenant digest and revisions, exact queue receipt, and the `task_pending_ops` intent in the same SQL transaction. A failed producer insert rolls back completion and the last subtask decrement. Summary/QA finalizer errors reach their actual task return so delivery can retry that handoff. Auto owns no parsing subtask slot.

Redis and Lite queueing are wakeups. Startup and periodic recovery replay the stored payload, never capture a current body or manufacture an old original receipt. Pending rows remain durable across enqueue/network failure; stale SQL consumer claims can be reclaimed. A claim timestamp fences both tag relation publication and finalization. The tag relations, their derived original receipt and successful intent terminal state commit together. Duplicate terminal delivery does not repeat the model or tags. Definitive current source/actor/model failures retain a rejected intent; restored availability cannot revive that rejected task. Transport/server failures retain pending work. No source tombstone or physical deletion is inferred from a classification task failure.

The full classifier snapshot digest remains intact. PostgreSQL 153 / SQLite 72 add a separate publication revision counter and append-only exact transitions. Only actual source-checked Publish/Unpublish SQL writes can append same-admission before/after digests and revisions. A contiguous legitimate publication/hide/republication chain lets the original classifier consume its immutable inputs; an external ETag, timestamp or metadata ABA creates a revision gap and is rejected. Missing old transitions are unknown. Completed documents no longer receive processing heartbeat writes.

## Actual verification

Final `r12`: **156 pass events / 0 fail / 0 skip**, including 152 test/subtest events and four package passes; actual terminal exit **0**. Packages: repository, service, container and signed Nextcloud connector. The container package compiles here; the parent owns whole-app startup acceptance.

The new durability cases execute production `ProcessSync`, signed original transport, actual KnowledgeService/storage admission, parser/chunk SQL, actual Summary/QA handlers, completion SQL, actual Auto handler/tag SQL, real publication and guarded reads. Their SQLite and PostgreSQL fixtures use a connection pool of one. Models and the existing retrieval backend are isolated test implementations; real Composite backend selection runs. This is Markdown pipeline coverage, not native vector or real enterprise directory acceptance.

Covered boundaries:

- Completion enqueue failure, completion before queue wakeup, and a real subprocess SQL claim followed by `os.Exit(73)`, restart and redelivery. The subprocess commits the actual claim; only the three-minute recovery clock is advanced by SQL.
- Actual Asynq TCP enqueue failure, persisted SQL intent, recovery into isolated Redis, exact stored payload inspection, and production handler execution under a real exact build lease. These package tests do not run a background Asynq server.
- Actual pending table outage at Summary/QA fan-in, complete rollback of completion/queue/model receipts, and original task redelivery.
- Zero Summary/QA subtasks through the real signed sync/file service with explicit original upload process controls; completion and Auto recovery succeed.
- A replaced consumer claim cannot persist tags or overwrite its successor's claim; its tag/producer writes roll back. Successful replay attaches one relation.
- Actual publication HTTP503 before commit and after commit, with real SQL hide and publication retries. The immutable receipt chain remains contiguous.
- Current source and actor withdrawal; model, tenant, body, publication ETag/timestamp/metadata ABA; forged queue controls. The new pre-delivery rejection cases make zero classification model calls and add no candidate relation. Existing post-model withdrawal regressions may make the model call but cannot publish tags.
- Existing actual machine Summary/QA-to-Auto/publish positives, candidate and inherited public-source authorization, ordinary actor/API key negatives, model/tenant credential controls, spans, and SQL relation scope/idempotency regressions.

Every run used 1 CPU / 2 GiB, a read-only source mount, an internal owned network and no host ports. Existing module/build caches are retained. The adjacent JSON records stdout SHA-256, actual terminal exits, failed/skipped test names and cleanup metadata without copying body/credential-bearing stdout. Historical failures are retained: r2 publication self-invalidation; r5 missing span publication transition; r6 incorrect fresh-producer fixture lease plus absent old PG fixture DSN; r8–r11 completed-row heartbeat self-invalidation. r3's broad bookkeeping normalization was replaced by exact SQL transition receipts and is not final boundary evidence.

## Root integration and cleanup

Cherry-pick the code freeze plus this evidence commit. Preserve the existing same-transaction actor/group/share factory and source verifier wiring. Container startup already installs the new durable recovery after task handlers are registered. Apply PostgreSQL **153** / SQLite **72**; SQLite **71** belongs to the independent body-retention migration **152** and must be present in the combined root tree. No ordinals 150/151/152 were reused.

Before cleanup the owned PostgreSQL query counted **zero non-system schemas**. Exact owned PG/Redis container IDs and the internal network ID are in the JSON. `docker stop` removed both AutoRemove containers; `docker network rm` removed that exact network. Final owner-filtered container/network inventories are empty. `weknora-go-mod` and `weknora-go-build` remain. Shared services were not modified.

Whole local application Docker acceptance and GitHub submission remain with the parent task. No paid model, real enterprise AD or deployed Nextcloud server was used by this independent closure.
