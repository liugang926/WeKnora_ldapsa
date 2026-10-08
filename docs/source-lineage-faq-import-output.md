# FAQ import output provenance

## Actual producer graph

The repository-wide Go caller audit for `CreateFAQEntry`, `UpsertFAQEntries`,
`ProcessFAQImport`, and `TypeFAQImport` finds these production paths:

| Boundary | Actual next step |
| --- | --- |
| `internal/handler/faq.go` CreateEntry | `knowledgeService.CreateFAQEntry` |
| `internal/handler/faq.go` UpsertEntries | `knowledgeService.UpsertFAQEntries` |
| UpsertFAQEntries | Admit exact original batch; enqueue `TypeFAQImport` |
| `internal/router/task.go` and `sync_task.go` | Dispatch to `ProcessFAQImport` |
| `client/faq.go` | SDK calls the HTTP create/import endpoints |

There is no production document-to-FAQ AI caller or GenerateFAQ endpoint.
Document `question_generation` is separate: `knowledge_post_process.go` queues
`TypeQuestionGeneration`, whose worker updates document chunk questions. It
does not create FAQ entries. This change therefore adds no new AI feature.

The private `WithDerivedFAQInputs` bridge requires complete original material
proofs for every consumed parent. Future generators must retain text, prompt,
template/configuration, selection/control, history, and unseen candidate inputs.
It unions all source identities, rejects a projected/unknown parent, and checks
each original receipt again. A source list or current metadata cannot substitute
for an original material proof. A history parent stays unknown in a FAQ SQL
transaction until a transaction-bound message publication verifier is available;
the current global message verifier must not obtain a second SQLite connection.

## Actual output receipts

The authenticated original import session owns progress/result publication.
Every progress write seals a detached immutable JSON body before memory/Redis
storage. A LastFAQImportResult and its immutable output receipt commit in the
same SQL transaction. Reads use the immutable producer body and exact current
head; Redis or current knowledge metadata cannot authenticate legacy output.

Result material revisions increase on actual LastFAQImportResult changes and
detect write-and-restore ABA. Output heads are monotonic. A display edit can
change only display status and must retain the old result's original source,
parent proofs and counters; missing legacy receipts remain unknown. The matching
task progress receives a new receipt in the same transaction.

FAQ progress HTTP responses, ordinary knowledge collection LastResult fields,
and knowledge output guards independently check the result producer's current
source, actor, API key capabilities/scopes, KB read permission, and original
parent material. The existing serialized collection writer is used for FAQ
progress. A fixed FAQ catalog header does not certify an import result.

## Read plans and transaction connection

`FAQOutputAuthorizer.ProgressReadPlan` and `ReadPlan` read only immutable
source/parent proof metadata, receipt IDs, membership and monotonic revisions.
They return all target, parent and source KB scopes. Callers acquire real content
read leases for every scope before binding the plan and reading bodies. The
capture transaction verifies the exact metadata again before reading config,
tags, entry body, or parent chunks. A normal all-entry plan can cover a single
entry read; a stale membership/head/receipt plan is rejected.

The context plan does not itself prove lease acquisition. Root HTTP lease wiring
owns that step. This module does not claim full GC reader coverage.

FAQ parent captures use production `displayAuthorityFactory(tx)` for all current
actor, group and organization checks. The authority store must hold the same SQL
transaction; no global repository fallback is allowed. Actual SQLite tests use
the production single-connection shape and bounded contexts.

## Verification

Actual SQL/HTTP tests cover ordinary import/pending/completed progress, LastResult,
display-only transfer, current account/membership/key/KB policy withdrawal,
missing legacy proof, result/body ABA, metadata plan changes, and an actual
published Nextcloud parent followed by source retirement. That controlled test
uses MaxOpenConns(1), the real producer/worker, HTTP list/progress/tag statistics,
and all-scope metadata plans. PostgreSQL runs the actual import producer, progress,
result, display transfer, account withdrawal and result ABA against freshly
created migrations in an owned temporary schema.

The existing private PG 144 / SQLite 63 migration is extended. No new migration
number is allocated. Existing unproven rows receive counters but no receipts.
