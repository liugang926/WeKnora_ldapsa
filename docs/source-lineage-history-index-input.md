# Committed history input adapter

This private source increment connects the configured history index to the
atomic knowledge-input capability. It does not complete the PRD or deploy the
producer to the shared test environment.

`IndexMessageToKB` ignores both caller question/answer strings. It resolves the
owned session, reloads the completed accounted assistant, resolves the actual
session tenant's configuration and history KB, checks current target read and
write permission, then supplies only tenant/KB/session/assistant IDs to
`CommittedHistoryKnowledgeInputWriter`. The capability reloads the unique
paired raw user and committed assistant and saves child origin, original parent
facts and message link atomically. There is no generic passage creation or
separate message-link update in the adapter.

Shared Agent execution remains separate from the original caller/session
tenant. A source tenant's configuration or read grant cannot mint an index
write for that tenant. The ordinary caller-owned history KB receives a newly
resolved scoped write grant without changing the original caller.

The concrete request-query writer admits fresh pure raw text as explicit
complete-empty; this is before prompt rendering or rewriting. Images and
attachments keep unknown proof. Generic `MessageService.CreateMessage` does
not infer a proof from a user role, and legacy NULL is not backfilled. Duplicate
steered users, unpaired messages and incomplete parents remain closed under
the atomic writer.

This adapter currently admits source-independent committed answers only.
Controlled history requires the store's mandatory
`MessageSourceLineageBatchVerifier.CheckMessagePublications` producer/read/worker
boundary plus actual combined multi-source revocation evidence before that
gate can be enabled. The batch interface includes original user proof and
preserves each parent's Agent/KB scope. No loop of single-message checks may
replace it. Missing capability, unknown proof or authorization failure skips
index admission; it cannot enqueue caller-supplied material.

Focused tests invoke the actual message adapter with forged caller strings,
target withdrawal, ownership/tenant mismatch, shared execution, unsupported
writer and late writer failure. They also invoke the actual raw request helper
for pure text and opaque inputs. The store's SQL atomicity and worker/hydration
tests are separate implementation evidence, and must also pass in the combined
tree before runtime activation.
