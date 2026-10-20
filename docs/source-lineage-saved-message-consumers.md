# Saved message consumers: verified primary checkpoint

Parent: `63d92bbf033b676e9a0eb3a4dc2d5f2c033dcfc8`. Includes the exact safe QA error/terminal SSE changes and tests from `05a4aca333e6ccd3011c68d249beea995c662ad7` (parent `e0b33efe`); other root changes are not included.

## Actual paths

- Agent LoadAgentHistory reads only original captured message/checkpoint/attachment material, holds all actual parent/source leases throughout AgentEngine.Execute, checks original controls and current authority before model and generated checkpoint callbacks.
- KnowledgeQA captures the actual current user and selects completed history excluding the actual current request before body capture. Every candidate control is pinned, including excluded candidates. The first real turn with persisted user and empty assistant placeholder reaches the actual stream model and final actual UpdateMessage.
- Actual rendered/image/checkpoint producers append independent field admissions with immutable actual parent receipts, source unions and monotonic field counters. Own field writes can transfer exactly their own prior receipt; unrelated parent/body ABA remains unavailable.
- Completed Continue rebuilds native frames from original saved materials. Cached Redis frame text is not admission. All scopes remain held through the actual final HTTP Write and Flush; current actor/source and body rechecks reject changes at the writer.
- Fork reads the actual original prefix and controls under leases and writes the child session/messages and immutable parent proof manifest atomically. Unknown checkpoint/artifact fields are unavailable; actual source/actor/owner/body ABA denies before the writer.

151/70 fresh DDL includes generated field admissions, pointers, monotonic counters and immutable fork parents. `UpgradeMessageGeneratedFieldSchema` is invoked by production DI for already-installed old 151/70. The forward helper initializes counters only and creates no admissions for old rows. Partial schema is unavailable pending reconciliation.

Read metadata projections for CommittedMessageParentRequest and producer generation lookup omit initial_input. Original source bodies remain subject to the existing availability callback; this checkpoint does not include body-retention 152.

## Acceptance

Actual SQLite pool1 with bounded context and owned PostgreSQL: Agent model/field checkpoint/replay, first QA persisted placeholder, render/image then final completion, native completed HTTP replay, actual original Fork, source/actor/body/owner/control ABA, cancellation and lease expiry. Both FAQ and lease PostgreSQL DSNs are supplied; the two core repository PostgreSQL tests formerly skipped now actually pass. Deterministic local models incur no paid model calls.

## Open consumer scope

Live QA HTTP consumes frames after the model goroutine may close its history guard. The request-local holder still needs to reach the actual HTTP final Write/Flush and transfer only its own immutable output control receipt after final persistence. The current model boundary is not claimed as live HTTP lifetime coverage.

Rewind, steer, memory direct cursor extraction, artifact catalogs/downloads, message search/pair reconstruction, transcript import/export and additional IM/MCP execution lifetimes continue in the next private tree. Actual cross-tenant Agent share revocation and full application history/reconnect acceptance remain required. Unknown legacy materials are not admitted from current bodies or headers.
