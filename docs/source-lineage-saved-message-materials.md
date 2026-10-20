# Saved message original materials: core producer/read closure

Parent: a9d31fb4e2f8164cbf382f9b1ec5c26c6c8f3a20. This module is a core checkpoint, not a claim that every history consumer is covered.

## Actual producer and receipts

Production DI configures the actual message repository/service after construction. A new user message and an empty assistant placeholder create `message_generation_origins`. Only the same actual producer, current owner and actor may advance its previous immutable material receipt. A foreign partial write or body/control ABA cannot be certified by loading the row and calling Update. Completed source envelopes retain the original previous sources. Initial empty code placeholders are the only completion transition with zero previously consumed body.

151 (PostgreSQL) / 70 (SQLite) create generation origins, immutable material admissions, current receipt pointers and monotonic body, owner and membership counters. Legacy rows receive counters, no receipts. `message_generation_origins.initial_input` is original JSON body; receipt digests are not retained body copies. Current messages are loaded only after metadata plans and durable read leases are checked in the same SQL transaction.

The raw author projection has an explicit own producer field list: IDs, role, request, authored Content, completion, creation and execution selectors. Generated rendered context, captions, attachments, memories and checkpoints are not loaded or emitted by that projection. Updating these unconsumed user fields does not certify them or invalidate the raw author receipt. Assistant material has no such body projection.

Create receipts use the actual persisted SQL row, including PostgreSQL timestamp normalization. Update validates the last actual receipt before its atomic write and seals the persisted result. Current user/key/member/role/group/share/Agent authority runs through transaction-bound production repositories; source HTTP checks remain outside SQL transactions. SQLite production's single connection is unchanged.

## Read and output paths implemented

| Actual path | Core behavior |
| --- | --- |
| Message Create/Update | Exact actual arguments, current owner/actor, immutable generation and prior material continuity |
| GetMessage / paginated / recent / before-time | Typed original capture; user raw field projection; legacy unavailable |
| HTTP LoadMessages | Holds actual scopes through serialization, final original-source/actor recheck and releases leases |
| IndexMessageToKB | Ignores caller Q/A strings; captures original assistant plus unique original user; current writable destination only |
| Actual queued ProcessDocument | Restores original current actor, acquires actual history parent scopes and holds them through producer/model/chunk writes |
| Ordinary KnowledgeOutput / KnowledgeCollection | Metadata read sets expand immutable history parents and bind all message plans before body capture |
| FAQ entry/read/export and FAQ progress/LastResult | Original knowledge parent plans expand saved history parent/source scopes; existing HTTP lease helper acquires them |

Read plans contain receipt/control metadata, no message body. Multiple session plans are copied into a private registry. The body transaction validates the exact original plans and actual durable lease rows (owner, scope, epoch, kind, expiry and fences). A changed candidate/body/owner invalidates an old plan. Source access is checked independently; an open physical lease never grants permission.

## Retention interface

`ConfigureMessageOriginalBodyAvailability(store, MessageOriginalBodyAvailabilityCheck)` accepts `func(context.Context,*gorm.DB,string,string) error`. It is called with the existing body transaction, table `message_generation_origins` and immutable generation ID before original capture, and again on recheck/producer continuity. Retention owner 152/71 must install the actual vault plus external purge-journal checker. This parent does not yet contain that owner; nil is not a claim of purge coverage. Unavailable originals become SourceLineageUnknown and are never reconstructed from current message contents.

Immutable triggers: PostgreSQL `message_generation_origins_immutable`, `message_material_admissions_immutable`, function `message_material_immutable`; SQLite each table has `_immutable_update` and `_immutable_delete`.

## Actual acceptance

SQLite pool1 with bounded deadlines: actual service Create user → Create assistant placeholder → Update completion → actual IndexMessageToKB → queued ProcessDocument → chunks/enabled knowledge → original output capture/SourceGuard. HTTP ordinary and signed Nextcloud-derived saved-message positives; personal source revoke while exact physical read lease remains open rejects output. Body, execution, membership and owner ABA, same-role different caller, current user/member/key/group/organization share revokes reject. Previous actor integration fixtures now use actual producers and plans. Old repository raw completed-row “positives” now assert that a global verifier/current JSON cannot mint receipts.

PostgreSQL fresh 151, pool1 and bounded deadline: actual producer/read → new ordinary destination → queued chunks → two actual scope leases → KnowledgeOutput capture/SourceGuard → parent body ABA rejects. Tests use deterministic local callbacks and owned PostgreSQL, no paid models.

## Remaining coverage: continue work

- Agent `LoadAgentHistory` directly reads checkpoint/reverse pages via the repository. It still needs exact candidate/selection control receipts and lease lifetime through the actual Agent model loop.
- RAG history adapters need leases through asynchronous model and final HTTP emission; synchronous service reads without an output boundary currently close their guard before the caller consumes the returned text. This is not complete model lifetime coverage.
- `GetMessageForStream` / ContinueStream currently read saved bodies via the direct repository and replay cached Redis frames. They need original completed-frame/material receipts or authoritative reconstruction, plus actual held source/parent leases. Current source envelopes alone do not authenticate cached frame text.
- Session clone/fork/rewind/steer/import/export and IM/MCP/skill callers require their actual original candidate/control and body consumption audit. Same trusted service Create/Update is only the producer core.
- Direct partial image/rendered/checkpoint/artifact writes are not new generated-field origins. Assistant partial writes invalidate the whole prior material; raw user generated fields remain unknown and omitted. Typed actual field producers must be added for supported consumption.
- Message search/reconstructed pairs, artifact download/list, memory extraction's direct cursor readers and transcript consumers are not claimed covered by this checkpoint. Memory text is never certified by current message metadata.
- Actual cross-tenant Agent-share histories and all native saved/reconnect/continue/model endpoints still need positive/revoke/ABA tests. Existing actual organization sharing and key/group gates cover the core only.
