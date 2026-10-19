# RAG actual-material capture

This source-only adapter binds database material to its original source
identity. Runtime, persistence, replay and other producer integration are owned
by the surrounding lineage work; this adapter does not complete those paths or
real AD acceptance.

## Frozen internal capability

`types/interfaces.RAGChunkSnapshotReader` accepts only
`types.RAGChunkReadRequest`: server-resolved tenant/KB/document/chunk IDs, the
original exact read token, and an optional original lineage for augmentation.
`MetadataOnly` returns the actual knowledge descriptor and a nil chunk. All
receipt/request fields use private JSON tags. No index text, citation metadata,
client origin claim or current tuple can authenticate old supplied text.

`repository.NewRAGChunkSnapshotReader` uses PostgreSQL read-only REPEATABLE READ
or SQLite's read snapshot. It reloads the descriptor, actual chunk body and
metadata, current published source tuple, source observation and fences in that
snapshot. It validates the original read token's durable ID, owner, read kind,
scope, release/expiry and exact epoch. A weaker enclosing GORM transaction is
rejected, since nested savepoints ignore requested isolation. A failed member
returns no partial batch.

`readlease.NextcloudReadGuard.ExactLease` returns a copy of the original token
without acquiring a new generation. The caller still checks current grants,
source publication and `Verify` after capture and before every subsequent
model/output boundary. A read lease protects rows from GC; it grants no access.

## Text search and augmentation

`ConfigureKnowledgeBaseRAGChunkHydration(base, reader, guard)` is a trusted DI
setter. Real `processSearchResults` hydration uses actual captured bodies and
headers. Vector/keyword material supplies candidate IDs/scores only; stale
`MatchedContent` is discarded, and FAQ formatting uses captured canonical
question metadata. The original receipt goes on private
`SearchResult.SourceLineage`.

Before building a result or its progress/title fields, captured receipts enter
the request-local union and `CheckedSnapshot`; without a turn runtime, the
mandatory source guard checks the union and actual KB scopes. A trusted direct
entry can carry a reauthorized Agent selector with `WithRAGChunkReadScope`.
Execution tenant never replaces the original caller. Missing runtime/guard
wiring cannot authorize a configured reader's material.

`ConfigurePluginMergeRAGChunkHydration` supplies the authorized augmentation
capability. Text parent/neighbor and FAQ row reads return actual receipts and
union with the original input. A missing original history receipt, missing
capability, changed generation or foreign-document relation is unknown; the
old body is never stamped with a fresh current identity. Overlap merging
preserves both dependencies. Generic image-child enrichment without a real
snapshot receipt poisons the result/turn as unknown. Image-chain, graph node/
edge/control text, wiki, analysis and other opaque adapters need their own
actual-material contract and remain unaccounted until integrated. This is a
coverage inventory, not a permanent removal of these features.

## Minimum ordinary writer/proof integration

`OrdinaryRAGInputProofLookup.ReadOrdinaryRAGInputLineage` runs inside the same
snapshot and receives the exact descriptor and chunk (nil for metadata-only).
No implementation is enabled by default. NULL legacy inputs, orphaned history
passages and unknown provenance stay unknown. Channel, `Knowledge.Type`, KB
name, current `ChatHistoryConfig`, a client metadata origin flag and absence of
an active data source cannot certify independence. Retained exact-document
source-version/revision evidence also prevents channel erasure from relabeling
a source row as ordinary.

The production writer must record server-owned provenance for every newly
authored material revision. A proof must cover the actual returned text,
title/file name/description/custom metadata, chunk metadata/image text and
revision or immutable digest. The origin record must be protected from client
claims and normal metadata edits. Direct user-authored uploads/passages can
receive explicit complete-empty only through their trusted creation boundary;
server-generated/copied inputs inherit their input union. An authenticated
caller alone does not prove that a server-derived payload is independent.

History indexing must atomically bind the completed message's original lineage
to the exact resulting passage before that passage can be searched. A failed
post-creation `messages.knowledge_id` link, a deleted/replaced history-KB setting
or a changed body cannot erase its dependency. Until that writer and all
derivative readers are covered, controlled/unknown indexing remains closed.
The lookup capability can support verified complete-empty ordinary documents
and source-aware derivatives once their real producers are wired; it must not
be left as a permanent ordinary-document blocker.

## Verification

Focused tests cover actual database text/header binding; original ETag/epoch;
old index/new state with an old token; inability to refresh old augmentation
receipts using a new token; missing/forged/expired/released tokens; cross-scope
IDs; erased source channel; untrusted metadata origin claims; ordinary/orphan
unknown; a separate exact-input digest proof producing complete-empty; changed
body invalidating that proof; early metadata/progress denial; parent/neighbor
inheritance and unknown history refusal. PostgreSQL pauses between two actual
document reads, commits withdrawal on another connection, then verifies the
coherent capture plus mandatory release denial and no partial retry result.
These use owned SQLite/PostgreSQL and policy stubs, not a real AD service.
