# Chunk REST original body candidate

This is an explicitly partial continuation from `96c60778` with the grant
final-check fix `7447bae6`. It covers `GET /chunks/by-id/:id` and
`GET /chunks/:knowledge_id` only. It does not close source leases/GC, the whole
REST inventory, deployment, or V1.

The single read uses the old service row only as a server scope/ID hint. The
actual serialized chunk comes from the persisted original full material
receipt, never a fresh channel/source lookup of the supplied body. The list
captures and authorizes every chunk type/storage status before applying type
filters, stable order, page slicing and totals. A nonmatching legacy candidate
remains unknown; complete fresh original rows retain normal pagination.

The request-local guard holds detached original proof and actual wire body.
Current source/KB/directory authorization runs before the whole original
material/membership SQLite or PostgreSQL RR check, and again after that local
check. The response writer compares its actual detached snapshot before
writing; callback mutation cannot change retained authority. Graph input
projection cannot certify full REST chunk fields or metadata. Changed
membership, content ABA, foreign/orphan rows and missing original receipts
close the response.

The existing original Nextcloud exact read lease is held for the direct
source document until the response is formatted/written. This change does not
claim exhaustive durable leases for every inherited source, source GC or every
HTTP/replay consumer. Current lineage permission checks remain mandatory.

## Composition hooks

Root owns `container.go` and the knowledge/file consumers. Add:

- Provide `service.NewChunkOutputAuthorizer(store, sourceLineageGuard)`.
- Invoke `handler.ConfigureChunkHandlerOutputLineage(chunkHandler, authorizer)`.

The origin store implements the new narrow
`interfaces.ChunkOutputInputReader` capability. It selects the original
metadata material receipt's admission and full chunk receipts. The coherent
recheck validates scope, each exact wire row, original digest/generation,
monotonic material versions and (for lists) the complete current candidate ID
membership in the same database transaction.

## Verified and remaining

Local HTTP/SQLite cases cover stale service text replacement, client JSON
claims ignored, nonmatching legacy image refusal before page/count, positive
fresh pages/totals, grant withdrawal after local checking, content ABA and hard
delete. Actual SQLite and a uniquely owned PostgreSQL 16 fixture exercise
original full body and complete membership, candidate addition/deletion,
unknown legacy contributions, wire-field mutation and graph-projection denial.
Model and enterprise services are not called. Current directory checks use a
local service stub; this is not real AD/Nextcloud acceptance.

Still unfinished and assigned for a fresh continuation:

- Processing spans: original attempt/input/output/error/control receipts,
  immutable start proof, same-transaction writer/read verification, unknown
  starts never repaired by a later End, old attempt/generation/source revoke.
  PG138/SQLite57 are reserved for that work; no span schema is added here.
- Knowledge list/search/folders: complete actual candidate set, current
  permissions before predicate/sort/count/page/has-more, exhaustive original
  metadata/control union, coherent membership and output snapshot verification.
- Chunk revision history and mutation/revert result consumers, source-bearing
  generated questions/image/FAQ/wide enrichment inventory, complete inherited
  source lease/GC coverage, real HTTP reconnect and production acceptance.
