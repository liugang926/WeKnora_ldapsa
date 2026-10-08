# Trusted source lineage policy boundary

The resolver/authorizer in this change is deliberately unused by existing
producers, message readers, history/SSE replay and indexing. It is a boundary
for the next implementation phase, not complete lineage coverage or V1
permission acceptance. No DI entry or runtime route enables it here.

## Resolver

The knowledge repository implements the narrow
`access.NextcloudLineageSnapshotLookup` capability:

```go
ResolveNextcloudSourceLineage(ctx, effectiveSourceTenant, kbID, knowledgeID)
```

Arguments identify a server-resolved document scope. Source identity and ETag
are never accepted from tool/model strings, citation metadata or request JSON.
The resolver reloads the document, current published source intent, active
paired data source/configuration, pair operation, latest observation record
and open KB/exact build fences in a coherent database snapshot. It requires
matching ownership and complete source metadata, denies disabled/deleted/
staging/retired/missing rows, and validates the full typed identity. PostgreSQL
uses a read-only REPEATABLE READ transaction; SQLite uses its read transaction.
This method itself does not grant caller access.

`SourceIdentity.Revision` is the **exact open build-fence epoch**, together
with the immutable knowledge ID. It is not the append-only source observation
sequence or manifest generation. A same-ETag/current-candidate rename or repeat
publication may append observations without changing the captured identity.
A changed candidate/build epoch or original ETag is a different dependency.
The latest ledger observation is checked for consistency with the current
published intent, not compared as the answer's generation.

This boundary relies on the existing fenced generation protocol. It does not
establish that every generic/external writer is covered; that broader build
and derived-GC coverage remains open. A real parser/build configuration change
must receive a new immutable candidate or admitted build epoch through that
protocol, never reuse an old generation silently.

## Authorizer

`access.NewNextcloudSourceLineageGuard` takes snapshot lookup, KB lookup,
current organization/Agent sharing, directory resource permission lookup and
the existing live `NextcloudPublicationGuard`. All controlling dependencies
are mandatory for source content; missing/error outcomes close access.

- `Check(ctx, lineage, LineageReadScope)` requires a complete validated envelope,
  compares every original identity/ETag/epoch to trusted current rows, verifies
  current KB and directory/group resource grants and interactive directory
  identity, and runs the live Nextcloud authorization/ETag check. It reloads
  current grants after all remote calls, then locally revalidates the same
  principal's AD identity/snapshot and the entire source dependency set in one
  coherent database snapshot. Source mismatch denies; snapshot storage failure
  remains unavailable rather than being redacted as an ordinary denial.
- `Resolve(ctx, tenant, kb, document, scope)` obtains a trusted identity and
  passes the same authorization before returning its single-document envelope.
  Producers must still union all actual influencing inputs and emitted source
  results; this envelope is not an exhaustive turn by itself.
- `LineageReadScope.Targets` contains server-resolved KB scopes whose existing
  rules still apply even for explicit complete-empty ordinary lineage. An
  optional Agent selector is reloaded/reauthorized against its actual source
  tenant/configuration even for empty targets, and also checks its current
  Agent/use directory resource policy. Owned/builtin Agent existence and
  lifecycle remain the entry-point's responsibility; the selected resource
  policy is still checked. It is not treated as a grant.

Source-bearing lineages require the captured interactive web caller and the
same principal/user, reject API-key/machine/uncaptured callers, and preserve
the original caller independently of shared effective source tenant. Cached
exact KB/context/Agent grants are not proof of current access. Each check
performs fresh organization/Agent and directory resource decisions. Ordinary
complete-empty lineages retain current KB/API-key rules and do not require
Nextcloud or AD source calls merely because no source was used.

Organization KB grants are checked independently before Agent KB fallback,
as in `ResolveKB`. A legitimate machine principal may hold a tenant/role
organization grant without a Web UserID; source-bearing lineages retain the
strict interactive principal requirement.

The batch capability is
`RecheckNextcloudSourceLineage(ctx, lineage) (matches bool, err error)`.
`false, nil` means a recorded tuple/fence no longer matches;
`false, err` is availability failure and must stop the operation.

Session/message ownership remains an entry-point responsibility. This policy
does not acquire a source lease, retain rows for GC, hide metadata for an
unrelated resource, backfill legacy NULL, persist answer/event lineage or
propagate dependencies through summaries/forks/memory/artifacts. Calls must
occur before actual model input and each output/replay/derivative boundary
once producer coverage is implemented. Current producer/reader behavior is
unchanged by this unused policy.

## Tests

The focused command is:

```sh
go test -p 1 ./internal/application/access ./internal/application/repository ./internal/types \
  -run '^Test(NextcloudSourceLineage|PostgresNextcloudSourceLineage|SourceLineage)' -count=1
```

Set `WEKNORA_LEASE_TEST_POSTGRES_DSN` to a disposable PostgreSQL to exercise
the PostgreSQL resolver (its schema is isolated). Tests cover real persisted
SQLite/PostgreSQL tuple resolution and same-ETag rename/republish, changed
ETag/build epoch/candidate, missing/corrupt rows, unknown/NULL/overflow,
current KB/directory/org/Agent revocation while Nextcloud still allows,
local change during remote authorization, current source denial/error/ETag
change, unsupported/machine callers, shared effective tenant and complete-empty
ordinary KB/API scope. Directory and Nextcloud policy tests use service stubs;
they are not real AD or external source acceptance.
